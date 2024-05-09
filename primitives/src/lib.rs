// #![allow(incomplete_features)]
#![feature(lazy_cell)]
#![feature(allocator_api)]
#![feature(os_str_display)]
#![feature(step_trait)]
#![feature(alloc_layout_extra)]
#![feature(generic_const_exprs)]

use std::{
    alloc::{AllocError, Allocator, Layout},
    mem::ManuallyDrop,
    ptr::NonNull,
    sync::Arc,
};

use anyhow::Result;
use indexmap::IndexMap;
use parking_lot::RwLock;

pub mod byte_encoding;
pub mod bytes;
pub mod data;
pub mod idx;
pub mod internal_path;
pub mod internal_string;
pub mod number;
pub mod oid;
pub mod shared_object;
pub mod text;
pub mod timestamp;
pub mod vector;

pub use bytes::Bytes;
pub use data::{DataType, ExpectedType};
pub use idx::{Idx, ThinIdx};
pub use internal_path::InternalPath;
pub use internal_string::InternalString;
pub use number::Number;
pub use oid::{O16, O32, O64};
pub use shared_object::SharedObject;
pub use text::Text;
pub use timestamp::Timestamp;
pub use vector::Vector;

/// ## !!! WARNING !!!
///
/// This is incredibly unsafe and should only be used in very specific circumstances.
///
/// For example, when designing a pool structure where the items of the pool write a
/// a type with a lifetime *AND* an `Arc`-like pointer to the pool itself, you can
/// use this function to force the lifetime of the underlying type to be `'static`.
///
/// The previous example is sound because the pool itself is responsible for managing
/// the lifetime of the items, and the items should not be allowed to outlive the pool.
pub unsafe fn force_transmute<T, U>(value: T) -> U {
    union Transmute<T, U> {
        from: ManuallyDrop<T>,
        to: ManuallyDrop<U>,
    }

    let transmute = Transmute {
        from: ManuallyDrop::new(value),
    };

    ManuallyDrop::into_inner(transmute.to)
}

/// We assert that these pointers will be used in a thread-safe manner.
///
/// Be warned, this is a very unsafe assumption.
pub struct UnsafeNonNull {
    inner: NonNull<[u8]>,
}

unsafe impl Send for UnsafeNonNull {}
unsafe impl Sync for UnsafeNonNull {}

impl std::fmt::Debug for UnsafeNonNull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:p}", self.inner.as_ptr())
    }
}

/// A stack of recycled memory blocks for a layout.
type StackEntry = Arc<RwLock<Vec<UnsafeNonNull>>>;

/// A map of layouts to their respective stack of recycled memory blocks.
type StackMap = Arc<RwLock<IndexMap<Layout, StackEntry>>>;

pub enum RecyclerError {
    Unexpected(anyhow::Error),
    Unavailable,
}

impl From<anyhow::Error> for RecyclerError {
    fn from(err: anyhow::Error) -> Self {
        Self::Unexpected(err)
    }
}

/// An allocator that recycles memory blocks for a given layout.
pub struct Recycler(StackMap);

impl Recycler {
    pub fn new(stack_map: IndexMap<Layout, StackEntry>) -> Self {
        Self(Arc::new(RwLock::new(stack_map)))
    }

    pub fn access_stack<F, E>(
        &self,
        layout: Layout,
        f: F,
    ) -> Result<Option<UnsafeNonNull>, RecyclerError>
    where
        F: FnOnce(&RwLock<Vec<UnsafeNonNull>>) -> Result<Option<UnsafeNonNull>, E>,
        E: Into<RecyclerError>,
    {
        let stack = {
            if let Some(found) = {
                let guard = self.0.try_read().ok_or(RecyclerError::Unavailable)?;
                guard.get(&layout).map(Arc::clone)
            } {
                found
            } else {
                let mut guard = self.0.try_write().ok_or(RecyclerError::Unavailable)?;
                let new = Arc::new(RwLock::new(Vec::new()));
                guard.insert(layout, Arc::clone(&new));
                new
            }
        };

        match f(stack.as_ref()) {
            Ok(result) => Ok(result),
            Err(err) => Err(err.into()),
        }
    }

    pub fn clear(&self) {
        let mut guard = self.0.write();
        guard.clear();
    }

    pub fn reserve<T>(&self, count: usize) -> Result<(), RecyclerError> {
        let layout = Layout::new::<T>();

        let stack = {
            if let Some(found) = {
                let guard = self.0.try_read().ok_or(RecyclerError::Unavailable)?;
                guard.get(&layout).map(Arc::clone)
            } {
                found
            } else {
                let mut guard = self.0.try_write().ok_or(RecyclerError::Unavailable)?;
                let new = Arc::new(RwLock::new(Vec::new()));
                guard.insert(layout, Arc::clone(&new));
                new
            }
        };

        let mut stack_guard = stack.write();
        stack_guard.reserve(count);

        Ok(())
    }
}

impl Clone for Recycler {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl PartialEq for Recycler {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Recycler {}

impl std::hash::Hash for Recycler {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let uint_ptr = Arc::as_ptr(&self.0) as usize;
        uint_ptr.hash(state)
    }
}

impl Default for Recycler {
    fn default() -> Self {
        Self::new(IndexMap::new())
    }
}

impl std::fmt::Debug for Recycler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let map_guard = self.0.read();

        if f.alternate() {
            return write!(f, "{:#?}", map_guard);
        }

        let mut d = f.debug_tuple("Recycler");

        struct StackInfo {
            layout: Layout,
            count: usize,
        }

        impl std::fmt::Debug for StackInfo {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_map()
                    .entry(&"layout", &self.layout)
                    .entry(&"count", &self.count)
                    .finish()
            }
        }

        for (layout, stack) in map_guard.iter() {
            let stack_guard = stack.read();

            d.field(&StackInfo {
                layout: *layout,
                count: stack_guard.len(),
            });
        }

        d.finish()
    }
}

unsafe impl Allocator for Recycler {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let recycled = self.access_stack(layout, |stack| {
            let mut guard = stack.write();
            Result::<_, RecyclerError>::Ok(guard.pop())
        });

        fn inner_allocate(layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            unsafe {
                let ptr = std::alloc::alloc(layout);

                if ptr.is_null() {
                    Err(AllocError)
                } else {
                    Ok(NonNull::new_unchecked(std::slice::from_raw_parts_mut(
                        ptr,
                        layout.size(),
                    )))
                }
            }
        }

        match recycled {
            Ok(Some(ptr)) => Ok(ptr.inner),
            Ok(None) => inner_allocate(layout),
            Err(err) => match err {
                RecyclerError::Unavailable => inner_allocate(layout),
                RecyclerError::Unexpected(err) => {
                    eprintln!("Recycler error: {:?}", err);
                    Err(AllocError)
                }
            },
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        let res = self.access_stack(layout, |stack| {
            let mut guard = stack.try_write().ok_or(RecyclerError::Unavailable)?;

            guard.push(UnsafeNonNull {
                inner: NonNull::new_unchecked(std::slice::from_raw_parts_mut(
                    ptr.as_ptr(),
                    layout.size(),
                )),
            });

            Result::<_, RecyclerError>::Ok(None)
        });

        if let Err(err) = res {
            if let RecyclerError::Unexpected(err) = err {
                eprintln!("Recycler error: {:?}", err);
            }

            std::alloc::dealloc(ptr.as_ptr(), layout);
        }
    }
}
