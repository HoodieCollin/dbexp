#![feature(lazy_cell)]
#![feature(allocator_api)]

use std::{
    alloc::{AllocError, Allocator, Layout},
    collections::HashMap,
    mem::ManuallyDrop,
    ptr::NonNull,
    sync::Arc,
};

use anyhow::Result;
use parking_lot::RwLock;

pub mod shared_object;
pub mod typed_arc;

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
type StackMap = Arc<RwLock<HashMap<Layout, StackEntry>>>;

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
    pub fn new(stack_map: HashMap<Layout, StackEntry>) -> Self {
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
        Self::new(HashMap::new())
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

// pub(crate) mod sealed {
//     use crate::Recycler;

//     pub trait GlobalRecycler {
//         fn recycler() -> Recycler;
//     }
// }

// macro_rules! new_global_recycler {
//     (
//         $name:ident
//     ) => {
//         #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
//         pub struct $name;

//         impl crate::sealed::GlobalRecycler for $name {
//             fn recycler() -> Recycler {
//                 static mut GLOBAL_RECYCLER: LazyCell<Recycler> = LazyCell::new(Recycler::default);
//                 unsafe { GLOBAL_RECYCLER.clone() }
//             }
//         }

//         unsafe impl std::alloc::Allocator for $name {
//             fn allocate(
//                 &self,
//                 layout: std::alloc::Layout,
//             ) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
//                 Self::recycler().allocate(layout)
//             }

//             unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
//                 Self::recycler().deallocate(ptr, layout)
//             }
//         }
//     };
// }

// pub(crate) use new_global_recycler;
