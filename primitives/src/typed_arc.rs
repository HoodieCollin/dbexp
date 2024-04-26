use std::{cell::LazyCell, sync::Arc};

use crate::{force_transmute, Recycler};

/// A thread-safe reference-counted pointer that can be used with any type.
///
/// This specialized version of `Arc` is used to allocate memory on the heap
/// that can be reused by other instances of the same type. This is useful
/// for types that are frequently created and destroyed.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TypedArc<T>(Arc<T, Recycler>);

/// An opaque version of `TypedArc` that can be used to store any type.
pub type OpaqueArc = TypedArc<()>;

// avoid using `LazyLock` because `Recycler` is inherently thread-safe thus the lock is unnecessary
static mut GLOBAL_ARC_RECYCLER: LazyCell<Recycler> = LazyCell::new(Recycler::default);

impl<T> TypedArc<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new_in(value, unsafe { GLOBAL_ARC_RECYCLER.clone() }))
    }

    /// Erase the type of this `TypedArc` and return an `OpaqueArc`.
    pub unsafe fn erase_type(self) -> OpaqueArc {
        force_transmute::<_, OpaqueArc>(self)
    }

    pub fn recycler() -> Recycler {
        unsafe { GLOBAL_ARC_RECYCLER.clone() }
    }

    pub fn into_inner(self) -> T
    where
        T: Clone,
    {
        Arc::unwrap_or_clone(self.0)
    }

    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        Arc::try_unwrap(this.0).map_err(|arc| Self(arc))
    }
}

impl TypedArc<()> {
    /// Assume the type of this `OpaqueArc` and return a `TypedArc`.
    pub unsafe fn assume_type<T>(self) -> TypedArc<T> {
        force_transmute::<_, TypedArc<T>>(self)
    }
}

impl<T> Clone for TypedArc<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Default for TypedArc<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<T> std::ops::Deref for TypedArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<T> for TypedArc<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}
