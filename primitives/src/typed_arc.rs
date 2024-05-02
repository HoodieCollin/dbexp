use std::{
    cell::LazyCell,
    sync::{Arc, Weak},
};

use crate::{force_transmute, new_global_recycler, sealed::GlobalRecycler, Recycler};

new_global_recycler!(TypedArcRecycler);

/// A thread-safe reference-counted pointer that can be used with any type.
///
/// This specialized version of `Arc` is used to allocate memory on the heap
/// that can be reused by other instances of the same type. This is useful
/// for types that are frequently created and destroyed.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TypedArc<T>(Arc<T, TypedArcRecycler>);

/// An opaque version of `TypedArc` that can be used to store any type.
pub type OpaqueArc = TypedArc<()>;

impl<T> TypedArc<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new_in(value, TypedArcRecycler))
    }

    /// Erase the type of this `TypedArc` and return an `OpaqueArc`.
    pub unsafe fn erase_type(self) -> OpaqueArc {
        force_transmute::<_, OpaqueArc>(self)
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

    pub fn downgrade(this: &Self) -> TypedWeak<T> {
        TypedWeak(Arc::downgrade(&this.0))
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

pub struct TypedWeak<T>(Weak<T, TypedArcRecycler>);

pub type OpaqueWeak = TypedWeak<()>;

impl<T> TypedWeak<T> {
    /// Erase the type of this `TypedWeak` and return an `OpaqueWeak`.
    pub unsafe fn erase_type(self) -> OpaqueWeak {
        force_transmute::<_, OpaqueWeak>(self)
    }

    pub fn upgrade(&self) -> Option<TypedArc<T>> {
        self.0.upgrade().map(TypedArc)
    }

    pub fn into_inner(self) -> Option<T>
    where
        T: Clone,
    {
        self.0.upgrade().map(Arc::unwrap_or_clone)
    }
}

impl TypedWeak<()> {
    /// Assume the type of this `OpaqueArc` and return a `TypedWeak`.
    pub unsafe fn assume_type<T>(self) -> TypedWeak<T> {
        force_transmute::<_, TypedWeak<T>>(self)
    }
}

impl<T> Clone for TypedWeak<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> std::ops::Deref for TypedWeak<T> {
    type Target = Weak<T, TypedArcRecycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> AsRef<Weak<T, TypedArcRecycler>> for TypedWeak<T> {
    fn as_ref(&self) -> &Weak<T, TypedArcRecycler> {
        &self.0
    }
}

impl<T> From<TypedArc<T>> for TypedWeak<T> {
    fn from(arc: TypedArc<T>) -> Self {
        Self(Arc::downgrade(&arc.0))
    }
}
