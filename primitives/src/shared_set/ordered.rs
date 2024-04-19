use std::{cell::LazyCell, collections::BTreeSet};

use anyhow::Result;
use serde::Serialize;

use crate::{
    sealed::GlobalRecycler,
    shared_object::{SharedObject, SharedObjectMut, SharedObjectRef},
    Recycler,
};

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct OrdSet<T>(BTreeSet<T, Recycler>);

impl<T> GlobalRecycler for OrdSet<T> {
    fn recycler() -> Recycler {
        // avoid using `LazyLock` because `Recycler` is inherently thread-safe thus the lock is unnecessary
        static mut GLOBAL_RECYCLER: LazyCell<Recycler> = LazyCell::new(Recycler::default);
        unsafe { GLOBAL_RECYCLER.clone() }
    }
}

impl<T> std::ops::Deref for OrdSet<T> {
    type Target = BTreeSet<T, Recycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for OrdSet<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> OrdSet<T> {
    pub fn new() -> Self {
        Self(BTreeSet::new_in(Self::recycler()))
    }

    pub fn into_shared(self) -> SharedOrdSet<T>
    where
        T: Send + Sync,
    {
        SharedOrdSet(SharedObject::new(self))
    }
}

impl<T: Serialize> Serialize for OrdSet<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.len()))?;

        for item in self.iter() {
            seq.serialize_element(item)?;
        }

        seq.end()
    }
}

#[repr(transparent)]
pub struct SharedOrdSet<T: 'static>(SharedObject<OrdSet<T>>);

impl<T: Send + Sync> SharedOrdSet<T> {
    pub fn new() -> Self {
        Self(SharedObject::new(OrdSet::new()))
    }

    pub fn new_copy(&self) -> Self
    where
        T: Clone,
    {
        self.0
            .read_with(|inner| Self(SharedObject::new(inner.clone())))
    }

    pub fn unwrap_or_clone(self) -> OrdSet<T>
    where
        T: Clone,
    {
        self.0.unwrap_or_clone()
    }

    pub fn try_unwrap(self) -> Result<OrdSet<T>, Self> {
        match SharedObject::try_unwrap(self.0) {
            Ok(inner) => Ok(inner),
            Err(src) => Err(Self(src)),
        }
    }

    pub fn len(&self) -> usize {
        self.0.read_with(|inner| inner.len())
    }

    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&OrdSet<T>) -> R,
    {
        self.0.read_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&OrdSet<T>) -> R,
    {
        self.0.read_recursive_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn read_guard(&self) -> SharedObjectRef<OrdSet<T>> {
        self.0.read_guard()
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut OrdSet<T>) -> R,
    {
        self.0.write_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn write_guard(&self) -> SharedObjectMut<OrdSet<T>> {
        self.0.write_guard()
    }
}

impl<T: Clone + Send + Sync> Clone for SharedOrdSet<T> {
    fn clone(&self) -> Self {
        Self(SharedObject::new(self.0.read_with(|inner| inner.clone())))
    }
}

impl<T: std::fmt::Debug + Send + Sync> std::fmt::Debug for SharedOrdSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.read_recursive_with(|inner| write!(f, "{:?}", inner))
    }
}

impl<T: Serialize + Send + Sync> Serialize for SharedOrdSet<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;

        self.read_recursive_with(|inner| {
            let mut seq = serializer.serialize_seq(Some(inner.len()))?;

            for item in inner.iter() {
                seq.serialize_element(item)?;
            }

            seq.end()
        })
    }
}
