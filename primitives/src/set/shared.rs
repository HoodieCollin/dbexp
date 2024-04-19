use anyhow::Result;
use serde::Serialize;

use crate::shared_object::{SharedObject, SharedObjectMut, SharedObjectRef};

use super::Set;

#[repr(transparent)]
pub struct SharedSet<T: Eq + std::hash::Hash + Send + Sync + 'static>(
    pub(super) SharedObject<Set<T>>,
);

impl<T: Eq + std::hash::Hash + Send + Sync> SharedSet<T> {
    pub fn new() -> Self {
        Self(SharedObject::new(Set::new()))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(SharedObject::new(Set::with_capacity(capacity)))
    }

    pub fn new_copy(&self) -> Self
    where
        T: Clone,
    {
        self.0
            .read_with(|inner| Self(SharedObject::new(inner.clone())))
    }

    pub fn unwrap_or_clone(self) -> Set<T>
    where
        T: Clone,
    {
        self.0.unwrap_or_clone()
    }

    pub fn try_unwrap(self) -> Result<Set<T>, Self> {
        match SharedObject::try_unwrap(self.0) {
            Ok(inner) => Ok(inner),
            Err(this) => Err(Self(this)),
        }
    }

    pub fn len(&self) -> usize {
        self.0.read_with(|inner| inner.len())
    }

    pub fn capacity(&self) -> usize {
        self.0.read_with(|inner| inner.capacity())
    }

    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Set<T>) -> R,
    {
        self.0.read_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Set<T>) -> R,
    {
        self.0.read_recursive_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn read_guard(&self) -> SharedObjectRef<Set<T>> {
        self.0.read_guard()
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Set<T>) -> R,
    {
        self.0.write_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn write_guard(&self) -> SharedObjectMut<Set<T>> {
        self.0.write_guard()
    }
}

impl<T: Clone + Eq + std::hash::Hash + Send + Sync> Clone for SharedSet<T> {
    fn clone(&self) -> Self {
        Self(SharedObject::new(self.0.read_with(|inner| inner.clone())))
    }
}

impl<T: std::fmt::Debug + Eq + std::hash::Hash + Send + Sync> std::fmt::Debug for SharedSet<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.read_recursive_with(|inner| write!(f, "{:?}", inner))
    }
}

impl<T: Serialize + Eq + std::hash::Hash + Send + Sync> Serialize for SharedSet<T> {
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
