use std::cell::LazyCell;

use anyhow::Result;
use hashbrown::{hash_map::DefaultHashBuilder, HashSet};
use serde::Serialize;

use crate::{
    sealed::GlobalRecycler,
    shared_object::{SharedObject, SharedObjectMut, SharedObjectRef},
    Recycler,
};

pub mod ordered;

#[derive(Debug, Clone, Serialize)]
pub struct Set<T: Eq + std::hash::Hash>(HashSet<T, DefaultHashBuilder, Recycler>);

impl<T: Eq + std::hash::Hash> GlobalRecycler for Set<T> {
    fn recycler() -> Recycler {
        // avoid using `LazyLock` because `Recycler` is inherently thread-safe thus the lock is unnecessary
        static mut GLOBAL_RECYCLER: LazyCell<Recycler> = LazyCell::new(Recycler::default);
        unsafe { GLOBAL_RECYCLER.clone() }
    }
}

impl<T: Eq + std::hash::Hash> std::ops::Deref for Set<T> {
    type Target = HashSet<T, DefaultHashBuilder, Recycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Eq + std::hash::Hash> std::ops::DerefMut for Set<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Eq + std::hash::Hash> FromIterator<T> for Set<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut set = Self::with_capacity(iter.size_hint().0);

        set.extend(iter);
        set
    }
}

impl<T: Eq + std::hash::Hash> IntoIterator for Set<T> {
    type Item = T;
    type IntoIter = hashbrown::hash_set::IntoIter<T, Recycler>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T: Eq + std::hash::Hash> IntoIterator for &'a Set<T> {
    type Item = &'a T;
    type IntoIter = hashbrown::hash_set::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: Eq + std::hash::Hash> Set<T> {
    pub fn new() -> Self {
        Self(HashSet::new_in(Self::recycler()))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(HashSet::with_capacity_and_hasher_in(
            capacity,
            DefaultHashBuilder::default(),
            Self::recycler(),
        ))
    }

    pub fn into_shared(self) -> SharedSet<T>
    where
        T: Send + Sync,
    {
        SharedSet(SharedObject::new(self))
    }
}

#[repr(transparent)]
pub struct SharedSet<T: Eq + std::hash::Hash + Send + Sync + 'static>(SharedObject<Set<T>>);

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
