use anyhow::Result;
use serde::Serialize;

use crate::shared_object::{SharedObject, SharedObjectMut, SharedObjectRef};

use super::Map;

#[repr(transparent)]
pub struct SharedMap<K: Eq + std::hash::Hash + Send + Sync + 'static, V: Send + Sync + 'static>(
    pub(super) SharedObject<Map<K, V>>,
);

impl<K: Eq + std::hash::Hash + Send + Sync, V: Send + Sync> SharedMap<K, V> {
    pub fn new() -> Self {
        Self(SharedObject::new(Map::new()))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(SharedObject::new(Map::with_capacity(capacity)))
    }

    pub fn new_copy(&self) -> Self
    where
        K: Clone,
        V: Clone,
    {
        self.0
            .read_with(|inner| Self(SharedObject::new(inner.clone())))
    }

    pub fn unwrap_or_clone(self) -> Map<K, V>
    where
        K: Clone,
        V: Clone,
    {
        self.0.unwrap_or_clone()
    }

    pub fn try_unwrap(self) -> Result<Map<K, V>, Self> {
        match SharedObject::try_unwrap(self.0) {
            Ok(inner) => Ok(inner),
            Err(inner) => Err(Self(inner)),
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
        F: FnOnce(&Map<K, V>) -> R,
    {
        self.0.read_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Map<K, V>) -> R,
    {
        self.0.read_recursive_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn read_guard(&self) -> SharedObjectRef<Map<K, V>> {
        self.0.read_guard()
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Map<K, V>) -> R,
    {
        self.0.write_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn write_guard(&self) -> SharedObjectMut<Map<K, V>> {
        self.0.write_guard()
    }
}

impl<K: Clone + Eq + std::hash::Hash + Send + Sync, V: Clone + Send + Sync> Clone
    for SharedMap<K, V>
{
    fn clone(&self) -> Self {
        Self(SharedObject::new(self.0.read_with(|inner| inner.clone())))
    }
}

impl<K: std::fmt::Debug + Eq + std::hash::Hash + Send + Sync, V: std::fmt::Debug + Send + Sync>
    std::fmt::Debug for SharedMap<K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.read_recursive_with(|inner| write!(f, "{:?}", inner))
    }
}

impl<K: Serialize + Eq + std::hash::Hash + Send + Sync, V: Serialize + Send + Sync> Serialize
    for SharedMap<K, V>
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        self.read_recursive_with(|inner| {
            let mut map = serializer.serialize_map(Some(inner.len()))?;

            for (k, v) in inner.iter() {
                map.serialize_entry(k, v)?;
            }

            map.end()
        })
    }
}
