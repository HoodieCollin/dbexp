use std::cell::LazyCell;

use anyhow::Result;
use serde::Serialize;

use crate::{
    sealed::GlobalRecycler,
    shared_object::{SharedObject, SharedObjectMut, SharedObjectRef},
    Recycler,
};

pub mod fixed;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Buffer<T>(Vec<T, Recycler>);

impl<T> GlobalRecycler for Buffer<T> {
    fn recycler() -> Recycler {
        // avoid using `LazyLock` because `Recycler` is inherently thread-safe thus the lock is unnecessary
        static mut GLOBAL_RECYCLER: LazyCell<Recycler> = LazyCell::new(Recycler::default);
        unsafe { GLOBAL_RECYCLER.clone() }
    }
}

impl<T> std::ops::Deref for Buffer<T> {
    type Target = Vec<T, Recycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Buffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> FromIterator<T> for Buffer<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut buf = Self::with_capacity(iter.size_hint().0 as u32);

        buf.extend(iter);
        buf
    }
}

impl<T> Buffer<T> {
    pub fn new() -> Self {
        Self(Vec::new_in(Self::recycler()))
    }

    pub fn with_capacity(capacity: u32) -> Self {
        Self(Vec::with_capacity_in(capacity as usize, Self::recycler()))
    }

    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }

    pub fn capacity(&self) -> u32 {
        self.0.capacity() as u32
    }

    pub fn as_slice(&self) -> &[T] {
        self.0.as_slice()
    }

    pub fn as_slice_mut(&mut self) -> &mut [T] {
        self.0.as_mut()
    }

    pub fn into_shared(self) -> SharedBuffer<T>
    where
        T: Send + Sync + 'static,
    {
        SharedBuffer(SharedObject::new(self))
    }

    pub fn into_iter(self) -> std::vec::IntoIter<T, Recycler> {
        self.0.into_iter()
    }
}

impl<T: Serialize> Serialize for Buffer<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.len() as usize))?;

        for item in self.iter() {
            seq.serialize_element(item)?;
        }

        seq.end()
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct SharedBuffer<T: 'static>(SharedObject<Buffer<T>>);

impl<T> SharedBuffer<T> {
    pub fn new() -> Self {
        Self(SharedObject::new(Buffer::new()))
    }

    pub fn with_capacity(capacity: u32) -> Self {
        Self(SharedObject::new(Buffer::with_capacity(capacity)))
    }

    pub fn new_copy(&self) -> Self
    where
        T: Clone,
    {
        self.0
            .read_with(|inner| Self(SharedObject::new(inner.clone())))
    }

    pub fn unwrap_or_clone(self) -> Buffer<T>
    where
        T: Clone,
    {
        self.0.unwrap_or_clone()
    }

    pub fn try_unwrap(self) -> Result<Buffer<T>, Self> {
        match SharedObject::try_unwrap(self.0) {
            Ok(inner) => Ok(inner),
            Err(src) => Err(Self(src)),
        }
    }

    pub fn len(&self) -> u32 {
        self.0.read_with(|inner| inner.len())
    }

    pub fn capacity(&self) -> u32 {
        self.0.read_with(|inner| inner.capacity())
    }

    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Buffer<T>) -> R,
    {
        self.0.read_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Buffer<T>) -> R,
    {
        self.0.read_recursive_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn read_guard(&self) -> SharedObjectRef<Buffer<T>> {
        self.0.read_guard()
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Buffer<T>) -> R,
    {
        self.0.write_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn write_guard(&self) -> SharedObjectMut<Buffer<T>> {
        self.0.write_guard()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SharedBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.read_recursive_with(|inner| write!(f, "{:?}", inner))
    }
}

impl<T: std::hash::Hash> std::hash::Hash for SharedBuffer<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.read_recursive_with(|inner| inner.as_slice().hash(state))
    }
}

impl<T: Serialize> Serialize for SharedBuffer<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;

        self.read_recursive_with(|inner| {
            let mut seq = serializer.serialize_seq(Some(inner.len() as usize))?;

            for item in inner.iter() {
                seq.serialize_element(item)?;
            }

            seq.end()
        })
    }
}
