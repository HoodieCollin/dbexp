use std::cell::LazyCell;

use anyhow::Result;
use serde::Serialize;

use crate::{
    buffer::Buffer,
    sealed::GlobalRecycler,
    shared_object::{SharedObject, SharedObjectMut, SharedObjectRef},
    Recycler,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FixedBuffer<T>(Buffer<T>, u32);

impl<T> GlobalRecycler for FixedBuffer<T> {
    fn recycler() -> Recycler {
        // avoid using `LazyLock` because `Recycler` is inherently thread-safe thus the lock is unnecessary
        static mut GLOBAL_RECYCLER: LazyCell<Recycler> = LazyCell::new(Recycler::default);
        unsafe { GLOBAL_RECYCLER.clone() }
    }
}

impl<T> FixedBuffer<T> {
    pub fn new(cap: u32) -> Self {
        Self(Buffer::with_capacity(cap), cap)
    }

    pub fn as_slice(&self) -> &[T] {
        self.0.as_slice()
    }

    pub fn as_slice_mut(&mut self) -> &mut [T] {
        self.0.as_slice_mut()
    }

    pub fn len(&self) -> u32 {
        self.0.len()
    }

    pub fn capacity(&self) -> u32 {
        self.1
    }

    pub fn push(&mut self, item: T) -> Result<(), T> {
        if self.len() == self.capacity() {
            return Err(item);
        }

        self.0.push(item);

        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        self.0.pop()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn into_shared(self) -> SharedFixedBuffer<T>
    where
        T: Send + Sync + 'static,
    {
        SharedFixedBuffer(SharedObject::new(self))
    }

    pub fn extend(&mut self, iter: impl IntoIterator<Item = T>) -> Result<(), Buffer<T>> {
        let iter = iter.into_iter();

        let mut buffer = Buffer::with_capacity(iter.size_hint().0 as u32);

        for item in iter {
            buffer.push(item);
        }

        if buffer.len() as u32 > self.capacity() - self.len() {
            Err(buffer)
        } else {
            self.0.extend(buffer.into_iter());
            Ok(())
        }
    }

    pub fn extend_from_slice(&mut self, other: &[T]) -> Result<()> {
        if other.len() as u32 > self.capacity() - self.len() {
            anyhow::bail!("Bytes buffer is too small for slice");
        }

        Ok(())
    }

    /// ## !!! WARNING !!!
    ///
    /// This function **WILL** panic.
    pub fn extend_reserve(&mut self, _additional: u32) {
        panic!("FixedBuffer cannot be resized")
    }

    /// ## !!! WARNING !!!
    ///
    /// This function **WILL** panic.
    pub fn reserve(&mut self, _additional: u32) {
        panic!("FixedBuffer cannot be resized")
    }

    /// ## !!! WARNING !!!
    ///
    /// This function **WILL** panic.
    pub fn shrink_to(&mut self, _new_len: u32) {
        panic!("FixedBuffer cannot be resized")
    }

    /// ## !!! WARNING !!!
    ///
    /// This function **WILL** panic.
    pub fn shrink_to_fit(&mut self) {
        panic!("FixedBuffer cannot be resized")
    }
}

impl<T> std::ops::Deref for FixedBuffer<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> std::ops::DerefMut for FixedBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice_mut()
    }
}

impl<T> AsRef<[T]> for FixedBuffer<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<T> AsMut<[T]> for FixedBuffer<T> {
    fn as_mut(&mut self) -> &mut [T] {
        self.as_slice_mut()
    }
}

impl<T: Serialize> Serialize for FixedBuffer<T> {
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
pub struct SharedFixedBuffer<T: Send + Sync + 'static>(SharedObject<FixedBuffer<T>>);

impl<T: Send + Sync> SharedFixedBuffer<T> {
    pub fn new(cap: u32) -> Self {
        Self(SharedObject::new(FixedBuffer::new(cap)))
    }

    pub fn new_copy(&self) -> Self
    where
        T: Clone,
    {
        self.0
            .read_with(|inner| Self(SharedObject::new(inner.clone())))
    }

    pub fn len(&self) -> u32 {
        self.0.read_with(|inner| inner.len())
    }

    pub fn capacity(&self) -> u32 {
        self.0.read_with(|inner| inner.capacity())
    }

    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&FixedBuffer<T>) -> R,
    {
        self.0.read_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&FixedBuffer<T>) -> R,
    {
        self.0.read_recursive_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn read_guard(&self) -> SharedObjectRef<FixedBuffer<T>> {
        self.0.read_guard()
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut FixedBuffer<T>) -> R,
    {
        self.0.write_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn write_guard(&self) -> SharedObjectMut<FixedBuffer<T>> {
        self.0.write_guard()
    }
}

impl<T: Send + Sync + std::fmt::Debug> std::fmt::Debug for SharedFixedBuffer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.read_recursive_with(|inner| write!(f, "{:?}", inner))
    }
}

impl<T: Send + Sync + std::hash::Hash> std::hash::Hash for SharedFixedBuffer<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.read_recursive_with(|inner| inner.as_slice().hash(state))
    }
}

impl<T: Send + Sync + Serialize> Serialize for SharedFixedBuffer<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.read_recursive_with(|inner| inner.serialize(serializer))
    }
}
