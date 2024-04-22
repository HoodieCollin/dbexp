use anyhow::Result;
use serde::Serialize;

use crate::{buffer::Buffer, sealed::GlobalRecycler, Recycler};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FixedBuffer<T>(Buffer<T>, u32);

impl<T> GlobalRecycler for FixedBuffer<T> {
    fn recycler() -> Recycler {
        super::BufferRecycler::recycler()
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
