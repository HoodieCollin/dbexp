use std::cell::LazyCell;

use anyhow::Result;
use serde::Serialize;

use crate::{sealed::GlobalRecycler, Recycler};

pub mod fixed;

crate::new_global_recycler!(BufferRecycler);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Buffer<T>(Vec<T, BufferRecycler>);

impl<T> GlobalRecycler for Buffer<T> {
    fn recycler() -> Recycler {
        BufferRecycler::recycler()
    }
}

impl<T> std::ops::Deref for Buffer<T> {
    type Target = Vec<T, BufferRecycler>;

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
        Self(Vec::new_in(BufferRecycler))
    }

    pub fn with_capacity(capacity: u32) -> Self {
        Self(Vec::with_capacity_in(capacity as usize, BufferRecycler))
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

    pub fn into_iter(self) -> std::vec::IntoIter<T, BufferRecycler> {
        self.0.into_iter()
    }

    pub fn reserve(&mut self, additional: u32) {
        self.0.reserve(additional as usize)
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
