use std::{alloc::Layout, mem::ManuallyDrop, ptr::NonNull, sync::Arc};

use anyhow::Result;
use memmap2::MmapMut;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::number::U24;

const MAX_LEN: usize = 4096;

pub struct RawVector<T> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
}

impl<T> RawVector<T> {
    pub fn new(ptr: NonNull<T>, len: usize, cap: usize) -> Result<Self> {
        if cap > MAX_LEN {
            anyhow::bail!("Vector buffer capacity is too large");
        }

        Ok(Self { ptr, len, cap })
    }
}

#[derive(thiserror::Error, Debug)]
pub struct VectorError<T> {
    pub item: T,
    #[source]
    msg: anyhow::Error,
}

impl<T> VectorError<T> {
    pub fn new(item: T, msg: anyhow::Error) -> Self {
        Self { item, msg }
    }
}

/// A Vec with a fixed capacity.
pub struct Vector<T> {
    storage: Option<Arc<MmapMut>>,
    inner: ManuallyDrop<Vec<T>>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for Vector<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.inner[..].iter()).finish()
    }
}

impl<T> std::ops::Deref for Vector<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for Vector<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T> AsRef<[T]> for Vector<T> {
    fn as_ref(&self) -> &[T] {
        self.inner.as_slice()
    }
}

impl<T> AsMut<[T]> for Vector<T> {
    fn as_mut(&mut self) -> &mut [T] {
        self.inner.as_mut_slice()
    }
}

impl<T> Drop for Vector<T> {
    fn drop(&mut self) {
        unsafe {
            if self.storage.is_none() {
                ManuallyDrop::drop(&mut self.inner);
            }
        }
    }
}

impl<T: Serialize> Serialize for Vector<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.inner.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Vector<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let inner = Vec::deserialize(deserializer)?;

        Ok(Self {
            storage: None,
            inner: ManuallyDrop::new(inner),
        })
    }
}

impl<T: Clone> Clone for Vector<T> {
    fn clone(&self) -> Self {
        let mut inner = Vec::with_capacity(self.capacity());
        inner.extend_from_slice(self.as_slice());

        Self {
            storage: None,
            inner: ManuallyDrop::new(inner),
        }
    }
}

impl<T: PartialEq> PartialEq for Vector<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<T: Eq> Eq for Vector<T> {}

impl<T: PartialOrd> PartialOrd for Vector<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

impl<T: Ord> Ord for Vector<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<T: std::hash::Hash> std::hash::Hash for Vector<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl<T> Vector<T> {
    pub const MAX_LEN: usize = U24::MAX;

    pub fn layout_and_item_offset_for(cap: usize) -> Result<(Layout, usize)> {
        if cap > MAX_LEN {
            anyhow::bail!("Vector buffer capacity is too large");
        }

        Ok(Layout::new::<T>().repeat(cap)?)
    }

    #[must_use]
    pub fn new(cap: usize) -> Result<Self> {
        if cap > MAX_LEN {
            anyhow::bail!("Vector buffer capacity is too large");
        }

        let inner = ManuallyDrop::new(Vec::with_capacity(cap));

        Ok(Self {
            storage: None,
            inner,
        })
    }

    #[must_use]
    pub fn into_raw(mut self) -> RawVector<T> {
        // ensure the storage is dropped
        let _ = self.storage.take();

        let ptr = NonNull::new(self.inner.as_mut_ptr()).unwrap();
        let len = self.inner.len();
        let cap = self.inner.capacity();

        std::mem::forget(self);

        RawVector { ptr, len, cap }
    }

    #[must_use]
    pub fn from_raw(
        raw: RawVector<T>,
        storage: Arc<MmapMut>,
    ) -> Result<Self, VectorError<RawVector<T>>> {
        if raw.cap > MAX_LEN {
            return Err(VectorError::new(
                raw,
                anyhow::anyhow!("Vector buffer capacity is too large"),
            ));
        }

        if raw.len > raw.cap {
            return Err(VectorError::new(
                raw,
                anyhow::anyhow!("Vector buffer length is too large"),
            ));
        }

        let inner =
            ManuallyDrop::new(unsafe { Vec::from_raw_parts(raw.ptr.as_ptr(), raw.len, raw.cap) });

        Ok(Self {
            storage: Some(storage),
            inner,
        })
    }

    pub fn into_vec(mut self) -> Vec<T> {
        if let Some(storage) = self.storage.take() {
            let mut vec = Vec::with_capacity(self.capacity());
            vec.append(&mut self.inner);

            drop(storage);
            vec
        } else {
            unsafe { ManuallyDrop::take(&mut self.inner) }
        }
    }

    #[must_use]
    pub fn from_vec(mut vec: Vec<T>, cap: usize) -> Result<Self, VectorError<Vec<T>>> {
        if cap > MAX_LEN {
            return Err(VectorError::new(
                vec,
                anyhow::anyhow!("Vector buffer capacity is too large"),
            ));
        }

        if vec.len() > cap {
            return Err(VectorError::new(
                vec,
                anyhow::anyhow!("Vector buffer length is too large"),
            ));
        }

        vec.reserve_exact(cap - vec.len());

        let inner = ManuallyDrop::new(vec);

        Ok(Self {
            storage: None,
            inner,
        })
    }

    #[must_use]
    pub fn try_from_slice(items: &[T], cap: usize) -> Result<Self>
    where
        T: Clone,
    {
        if items.len() > cap {
            anyhow::bail!("Vector buffer is too small for slice");
        }

        let mut buf = Self::new(cap)?;
        buf.inner.extend_from_slice(items);
        Ok(buf)
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        self.inner.as_slice()
    }

    #[inline(always)]
    pub fn as_slice_mut(&mut self) -> &mut [T] {
        self.inner.as_mut_slice()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline(always)]
    pub fn available(&self) -> usize {
        self.capacity() - self.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn try_extend<I>(&mut self, iter: I) -> Result<(), VectorError<Vec<T>>>
    where
        I: IntoIterator<Item = T>,
    {
        let items = iter.into_iter().collect::<Vec<_>>();

        if items.len() + self.len() > self.capacity() {
            return Err(VectorError::new(
                items,
                anyhow::anyhow!("Vector buffer is too small"),
            ));
        }

        self.inner.extend(items);
        Ok(())
    }

    pub fn try_extend_exact<I>(&mut self, iter: I) -> Result<(), VectorError<I::IntoIter>>
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: ExactSizeIterator,
    {
        let iter = iter.into_iter();

        if self.len() + iter.len() > self.capacity() {
            return Err(VectorError::new(
                iter,
                anyhow::anyhow!("Vector buffer is too small for iterator"),
            ));
        }

        self.inner.extend(iter);
        Ok(())
    }

    pub fn try_extend_from_slice(&mut self, items: &[T]) -> Result<()>
    where
        T: Copy,
    {
        if self.len() + items.len() > self.capacity() {
            anyhow::bail!("Vector buffer is too small for slice");
        }

        self.inner.extend_from_slice(items);
        Ok(())
    }

    pub fn try_push(&mut self, item: T) -> Result<(), VectorError<T>> {
        if self.is_full() {
            return Err(VectorError::new(
                item,
                anyhow::anyhow!("Vector buffer is full"),
            ));
        }

        self.inner.push(item);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop()
    }

    pub fn try_insert(&mut self, index: usize, item: T) -> Result<(), VectorError<T>> {
        if index > self.len() {
            return Err(VectorError::new(
                item,
                anyhow::anyhow!("Index out of bounds"),
            ));
        }

        if self.is_full() {
            return Err(VectorError::new(
                item,
                anyhow::anyhow!("Vector buffer is full"),
            ));
        }

        self.inner.insert(index, item);
        Ok(())
    }

    pub fn remove(&mut self, index: usize) -> Result<T, VectorError<()>> {
        if index >= self.len() {
            return Err(VectorError::new((), anyhow::anyhow!("Index out of bounds")));
        }

        Ok(self.inner.remove(index))
    }

    pub fn swap(&mut self, a: usize, b: usize) {
        self.inner.swap(a, b);
    }

    pub fn swap_remove(&mut self, index: usize) -> T {
        self.inner.swap_remove(index)
    }

    pub fn sort(&mut self)
    where
        T: Ord,
    {
        self.inner.sort();
    }

    pub fn sort_by<F>(&mut self, compare: F)
    where
        F: FnMut(&T, &T) -> std::cmp::Ordering,
    {
        self.inner.sort_by(compare);
    }

    pub fn sort_by_key<K, F>(&mut self, key: F)
    where
        K: Ord,
        F: FnMut(&T) -> K,
    {
        self.inner.sort_by_key(key);
    }

    pub fn sort_unstable(&mut self)
    where
        T: Ord,
    {
        self.inner.sort_unstable();
    }

    pub fn reverse(&mut self) {
        self.inner.reverse();
    }

    pub fn sort_unstable_by<F>(&mut self, compare: F)
    where
        F: FnMut(&T, &T) -> std::cmp::Ordering,
    {
        self.inner.sort_unstable_by(compare);
    }

    pub fn sort_unstable_by_key<K, F>(&mut self, key: F)
    where
        K: Ord,
        F: FnMut(&T) -> K,
    {
        self.inner.sort_unstable_by_key(key);
    }

    pub fn dedup_by<F>(&mut self, same_bucket: F)
    where
        F: FnMut(&mut T, &mut T) -> bool,
    {
        self.inner.dedup_by(same_bucket);
    }

    pub fn dedup_by_key<K, F>(&mut self, key: F)
    where
        K: PartialEq,
        F: FnMut(&mut T) -> K,
    {
        self.inner.dedup_by_key(key);
    }

    pub fn dedup(&mut self)
    where
        T: PartialEq,
    {
        self.inner.dedup();
    }

    pub fn retain<F>(&mut self, keep: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.inner.retain(keep);
    }

    pub fn drain<R>(&mut self, range: R) -> std::vec::Drain<T>
    where
        R: std::ops::RangeBounds<usize>,
    {
        self.inner.drain(range)
    }

    pub fn truncate(&mut self, len: usize) {
        self.inner.truncate(len);
    }
}
