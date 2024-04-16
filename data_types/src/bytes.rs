use core::hash;
use std::{alloc::Layout, ptr::NonNull, sync::Arc};

use anyhow::Result;
use bumpalo::Bump;

pub struct Bytes {
    ptr: NonNull<u8>,
    len: u32,
    cap: u32,
    alloc: Arc<Bump>,
}

impl Bytes {
    pub fn new(cap: u32, alloc: &Arc<Bump>) -> Self {
        let layout = Layout::from_size_align(cap as usize, 1).expect("Invalid layout");
        let alloc = Arc::clone(alloc);
        let ptr = alloc.alloc_layout(layout);
        Self {
            ptr,
            len: 0,
            cap,
            alloc,
        }
    }

    pub unsafe fn from_parts(ptr: NonNull<u8>, len: u32, cap: u32, alloc: Arc<Bump>) -> Self {
        Self {
            ptr,
            len,
            cap,
            alloc,
        }
    }

    pub fn into_parts(self) -> (NonNull<u8>, u32, u32, Arc<Bump>) {
        (self.ptr, self.len, self.cap, self.alloc)
    }

    pub fn from_slice(bytes: &[u8], cap: u32, alloc: &Arc<Bump>) -> Result<Self> {
        if bytes.len() > cap as usize {
            anyhow::bail!("Bytes buffer is too small for slice");
        }

        unsafe { Ok(Self::from_slice_unchecked(bytes, cap, alloc)) }
    }

    pub unsafe fn from_slice_unchecked(bytes: &[u8], cap: u32, alloc: &Arc<Bump>) -> Self {
        let mut buf = Self::new(cap, alloc);
        buf.push_bytes(bytes)
            .expect("Failed to create Bytes from slice");
        buf
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len as usize) }
    }

    #[inline(always)]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len as usize) }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.cap as usize
    }

    #[inline(always)]
    fn available(&self) -> usize {
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
        self.len = 0;
    }

    pub fn alloc(&self) -> &Arc<Bump> {
        &self.alloc
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let val_len = bytes.len();

        if val_len > self.available() {
            return Err(anyhow::anyhow!("Bytes buffer is full"));
        }

        unsafe {
            let dst = self.ptr.as_ptr().add(self.len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, val_len);
            self.len += val_len as u32;
        }

        Ok(())
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl std::ops::DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice_mut()
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for Bytes {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_slice())
    }
}

impl serde::Serialize for Bytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.as_slice())
    }
}

impl Clone for Bytes {
    fn clone(&self) -> Self {
        let mut new = Self::new(self.cap, &self.alloc);
        new.push_bytes(self.as_slice())
            .expect("Failed to clone Bytes");

        new
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Bytes {}

impl PartialOrd for Bytes {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.len().partial_cmp(&other.len())
    }
}

impl Ord for Bytes {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.len().cmp(&other.len())
    }
}

impl hash::Hash for Bytes {
    fn hash<H: hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}
