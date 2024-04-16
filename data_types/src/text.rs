use std::{ptr::NonNull, sync::Arc};

use super::bytes::Bytes;
use anyhow::Result;
use bumpalo::Bump;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Text(Bytes);

impl Text {
    pub fn new(cap: u32, alloc: &Arc<Bump>) -> Self {
        Self(Bytes::new(cap, alloc))
    }

    pub fn try_clone_with_capacity(&self, cap: u32, alloc: Option<&Arc<Bump>>) -> Result<Self> {
        let mut new = Self(Bytes::new(cap, alloc.unwrap_or(self.alloc())));
        new.try_push_str(self.as_str())?;
        Ok(new)
    }

    pub unsafe fn from_parts(ptr: NonNull<u8>, len: u32, cap: u32, alloc: Arc<Bump>) -> Self {
        Self(Bytes::from_parts(ptr, len, cap, alloc))
    }

    pub fn into_parts(self) -> (NonNull<u8>, u32, u32, Arc<Bump>) {
        self.0.into_parts()
    }

    pub fn try_from_str(value: &str, cap: u32, alloc: &Arc<Bump>) -> Result<Self> {
        if value.len() > cap as usize {
            anyhow::bail!("Text buffer is too small for string");
        }

        Ok(Self(Bytes::try_from_slice(value.as_bytes(), cap, alloc)?))
    }

    pub fn try_from_i128(value: i128, cap: u32, alloc: &Arc<Bump>) -> Result<Self> {
        let mut num = itoa::Buffer::new();
        let value = num.format(value);

        if value.len() > cap as usize {
            anyhow::bail!("Text buffer is too small for this i128");
        }

        let mut buf = Self::new(cap, alloc);
        buf.try_push_str(value)?;
        Ok(buf)
    }

    pub fn try_from_f64(value: f64, cap: u32, alloc: &Arc<Bump>) -> Result<Self> {
        let mut num = ryu::Buffer::new();
        let value = num.format(value);

        if value.len() > cap as usize {
            anyhow::bail!("Text buffer is too small for this f64");
        }

        let mut buf = Self::new(cap, alloc);
        buf.try_push_str(value)?;
        Ok(buf)
    }

    pub fn try_from_slice(bytes: &[u8], cap: u32, alloc: &Arc<Bump>) -> Result<Self> {
        if bytes.len() > cap as usize {
            anyhow::bail!("Text buffer is too small for slice");
        }

        // SAFETY: bytes is guaranteed to be valid UTF-8
        std::str::from_utf8(bytes)?;

        Ok(Self(Bytes::try_from_slice(bytes, cap, alloc)?))
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.0.as_ptr()
    }

    #[inline(always)]
    pub fn as_str(&self) -> &str {
        unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                self.as_ptr() as _,
                self.len(),
            ))
        }
    }

    #[inline(always)]
    pub fn as_str_mut(&mut self) -> &mut str {
        unsafe {
            std::str::from_utf8_unchecked_mut(std::slice::from_raw_parts_mut(
                self.as_ptr(),
                self.len(),
            ))
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    #[inline(always)]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_ptr(), self.len()) }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn available(&self) -> usize {
        self.capacity() - self.len()
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn alloc(&self) -> &Arc<Bump> {
        self.0.alloc()
    }

    pub fn try_push_str(&mut self, value: &str) -> Result<()> {
        self.0.try_push_bytes(value.as_bytes())
    }
}

impl std::ops::Deref for Text {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::ops::DerefMut for Text {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_str_mut()
    }
}

impl AsRef<str> for Text {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsMut<str> for Text {
    fn as_mut(&mut self) -> &mut str {
        self.as_str_mut()
    }
}

impl AsRef<[u8]> for Text {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for Text {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl std::fmt::Debug for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_str())
    }
}

impl std::fmt::Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl serde::Serialize for Text {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl std::fmt::Write for Text {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.try_push_str(s).map_err(|_| std::fmt::Error)
    }
}
