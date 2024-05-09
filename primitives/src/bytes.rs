use anyhow::Result;
use serde::Serialize;

use crate::Vector;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Bytes(pub(crate) Vector<u8>);

impl Bytes {
    pub const MAX_LEN: usize = Vector::<u8>::MAX_LEN;

    #[must_use]
    pub fn new(cap: usize) -> Result<Self> {
        if cap > Self::MAX_LEN {
            anyhow::bail!("Bytes buffer capacity is too large");
        }

        Ok(Self(Vector::new(cap)?))
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        self.0.as_slice_mut()
    }

    #[must_use]
    pub fn try_from_str(value: &str, cap: usize) -> Result<Self> {
        if value.len() > cap {
            anyhow::bail!("Bytes buffer is too small for string");
        }

        let mut buf = Self::new(cap)?;
        buf.0.try_extend_from_slice(value.as_bytes())?;
        Ok(buf)
    }

    #[must_use]
    pub fn try_from_slice(bytes: &[u8], cap: usize) -> Result<Self> {
        if bytes.len() > cap {
            anyhow::bail!("Bytes buffer is too small for slice");
        }

        let mut buf = Self::new(cap)?;
        buf.0.try_extend_from_slice(bytes)?;
        Ok(buf)
    }

    #[must_use]
    pub fn try_from_i128(value: i128, cap: usize) -> Result<Self> {
        if cap < 16 {
            return Err(anyhow::anyhow!("Buffer is too small for i128"));
        }

        let mut buf = Self::new(cap)?;
        buf.0.try_extend_from_slice(&value.to_ne_bytes())?;
        Ok(buf)
    }

    #[must_use]
    pub fn try_from_f64(value: f64, cap: usize) -> Result<Self> {
        if cap < 8 {
            return Err(anyhow::anyhow!("Buffer is too small for f64"));
        }

        let mut buf = Self::new(cap)?;
        buf.0.try_extend_from_slice(&value.to_ne_bytes())?;
        Ok(buf)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
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
        self.0.clear();
    }

    pub fn try_push_bytes(&mut self, bytes: impl AsRef<[u8]>) -> Result<()> {
        if self.available() < bytes.as_ref().len() {
            anyhow::bail!("Bytes buffer is full");
        }

        Ok(self.0.try_extend_from_slice(bytes.as_ref())?)
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0.iter() {
            write!(f, "{:02x}", byte)?;
        }

        Ok(())
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl AsMut<[u8]> for Bytes {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_slice_mut()
    }
}
