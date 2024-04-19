use anyhow::Result;
use primitives::buffer::fixed::FixedBuffer;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[repr(transparent)]
pub struct Bytes(pub(crate) FixedBuffer<u8>);

impl Bytes {
    pub fn new(cap: u32) -> Self {
        Self(FixedBuffer::new(cap))
    }

    pub fn try_from_str(value: &str, cap: u32) -> Result<Self> {
        if value.len() > cap as usize {
            anyhow::bail!("Bytes buffer is too small for string");
        }

        let mut buf = Self::new(cap);
        buf.0.extend_from_slice(value.as_bytes())?;
        Ok(buf)
    }

    pub fn try_from_slice(bytes: &[u8], cap: u32) -> Result<Self> {
        if bytes.len() > cap as usize {
            anyhow::bail!("Bytes buffer is too small for slice");
        }

        let mut buf = Self::new(cap);
        buf.0.extend_from_slice(bytes)?;
        Ok(buf)
    }

    pub fn try_from_i128(value: i128, cap: u32) -> Result<Self> {
        if cap < 16 {
            return Err(anyhow::anyhow!("Buffer is too small for i128"));
        }

        let mut buf = Self::new(cap);
        buf.0.extend_from_slice(&value.to_ne_bytes())?;
        Ok(buf)
    }

    pub fn try_from_f64(value: f64, cap: u32) -> Result<Self> {
        if cap < 8 {
            return Err(anyhow::anyhow!("Buffer is too small for f64"));
        }

        let mut buf = Self::new(cap);
        buf.0.extend_from_slice(&value.to_ne_bytes())?;
        Ok(buf)
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn available(&self) -> u32 {
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
        self.0.extend_from_slice(bytes.as_ref())
    }
}

impl std::ops::Deref for Bytes {
    type Target = FixedBuffer<u8>;

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

impl AsRef<FixedBuffer<u8>> for Bytes {
    fn as_ref(&self) -> &FixedBuffer<u8> {
        &self.0
    }
}

impl AsMut<FixedBuffer<u8>> for Bytes {
    fn as_mut(&mut self) -> &mut FixedBuffer<u8> {
        &mut self.0
    }
}
