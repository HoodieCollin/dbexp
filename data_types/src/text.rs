use super::bytes::Bytes;
use anyhow::Result;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Text(Bytes);

impl Text {
    pub fn new(cap: usize) -> Self {
        Self(Bytes::new(cap))
    }

    pub fn try_from_str(value: &str, cap: usize) -> Result<Self> {
        if value.len() > cap as usize {
            anyhow::bail!("Text buffer is too small for string");
        }

        Ok(Self(Bytes::try_from_slice(value.as_bytes(), cap)?))
    }

    pub fn try_from_slice(bytes: &[u8], cap: usize) -> Result<Self> {
        if bytes.len() > cap as usize {
            anyhow::bail!("Text buffer is too small for slice");
        }

        // SAFETY: bytes is guaranteed to be valid UTF-8
        std::str::from_utf8(bytes)?;

        Ok(Self(Bytes::try_from_slice(bytes, cap)?))
    }

    pub fn try_from_i128(value: i128, cap: usize) -> Result<Self> {
        let mut num = itoa::Buffer::new();
        let value = num.format(value);

        if value.len() > cap as usize {
            anyhow::bail!("Text buffer is too small for this i128");
        }

        let mut buf = Self::new(cap);
        buf.try_push_str(value)?;
        Ok(buf)
    }

    pub fn try_from_f64(value: f64, cap: usize) -> Result<Self> {
        let mut num = ryu::Buffer::new();
        let value = num.format(value);

        if value.len() > cap as usize {
            anyhow::bail!("Text buffer is too small for this f64");
        }

        let mut buf = Self::new(cap);
        buf.try_push_str(value)?;
        Ok(buf)
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    #[inline(always)]
    pub fn available(&self) -> usize {
        self.0.available()
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

    pub fn try_push_str(&mut self, value: impl AsRef<str>) -> Result<()> {
        self.0.try_push_bytes(value.as_ref().as_bytes())
    }

    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    pub fn as_str(&self) -> &str {
        // SAFETY: Text is guaranteed to be valid UTF-8
        unsafe { std::str::from_utf8_unchecked(self.0 .0.as_slice()) }
    }

    pub fn as_str_mut(&mut self) -> &mut str {
        // SAFETY: Text is guaranteed to be valid UTF-8
        unsafe { std::str::from_utf8_unchecked_mut(self.0 .0.as_mut_slice()) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0 .0.as_slice()
    }

    pub fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
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

impl std::fmt::Debug for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl std::fmt::Display for Text {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl serde::Serialize for Text {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}

impl std::fmt::Write for Text {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.try_push_str(s).map_err(|_| std::fmt::Error)
    }
}
