use anyhow::Result;
use bumpalo::collections::{String as BumpString, Vec as BumpVec};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Text<'bump> {
    inner: BumpString<'bump>,
    max_capacity: u32,
}

impl<'bump> Text<'bump> {
    pub fn new(bump: &'bump bumpalo::Bump, value: &str, max_capacity: u32) -> Result<Self> {
        if value.len() > max_capacity as usize {
            return Err(anyhow::anyhow!("Text buffer is full"));
        }

        let mut inner = BumpString::with_capacity_in(max_capacity, bump);
        inner.push_str(value);

        Ok(Self {
            inner,
            max_capacity,
        })
    }

    pub fn from_bytes(bump: &'bump bumpalo::Bump, bytes: &[u8], max_capacity: u32) -> Result<Self> {
        if bytes.len() > max_capacity as usize {
            return Err(anyhow::anyhow!("Text buffer is full"));
        }

        let s = std::str::from_utf8(bytes)?;

        let mut inner = BumpString::with_capacity_in(max_capacity, bump);
        inner.push_str(s);

        Ok(Self {
            inner,
            max_capacity,
        })
    }

    pub fn into_bytes(self) -> BumpVec<'bump, u8> {
        self.inner.into_bytes()
    }

    pub fn as_str(&self) -> &str {
        &self.inner
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    fn available(&self) -> usize {
        self.inner.capacity() - self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn push_str(&mut self, value: &str) -> Result<()> {
        if value.len() > self.available() {
            return Err(anyhow::anyhow!("Text buffer is full"));
        }

        self.inner.push_str(value);
    }

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.available() {
            return Err(anyhow::anyhow!("Text buffer is full"));
        }

        self.inner.push_str(std::str::from_utf8(bytes)?);
    }

    pub fn range_mut(&mut self, range: std::ops::Range<usize>) -> Result<&mut str> {
        if range.end > self.len() {
            return Err(anyhow::anyhow!("Index out of bounds"));
        }

        Ok(&mut self.inner[range])
    }
}

impl<'bump> std::ops::Deref for Text<'bump> {
    type Target = BumpString<'bump>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'bump> std::fmt::Debug for Text<'bump> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<'bump> std::fmt::Display for Text<'bump> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<'bump> serde::Serialize for Text<'bump> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.inner)
    }
}
