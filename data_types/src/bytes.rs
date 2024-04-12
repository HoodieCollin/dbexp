use anyhow::Result;
use bumpalo::collections::Vec as BumpVec;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bytes<'bump> {
    inner: BumpVec<'bump>,
    max_capacity: u32,
}

impl<'bump> Bytes<'bump> {
    pub fn new(bump: &'bump bumpalo::Bump, value: &[u8], max_capacity: u32) -> Result<Self> {
        if value.len() > max_capacity as usize {
            return Err(anyhow::anyhow!("Bytes buffer is full"));
        }

        let mut bytes = BumpVec::with_capacity_in(value.len(), bump);
        bytes.extend_from_slice(value);

        Ok(Self {
            inner: bytes,
            max_capacity,
        })
    }

    #[inline(always)]
    pub fn from_bytes(bump: &'bump bumpalo::Bump, bytes: &[u8], max_capacity: u32) -> Result<Self> {
        Self::new(bump, bytes, max_capacity)
    }

    pub fn into_bytes(self) -> BumpVec<'bump, u8> {
        self.inner
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

    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.available() {
            return Err(anyhow::anyhow!("Bytes buffer is full"));
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

impl<'bump> std::ops::Deref for Bytes<'bump> {
    type Target = BumpVec<'bump>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'bump> std::fmt::Debug for Bytes<'bump> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<'bump> std::fmt::Display for Bytes<'bump> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl<'bump> serde::Serialize for Bytes<'bump> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.inner)
    }
}
