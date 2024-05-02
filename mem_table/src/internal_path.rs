use std::{ffi::os_str, os::unix::ffi::OsStrExt, path::Path};

use anyhow::Result;

use crate::byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes};

#[derive(Clone, Copy)]
pub struct InternalPath {
    len: usize,
    buf: [u8; 256],
}

impl Default for InternalPath {
    fn default() -> Self {
        Self {
            len: 0,
            buf: [0; 256],
        }
    }
}

impl std::ops::Deref for InternalPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        self.as_path()
    }
}

impl AsRef<Path> for InternalPath {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl AsRef<[u8]> for InternalPath {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for InternalPath {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl std::fmt::Debug for InternalPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_path().fmt(f)
    }
}

impl PartialEq for InternalPath {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for InternalPath {}

impl std::hash::Hash for InternalPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl IntoBytes for InternalPath {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.len)?;
        x.encode_bytes(self.as_slice())?;
        Ok(())
    }
}

impl FromBytes for InternalPath {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.len)?;
        x.read_exact(&mut this.buf[..this.len])?;
        Ok(())
    }
}

impl InternalPath {
    pub fn new(path: &Path) -> Result<Self> {
        let mut buf = [0; 256];
        let path_bytes = path.as_os_str().as_bytes();
        let path_len = path_bytes.len();

        if path_len > 256 {
            anyhow::bail!("path too long");
        }

        buf[..path_len].copy_from_slice(path_bytes);
        Ok(Self { buf, len: path_len })
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_path(&self) -> &Path {
        Path::new(os_str::OsStr::from_bytes(self.as_slice()))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        &mut self.buf[..self.len]
    }

    pub fn replace(&mut self, path: &Path) -> Result<()> {
        let path_bytes = path.as_os_str().as_bytes();
        let path_len = path_bytes.len();

        if path_len > 256 {
            anyhow::bail!("path too long");
        }

        self.len = path_len;
        self.buf[..path_len].copy_from_slice(path_bytes);
        Ok(())
    }
}
