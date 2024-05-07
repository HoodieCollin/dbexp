use anyhow::Result;
use primitives::byte_encoding::{AccessBytes, ScalarFromBytes};
use primitives::oid::O32;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct TableId(O32);

impl AccessBytes for TableId {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        f(&self.into_array())
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        Ok(Some(f(&mut self.into_array())?))
    }
}

impl ScalarFromBytes for TableId {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl TableId {
    pub const INVALID: Self = Self(O32::INVALID);
    pub const NIL: Option<Self> = None;

    pub fn new() -> Self {
        Self(O32::new())
    }

    pub fn into_array(&self) -> [u8; 4] {
        self.0.into_array()
    }

    pub fn from_array(bytes: [u8; 4]) -> Option<Self> {
        Some(Self(O32::from_array(bytes)?))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 4]>) -> Result<Self> {
        Ok(Self(O32::try_from_array(bytes)?))
    }
}
