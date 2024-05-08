use anyhow::Result;
use primitives::byte_encoding::{AccessBytes, ScalarFromBytes};
use primitives::idx::{Gen, Idx};
use primitives::ThinIdx;
use serde::{Deserialize, Serialize};

use super::RecordId;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ThinRecordId(pub(super) Idx);

impl AccessBytes for ThinRecordId {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let bytes = self.into_array();
        f(&bytes)
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        let mut bytes = self.into_array();
        Ok(Some(f(&mut bytes)?))
    }
}

impl ScalarFromBytes for ThinRecordId {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl std::fmt::Debug for ThinRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !f.alternate() {
            write!(f, "ThinRecordId({})", self.to_string())
        } else {
            f.debug_struct("ThinRecordId")
                .field("gen", &self.gen().into_raw())
                .field("index", &self.0.into_usize())
                .finish()
        }
    }
}

impl std::fmt::Display for ThinRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::with_capacity(32);
        base62::encode_buf(self.0.into_gen().into_raw().into_u64(), &mut s);
        base62::encode_buf(self.0.into_u64(), &mut s);
        write!(f, "{}", s)
    }
}

impl Into<Idx> for ThinRecordId {
    fn into(self) -> Idx {
        self.0
    }
}

impl Into<ThinIdx> for ThinRecordId {
    fn into(self) -> ThinIdx {
        self.0.into()
    }
}

impl From<RecordId> for ThinRecordId {
    fn from(record: RecordId) -> Self {
        Self::from_record(record)
    }
}

impl ThinRecordId {
    pub const INVALID: Self = Self(Idx::INVALID);
    pub const NIL: Option<Self> = None;

    pub fn new(n: impl Into<ThinIdx>) -> Self {
        Self(Idx::from_thin(n.into()))
    }

    pub fn from_record(record: RecordId) -> Self {
        record.0
    }

    pub fn from_array(bytes: [u8; 8]) -> Option<Self> {
        Some(Self(Idx::from_array(bytes)?))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 8]>) -> Result<Self> {
        Ok(Self(Idx::try_from_array(bytes)?))
    }

    pub fn into_array(self) -> [u8; 8] {
        self.0.into_array()
    }

    pub fn gen(&self) -> Gen {
        self.0.into_gen()
    }

    pub fn as_u64(&self) -> u64 {
        self.0.into_u64()
    }

    pub fn as_usize(&self) -> usize {
        self.0.into_usize()
    }
}
