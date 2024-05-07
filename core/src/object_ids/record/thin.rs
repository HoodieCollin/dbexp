use anyhow::Result;
use primitives::byte_encoding::{AccessBytes, ScalarFromBytes};
use primitives::idx::Idx;
use primitives::{ThinIdx, O16};
use serde::{Deserialize, Serialize};

use super::RecordId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ThinRecordId(pub(self) Idx);

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

    pub fn new(n: ThinIdx) -> Self {
        Self(Idx::from_thin(n))
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

    pub fn gen_id(&self) -> O16 {
        self.0.into_gen_id()
    }
}
