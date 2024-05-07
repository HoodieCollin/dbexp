use anyhow::Result;
use primitives::byte_encoding::{AccessBytes, ScalarFromBytes};
use primitives::idx::Idx;
use primitives::{ThinIdx, O16};
use serde::{Deserialize, Serialize};

use super::{TableId, ThinRecordId};

pub mod thin;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordId(ThinRecordId, TableId);

impl AccessBytes for RecordId {
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

impl ScalarFromBytes for RecordId {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl Into<Idx> for RecordId {
    fn into(self) -> Idx {
        self.0.into()
    }
}

impl Into<ThinIdx> for RecordId {
    fn into(self) -> ThinIdx {
        self.0.into()
    }
}

impl RecordId {
    pub const INVALID: Self = Self(ThinRecordId::INVALID, TableId::INVALID);
    pub const NIL: Option<Self> = None;

    pub fn new(n: ThinIdx, table: TableId) -> Self {
        Self(ThinRecordId::new(n), table)
    }

    pub fn table(&self) -> TableId {
        self.1
    }

    pub fn from_thin(thin: ThinRecordId, table: TableId) -> Self {
        Self(thin, table)
    }

    pub fn from_array(bytes: [u8; 12]) -> Option<Self> {
        let thin = ThinRecordId::from_array(bytes[..8].try_into().ok()?)?;
        let table = TableId::from_array(bytes[8..].try_into().ok()?)?;

        Some(Self(thin, table))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 12]>) -> Result<Self> {
        let bytes: [u8; 12] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid value"))?;

        let thin = ThinRecordId::try_from_array(&bytes[..8])?;
        let table = TableId::try_from_array(&bytes[8..])?;

        Ok(Self(thin, table))
    }

    pub fn into_array(self) -> [u8; 12] {
        let mut bytes = [0; 12];
        bytes[..8].copy_from_slice(&self.0.into_array());
        bytes[8..].copy_from_slice(&self.1.into_array());
        bytes
    }

    pub fn into_thin(self) -> ThinRecordId {
        self.0
    }

    pub fn gen_id(&self) -> O16 {
        self.0.gen_id()
    }
}
