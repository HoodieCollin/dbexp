use anyhow::Result;
use primitives::byte_encoding::{AccessBytes, ScalarFromBytes};
use primitives::{
    oid::{self},
    ExpectedType,
};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct TableId(oid::O32);

impl AccessBytes for TableId {
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

impl ScalarFromBytes for TableId {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl TableId {
    pub const INVALID: Self = Self(oid::O32::INVALID);
    pub const SENTINEL: Option<Self> = None;

    pub fn new() -> Self {
        Self(oid::O32::new())
    }

    pub fn into_array(&self) -> [u8; 4] {
        self.0.into_array()
    }

    pub fn from_array(bytes: [u8; 4]) -> Option<Self> {
        Some(Self(oid::O32::from_array(bytes)?))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 4]>) -> Result<Self> {
        Ok(Self(oid::O32::try_from_array(bytes)?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ThinRecordId(pub(self) oid::O32);

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

impl ThinRecordId {
    pub const INVALID: Self = Self(oid::O32::INVALID);
    pub const SENTINEL: Option<Self> = None;

    pub fn new() -> Self {
        Self(oid::O32::new())
    }

    pub fn from_array(bytes: [u8; 4]) -> Option<Self> {
        Some(Self(oid::O32::from_array(bytes)?))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 4]>) -> Result<Self> {
        Ok(Self(oid::O32::try_from_array(bytes)?))
    }

    pub fn into_array(self) -> [u8; 4] {
        self.0.into_array()
    }
}

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

impl RecordId {
    pub const INVALID: Self = Self(ThinRecordId::INVALID, TableId::INVALID);
    pub const SENTINEL: Option<Self> = None;

    pub fn new(table: TableId) -> Self {
        Self(ThinRecordId::new(), table)
    }

    pub fn table(&self) -> TableId {
        self.1
    }

    pub fn from_thin(thin: ThinRecordId, table: TableId) -> Self {
        Self(thin, table)
    }

    pub fn from_array(bytes: [u8; 8]) -> Option<Self> {
        let thin = ThinRecordId::from_array(bytes[..4].try_into().ok()?)?;
        let table = TableId::from_array(bytes[4..].try_into().ok()?)?;

        Some(Self(thin, table))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 8]>) -> Result<Self> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid value"))?;

        let thin = ThinRecordId::try_from_array(&bytes[..4])?;
        let table = TableId::try_from_array(&bytes[4..])?;

        Ok(Self(thin, table))
    }

    pub fn into_array(self) -> [u8; 8] {
        let mut bytes = [0; 8];
        bytes[..4].copy_from_slice(&self.0.into_array());
        bytes[4..].copy_from_slice(&self.1.into_array());
        bytes
    }

    pub fn into_thin(self) -> ThinRecordId {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnId(oid::O32, TableId, ExpectedType);

impl ColumnId {
    pub fn new(table: TableId, kind: impl Into<ExpectedType>) -> Self {
        Self(oid::O32::new(), table, kind.into())
    }

    pub fn table(&self) -> TableId {
        self.1
    }

    pub fn kind(&self) -> ExpectedType {
        self.2
    }
}
