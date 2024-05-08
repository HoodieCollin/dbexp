use anyhow::Result;
use primitives::byte_encoding::{AccessBytes, ScalarFromBytes};
use primitives::{oid::O32, ExpectedType};
use serde::{Deserialize, Serialize};

use super::TableId;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnId(O32, TableId, ExpectedType);

impl AccessBytes for ColumnId {
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

impl ScalarFromBytes for ColumnId {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl std::fmt::Debug for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !f.alternate() {
            f.debug_struct("ColumnId")
                .field("id", &self.0.to_string())
                .field("table", &self.1.to_string())
                .field("kind", &self.2.into_base62())
                .finish()
        } else {
            f.debug_struct("ColumnId")
                .field("id", &self.0)
                .field("table", &self.1)
                .field("kind", &self.2)
                .finish()
        }
    }
}

impl ColumnId {
    pub fn new(table: TableId, kind: impl Into<ExpectedType>) -> Self {
        Self(O32::new(), table, kind.into())
    }

    pub fn into_array(&self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[..4].copy_from_slice(&self.0.into_array());
        bytes[4..8].copy_from_slice(&self.1.into_array());
        bytes[8..].copy_from_slice(&self.2.into_array());
        bytes
    }

    pub fn from_array(bytes: [u8; 16]) -> Option<Self> {
        Some(Self(
            O32::from_array(bytes[..4].try_into().unwrap())?,
            TableId::from_array(bytes[4..8].try_into().unwrap())?,
            ExpectedType::from_array(bytes[8..].try_into().unwrap())?,
        ))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 16]>) -> Result<Self> {
        match bytes.try_into() {
            Ok(bytes) => Ok(Self(
                O32::try_from_array(&bytes[..4])?,
                TableId::try_from_array(&bytes[4..8])?,
                ExpectedType::try_from_array(&bytes[8..])?,
            )),
            Err(_) => Err(anyhow::anyhow!("Invalid length")),
        }
    }

    pub fn table(&self) -> TableId {
        self.1
    }

    pub fn kind(&self) -> ExpectedType {
        self.2
    }
}
