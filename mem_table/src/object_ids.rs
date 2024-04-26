use data_types::{
    oid::{self, ObjectId},
    ExpectedType,
};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct TableId(oid::O32);

impl TableId {
    pub fn new() -> Self {
        Self(oid::O32::new())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordId(oid::O32, TableId);

impl RecordId {
    pub fn new(table: TableId) -> Self {
        Self(oid::O32::new(), table)
    }

    pub fn table(&self) -> TableId {
        self.1
    }

    pub fn from_array(bytes: [u8; 4], table: TableId) -> Self {
        Self(oid::O32::from_array(bytes), table)
    }

    pub fn into_array(self) -> [u8; 4] {
        self.0.into_array()
    }
}

/// This identifier is __NOT__ stable across restarts or even when a cell is unloaded and reloaded.
///
/// There is a global pool of `Cell`s allocated in a slab. When a `Cell` is dropped, it is returned to
/// the pool. When a new `Cell` is needed, it is allocated from the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CellId(oid::O64);

impl CellId {
    pub fn new(id: usize) -> Self {
        Self(oid::O64::from_uint(id as u64))
    }

    pub fn as_usize(&self) -> usize {
        self.0.as_usize()
    }
}
