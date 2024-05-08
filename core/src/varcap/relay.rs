use std::{collections::BTreeMap, num::NonZeroUsize, ops::RangeBounds};

use anyhow::Result;

use primitives::{
    byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
    shared_object::{SharedObject, SharedObjectReadGuard, SharedObjectWriteGuard},
    Idx, ThinIdx,
};

use crate::{
    indices::CellIdx,
    object_ids::{ColumnId, RecordId, TableId},
    slot::{SlotHandle, SlotTuple},
    store::{inner::StoreInner, InsertError, InsertState, StoreConfig, StoreMeta},
    values::DataValue,
};

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VarcapRelay {
    column: usize,
    cell: CellIdx,
}

impl IntoBytes for VarcapRelay {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.column)?;
        x.encode(self.cell)?;
        Ok(())
    }
}

impl FromBytes for VarcapRelay {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.column)?;
        x.decode(&mut this.cell)?;
        Ok(())
    }
}

impl VarcapRelay {
    pub fn new(column: usize, cell: CellIdx) -> Self {
        Self { column, cell }
    }

    pub fn column(&self) -> usize {
        self.column
    }

    pub fn cell(&self) -> CellIdx {
        self.cell
    }
}
