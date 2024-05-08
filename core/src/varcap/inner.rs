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

use super::{config::VarcapConfig, relay::VarcapRelay};

pub struct VarcapInner {
    pub(super) relay: StoreInner<VarcapRelay>,
    pub(super) buckets: BTreeMap<usize, StoreInner<DataValue>>,
}

impl VarcapInner {
    #[must_use]
    pub fn new(table: Option<TableId>, config: VarcapConfig) -> Result<Self> {
        let table = table.unwrap_or_else(|| TableId::new());
        let relay = StoreInner::new(Some(table), Some(config.into()))?;
        let buckets = BTreeMap::new();

        Ok(Self { relay, buckets })
    }
}
