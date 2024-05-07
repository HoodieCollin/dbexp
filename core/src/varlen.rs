use std::{num::NonZeroUsize, ops::RangeBounds};

use anyhow::Result;

use primitives::{
    byte_encoding::IntoBytes,
    shared_object::{SharedObject, SharedObjectReadGuard, SharedObjectWriteGuard},
    Idx, ThinIdx,
};

use crate::{
    block::{self, Block, BlockConfig},
    object_ids::{RecordId, TableId},
    slot::{SlotHandle, SlotTuple},
    store::{inner::StoreInner, InsertError, InsertState, StoreConfig, StoreMeta},
};
