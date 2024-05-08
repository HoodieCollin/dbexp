pub mod data;
pub mod handle;

use crate::object_ids::RecordId;

pub use {
    data::{SlotData, SlotDataRef},
    handle::SlotHandle,
};

pub(super) const GAP_HEAD: usize = usize::MAX;

pub type SlotTuple<T> = (Option<RecordId>, T);
