use std::mem::MaybeUninit;

use anyhow::Result;
use parking_lot::{Once, OnceState};
use primitives::byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes};

use crate::{
    column_indices::CellIdx,
    object_ids::{ColumnId, RecordId},
    values::DataValue,
};

pub mod data;
pub mod handle;

pub use {
    data::{SlotData, SlotDataRef},
    handle::SlotHandle,
};

pub(super) const GAP_HEAD: usize = usize::MAX;

pub type SlotTuple<T> = (Option<RecordId>, T);

static mut DATA_LOOKUP_FN: MaybeUninit<fn(SlotIndirection) -> Option<DataValue>> =
    MaybeUninit::uninit();
static DATA_LOOKUP_FN_INIT: Once = Once::new();

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotIndirection {
    column: ColumnId,
    cell: CellIdx,
}

impl IntoBytes for SlotIndirection {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.column)?;
        x.encode(self.cell)?;
        Ok(())
    }
}

impl FromBytes for SlotIndirection {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.column)?;
        x.decode(&mut this.cell)?;
        Ok(())
    }
}

impl PartialEq<DataValue> for SlotIndirection {
    fn eq(&self, other: &DataValue) -> bool {
        if let Ok(Some(value)) = self.try_data_lookup() {
            return value == *other;
        }

        false
    }
}

impl PartialEq<SlotIndirection> for DataValue {
    fn eq(&self, other: &SlotIndirection) -> bool {
        other == self
    }
}

impl PartialEq<Option<DataValue>> for SlotIndirection {
    fn eq(&self, other: &Option<DataValue>) -> bool {
        if let Ok(value) = self.try_data_lookup() {
            return value == *other;
        }

        other.is_none()
    }
}

impl PartialEq<SlotIndirection> for Option<DataValue> {
    fn eq(&self, other: &SlotIndirection) -> bool {
        other == self
    }
}

impl PartialOrd<DataValue> for SlotIndirection {
    fn partial_cmp(&self, other: &DataValue) -> Option<std::cmp::Ordering> {
        if let Ok(Some(value)) = self.try_data_lookup() {
            return value.partial_cmp(other);
        }

        None
    }
}

impl PartialOrd<SlotIndirection> for DataValue {
    fn partial_cmp(&self, other: &SlotIndirection) -> Option<std::cmp::Ordering> {
        other.partial_cmp(self).map(|o| o.reverse())
    }
}

impl PartialOrd<Option<DataValue>> for SlotIndirection {
    fn partial_cmp(&self, other: &Option<DataValue>) -> Option<std::cmp::Ordering> {
        if let Ok(value) = self.try_data_lookup() {
            return value.partial_cmp(other);
        }

        None
    }
}

impl PartialOrd<SlotIndirection> for Option<DataValue> {
    fn partial_cmp(&self, other: &SlotIndirection) -> Option<std::cmp::Ordering> {
        other.partial_cmp(self).map(|o| o.reverse())
    }
}

impl PartialOrd<SlotIndirection> for SlotIndirection {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let a = if let Ok(value) = self.try_data_lookup() {
            value
        } else {
            None
        };

        let b = if let Ok(value) = other.try_data_lookup() {
            value
        } else {
            None
        };

        match (a, b) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            (Some(_), None) => Some(std::cmp::Ordering::Greater),
            (None, Some(_)) => Some(std::cmp::Ordering::Less),
            (None, None) => Some(std::cmp::Ordering::Equal),
        }
    }
}

impl Ord for SlotIndirection {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::hash::Hash for SlotIndirection {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Ok(Some(value)) = self.try_data_lookup() {
            value.hash(state);
        } else {
            // should we error here?
        }
    }
}

impl SlotIndirection {
    pub fn new(column: ColumnId, cell: CellIdx) -> Self {
        Self { column, cell }
    }

    pub fn column(&self) -> ColumnId {
        self.column
    }

    pub fn cell(&self) -> CellIdx {
        self.cell
    }

    pub fn init_data_lookup_fn(fn_ptr: fn(SlotIndirection) -> Option<DataValue>) {
        unsafe {
            DATA_LOOKUP_FN_INIT.call_once(|| {
                DATA_LOOKUP_FN = MaybeUninit::new(fn_ptr);
            });

            if DATA_LOOKUP_FN_INIT.state() == OnceState::Poisoned {
                std::process::abort();
            }
        }
    }

    pub fn try_data_lookup(&self) -> Result<Option<DataValue>> {
        loop {
            match DATA_LOOKUP_FN_INIT.state() {
                OnceState::Done => break,
                OnceState::InProgress => {
                    std::hint::spin_loop();
                    continue;
                }
                OnceState::New => {
                    return Err(anyhow::anyhow!("Data lookup function not initialized"));
                }
                OnceState::Poisoned => {
                    unreachable!("Data lookup function will abort process if failed to initialize")
                }
            }
        }

        Ok(unsafe { DATA_LOOKUP_FN.assume_init()(self.clone()) })
    }
}
