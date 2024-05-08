use anyhow::Result;
use primitives::idx::MaybeThinIdx;

use crate::{block::Block, object_ids::RecordId};

use super::{
    data::{SlotDataMut, SlotDataRef},
    SlotTuple,
};

pub struct SlotHandle<T: 'static> {
    pub block: Block<T>,
    pub idx: MaybeThinIdx,
}

impl<T> SlotHandle<T> {
    pub fn ensure_idx_has_gen(self) -> Self {
        let SlotHandle { block, idx } = self;
        Self {
            block,
            idx: idx.into_upgraded(),
        }
    }

    pub fn erase_idx_gen(self) -> Self {
        let SlotHandle { block, idx } = self;
        Self {
            block,
            idx: idx.into_downgraded(),
        }
    }

    #[must_use]
    pub fn read_with<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(SlotDataRef<'_, T>) -> Result<R>,
    {
        let outer = self.block.inner.read_recursive();
        let slot = SlotDataRef::new(&outer.slots_by_index[self.idx]);

        if let Some(expected_gen) = self.idx.into_gen() {
            slot.check_gen(expected_gen)?;
        }

        f(slot)
    }

    #[must_use]
    pub fn write_with<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(SlotDataMut<'_, T>) -> Result<R>,
    {
        let outer = self.block.inner.read_recursive();
        let slot = SlotDataMut::new(&outer.slots_by_index[self.idx]);

        if let Some(expected_gen) = self.idx.into_gen() {
            slot.check_gen(expected_gen)?;
        }

        f(slot)
    }

    #[must_use]
    pub fn remove_self(self) -> Option<SlotTuple<T>> {
        let mut outer = self.block.inner.write();
        let prev_tail = outer.meta.gap_tail;

        let (record, data) = {
            let mut slot = SlotDataMut::new(&outer.slots_by_index[self.idx]);

            if let Some(expected_gen) = self.idx.into_gen() {
                // When generation id is invalid, it means the slot is no longer owned by this handle and we can't remove it.
                slot.check_gen(expected_gen).ok()?;
            }

            let (record, data) = unsafe { slot.read_parts()? };
            slot.create_gap(prev_tail);

            (record, data)
        };

        outer.meta.gap_tail = Some(self.idx.into_thin());
        outer.meta.gap_count += 1;

        let record = if let Some(thin) = record {
            outer.index_by_record.shift_remove(&thin);
            Some(RecordId::from_thin(thin, outer.meta.table))
        } else {
            None
        };

        Some((record, data))
    }
}

impl<T> Clone for SlotHandle<T> {
    fn clone(&self) -> Self {
        Self {
            block: self.block.clone(),
            idx: self.idx,
        }
    }
}

impl<T> PartialEq for SlotHandle<T> {
    fn eq(&self, other: &Self) -> bool {
        self.idx == other.idx
    }
}

impl<T> Eq for SlotHandle<T> {}

impl<T> PartialOrd for SlotHandle<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.idx.into_gen() != other.idx.into_gen() {
            None
        } else {
            Some(self.idx.into_u64().cmp(&other.idx.into_u64()))
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SlotHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SlotHandle");

        d.field("idx", &self.idx);

        let res = self.read_with(|data| {
            d.field("valid", &!data.is_gap()).field("data", &data);
            Ok(())
        });

        match res {
            Ok(_) => d.finish(),
            Err(e) => d.field("valid", &false).field("error", &e).finish(),
        }
    }
}
