use anyhow::Result;
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLockReadGuard, RwLockWriteGuard,
};
use primitives::Idx;

use crate::{block::Block, object_ids::RecordId};

use super::{
    data::{SlotData, SlotDataRef},
    SlotTuple,
};

pub struct SlotHandle<T: 'static> {
    pub block: Block<T>,
    pub idx: Idx,
}

impl<T> SlotHandle<T> {
    #[must_use]
    pub fn read_with<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(MappedRwLockReadGuard<'_, SlotData<T>>) -> Result<R>,
    {
        let (expected_gen, slot_index) = self.idx.into_parts();
        let outer = self.block.inner.read_recursive();
        let inner = outer.slots_by_index[slot_index].read();

        SlotDataRef::new(&inner).check_gen(expected_gen)?;

        let guard = RwLockReadGuard::map(inner, |ptr| unsafe { ptr.as_ref() });

        f(guard)
    }

    #[must_use]
    pub fn write_with<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(MappedRwLockWriteGuard<'_, SlotData<T>>) -> Result<R>,
    {
        let (expected_gen, slot_index) = self.idx.into_parts();
        let outer = self.block.inner.read_recursive();
        let inner = outer.slots_by_index[slot_index].write();

        SlotDataRef::new(&inner).check_gen(expected_gen)?;

        let guard = RwLockWriteGuard::map(inner, |ptr| unsafe { ptr.as_mut() });

        f(guard)
    }

    #[must_use]
    pub fn remove_self(self) -> Result<SlotTuple<T>> {
        let mut outer = self.block.inner.write();
        let prev_tail = outer.meta.gap_tail;

        unsafe {
            let (record, data) = {
                let (expected_gen, slot_index) = self.idx.into_parts();
                let mut inner = outer.slots_by_index[slot_index].write();

                SlotDataRef::new(&inner).check_gen(expected_gen)?;

                let slot = inner.as_mut();
                let data = slot.read_data_unchecked();
                let record = slot.thin_record_id();

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

            Ok((record, data))
        }
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
        if self.idx.into_gen_id() != other.idx.into_gen_id() {
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
