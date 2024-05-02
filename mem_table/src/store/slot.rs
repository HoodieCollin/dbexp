use std::mem::MaybeUninit;

use anyhow::Result;
use data_types::oid;
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, RwLockReadGuard, RwLockWriteGuard,
};

use super::block::{Block, SlotTuple};
use crate::object_ids::{RecordId, ThinRecordId};

pub(super) const GAP_HEAD: usize = usize::MAX;

#[repr(C)]
pub struct SlotData<T>(ThinRecordId, MaybeUninit<T>);

impl<T> SlotData<T> {
    pub fn new(record: RecordId, data: T) -> Self {
        Self(record.into_raw(), MaybeUninit::new(data))
    }

    pub fn is_gap(&self) -> bool {
        self.0.is_sentinel()
    }

    /// Returns the previous gap idx in the chain.
    pub fn previous_gap(&self) -> Option<usize> {
        if self.is_gap() {
            Some(unsafe { self.previous_gap_unchecked() })
        } else {
            None
        }
    }

    pub unsafe fn previous_gap_unchecked(&self) -> usize {
        debug_assert!(self.is_gap());
        std::ptr::read_unaligned(self.1.as_ptr() as *const _)
    }

    pub fn raw_record_id(&self) -> Option<ThinRecordId> {
        if self.is_gap() {
            None
        } else {
            Some(unsafe { self.raw_record_id_unchecked() })
        }
    }

    pub unsafe fn raw_record_id_unchecked(&self) -> ThinRecordId {
        debug_assert!(!self.is_gap());
        self.0
    }

    pub fn data(&self) -> Option<&T> {
        if self.is_gap() {
            None
        } else {
            Some(unsafe { self.data_unchecked() })
        }
    }

    pub unsafe fn data_unchecked(&self) -> &T {
        debug_assert!(!self.is_gap());
        self.1.assume_init_ref()
    }

    pub unsafe fn read_data_unchecked(&self) -> T {
        debug_assert!(!self.is_gap());
        std::ptr::read(self.1.as_ptr())
    }

    /// Blocks the previous gap idx in the chain.
    pub fn create_gap(&mut self, previous_gap: Option<usize>) {
        if self.is_gap() {
            return;
        }

        self.0 = ThinRecordId::SENTINEL;

        unsafe {
            std::ptr::write_unaligned(
                self.1.as_mut_ptr() as *mut _,
                previous_gap.unwrap_or(GAP_HEAD),
            );
        }
    }

    pub fn fill_gap(&mut self, record: ThinRecordId, data: T) {
        #[cfg(debug_assertions)]
        {
            if !self.is_gap() {
                panic!("slot is not a gap");
            }
        }

        self.0 = record;
        self.1 = MaybeUninit::new(data);
    }

    pub fn update_data(&mut self, data: T) {
        #[cfg(debug_assertions)]
        {
            if self.is_gap() {
                panic!("slot is a gap");
            }
        }

        self.1 = MaybeUninit::new(data);
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SlotData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_gap() {
            let mut d = f.debug_struct("Gap");
            let prev = unsafe { self.previous_gap_unchecked() };

            if prev != GAP_HEAD {
                d.field("next", &prev);
            } else {
                d.field("next", &Option::<usize>::None);
            }

            d.finish()
        } else {
            unsafe { std::fmt::Debug::fmt(self.data_unchecked(), f) }
        }
    }
}

pub struct SlotHandle<T: 'static> {
    pub(super) block: Block<T>,
    pub(super) gen: oid::O64,
    pub(super) idx: usize,
}

impl<T> SlotHandle<T> {
    pub fn read_with<F, R>(&self, mut f: F) -> Result<R>
    where
        F: FnMut(MappedRwLockReadGuard<'_, SlotData<T>>) -> R,
    {
        let outer = self.block.0.read_recursive();
        let inner = outer.slots_by_index[self.idx].read();

        if inner.0 == oid::O64::SENTINEL {
            anyhow::bail!("slot is not initialized");
        }

        if inner.0 != self.gen {
            anyhow::bail!("slot has been invalidated");
        }

        let guard = RwLockReadGuard::map(inner, |(_, ptr)| unsafe { ptr.as_ref() });

        Ok(f(guard))
    }

    pub fn write_with<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(MappedRwLockWriteGuard<'_, SlotData<T>>),
    {
        let outer = self.block.0.read_recursive();
        let inner = outer.slots_by_index[self.idx].write();

        if inner.0 == oid::O64::SENTINEL {
            anyhow::bail!("slot is not initialized");
        }

        if inner.0 != self.gen {
            anyhow::bail!("slot has been invalidated");
        }

        let guard = RwLockWriteGuard::map(inner, |(_, ptr)| unsafe { ptr.as_mut() });

        f(guard);

        Ok(())
    }

    pub fn remove_self(self) -> Result<SlotTuple<T>> {
        let mut outer = self.block.0.write();
        let prev_tail = outer.meta.gap_tail;

        unsafe {
            let (record, data) = {
                let mut inner = outer.slots_by_index[self.idx].write();

                if inner.0 == oid::O64::SENTINEL {
                    anyhow::bail!("slot is not initialized");
                }

                if inner.0 != self.gen {
                    anyhow::bail!("slot has been invalidated");
                }

                let slot = inner.1.as_mut();
                let data = slot.read_data_unchecked();
                let record = slot.raw_record_id_unchecked();

                slot.create_gap(if prev_tail == GAP_HEAD {
                    None
                } else {
                    Some(prev_tail)
                });

                (record, data)
            };

            outer.index_by_record.remove(&record);
            outer.meta.gap_tail = self.idx;
            outer.meta.gap_count += 1;

            Ok((RecordId::from_raw(record, outer.meta.table), data))
        }
    }
}

impl<T> Clone for SlotHandle<T> {
    fn clone(&self) -> Self {
        Self {
            block: self.block.clone(),
            gen: self.gen,
            idx: self.idx,
        }
    }
}

impl<T> PartialEq for SlotHandle<T> {
    fn eq(&self, other: &Self) -> bool {
        self.gen == other.gen && self.idx == other.idx
    }
}

impl<T> Eq for SlotHandle<T> {}

impl<T> PartialOrd for SlotHandle<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.gen != other.gen {
            None
        } else {
            Some(self.idx.cmp(&other.idx))
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SlotHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SlotHandle");

        d.field("gen", &self.gen).field("idx", &self.idx);

        let res = self.read_with(|data| {
            d.field("valid", &!data.is_gap()).field("data", &data);
        });

        match res {
            Ok(_) => d.finish(),
            Err(e) => d.field("valid", &false).field("error", &e).finish(),
        }
    }
}
