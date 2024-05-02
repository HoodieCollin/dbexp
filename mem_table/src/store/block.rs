use std::fs::File;

use anyhow::Result;
use data_types::oid;
use primitives::{shared_object::SharedObject, typed_arc::TypedArc};

use crate::{
    object_ids::{RecordId, TableId},
    store::{
        block::inner::BlockInner,
        insert::{InsertError, InsertState},
        slot::{SlotHandle, SlotTuple},
    },
};

pub use crate::store::block::{config::BlockConfig, meta::BlockMeta};

pub mod config;
pub mod inner;
pub mod meta;

pub struct Block<T: 'static>(pub(super) SharedObject<BlockInner<T>>);

impl<T> Clone for Block<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Block<T> {
    pub(crate) const SLOT_BYTE_COUNT: usize = BlockInner::<T>::SLOT_BYTE_COUNT;

    pub fn new(idx: usize, table: TableId, file: TypedArc<File>, offset: usize) -> Result<Self> {
        Ok(Self(SharedObject::new(BlockInner::new(
            idx, table, file, offset,
        )?)))
    }

    pub fn new_anon(idx: usize, table: TableId, config: Option<BlockConfig>) -> Result<Self> {
        Ok(Self(SharedObject::new(BlockInner::new_anon(
            idx, table, config,
        )?)))
    }

    pub fn len_as_bytes(&self) -> usize {
        self.0.read_with(|inner| inner.len_as_bytes())
    }

    pub fn capacity_as_bytes(&self) -> usize {
        self.0.read_with(|inner| inner.capacity_as_bytes())
    }

    pub fn has_gaps(&self) -> bool {
        self.0.read_with(|inner| inner.has_gaps())
    }

    pub fn gap_count(&self) -> usize {
        self.0.read_with(|inner| inner.meta.gap_count)
    }

    pub fn len(&self) -> usize {
        self.0.read_with(|inner| inner.len())
    }

    pub fn capacity(&self) -> usize {
        self.0.read_with(|inner| inner.capacity())
    }

    pub fn is_full(&self) -> bool {
        self.0.read_with(|inner| inner.is_full())
    }

    pub fn is_empty(&self) -> bool {
        self.0.read_with(|inner| inner.is_empty())
    }

    pub fn sync_all(&self) -> Result<()> {
        self.0.read_with(|inner| inner.sync_all())
    }

    pub fn insert_one(&self, record: RecordId, data: T) -> Result<SlotHandle<T>, InsertError<T>> {
        self.0.write_with(|inner| {
            if inner.meta.table != record.table() {
                Err(InsertError::TableMismatch {
                    item: (record, data),
                    iter: None,
                })
            } else {
                self._insert_one(inner, (record, data))
            }
        })
    }

    pub(super) fn _insert_one(
        &self,
        inner: &mut BlockInner<T>,
        tuple: SlotTuple<T>,
    ) -> Result<SlotHandle<T>, InsertError<T>> {
        let is_gap;
        let idx;

        let (record, data) = tuple;
        let thin_record_id = record.into_raw();

        if inner.index_by_record.contains_key(&thin_record_id) {
            return Err(InsertError::AlreadyExists {
                item: (record, data),
                iter: None,
            });
        }

        if inner.meta.gap_count > 0 {
            idx = inner.meta.gap_tail;
            inner.meta.gap_count -= 1;
            is_gap = true;
        } else {
            idx = inner.meta.length;
            inner.meta.length += 1;
            is_gap = false;
        }

        inner.index_by_record.insert(thin_record_id, idx);

        let gen = oid::O64::new();
        let mut new_tail = None;

        unsafe {
            let slot = &inner.slots_by_index[idx];
            let mut slot = slot.write();

            slot.0 = gen;

            let slot = slot.1.as_mut();

            if is_gap {
                new_tail = Some(slot.previous_gap_unchecked());
            } else {
                slot.create_gap(None);
            }

            slot.fill_gap(thin_record_id, data);
        }

        if let Some(new_tail) = new_tail {
            inner.meta.gap_tail = new_tail;
        }

        Ok(SlotHandle {
            block: self.clone(),
            gen,
            idx,
        })
    }

    pub fn insert<I>(&self, iter: I) -> Result<InsertState<T>, InsertError<T>>
    where
        I: IntoIterator<Item = SlotTuple<T>> + 'static,
    {
        let mut iter = iter.into_iter();
        let (low, high) = iter.size_hint();

        if let Some(high) = high {
            if low == 0 && high == 0 {
                return Ok(InsertState::NoOp);
            }
        }

        let inner = self.0.upgradable();

        if inner.is_full() {
            return Err(InsertError::BlockFull {
                item: None,
                iter: Some(Box::new(iter)),
            });
        }

        let mut inner = inner.upgrade();
        let mut errors = Vec::new();
        let mut handles = Vec::new();
        let exhausted;

        loop {
            match iter.next() {
                Some(tuple) => match self._insert_one(&mut inner, tuple) {
                    Ok(handle) => {
                        handles.push(handle);
                    }
                    Err(err) => {
                        errors.push(err);
                    }
                },
                None => {
                    exhausted = true;
                    break;
                }
            }

            if inner.is_full() {
                exhausted = false;
                break;
            }
        }

        if !exhausted {
            Ok(InsertState::Partial {
                errors,
                handles,
                iter: Some(Box::new(iter)),
            })
        } else if !errors.is_empty() {
            Ok(InsertState::Partial {
                errors,
                handles,
                iter: None,
            })
        } else {
            Ok(InsertState::Done(handles))
        }
    }

    pub fn foreach_slot<F>(&self, mut f: F)
    where
        F: FnMut(SlotHandle<T>),
    {
        let inner = self.0.upgradable();

        inner
            .slots_by_index
            .iter()
            .enumerate()
            .for_each(|(idx, slot)| {
                let slot = slot.read();

                f(SlotHandle {
                    block: self.clone(),
                    gen: slot.0,
                    idx,
                });
            });
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Block<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.read_recursive();

        let mut d = f.debug_struct("Block");

        d.field("meta", &inner.meta);

        let mut slots = Vec::with_capacity(inner.meta.length);

        inner
            .slots_by_index
            .iter()
            .take_while(|slot| {
                let slot = slot.read();

                if slot.0 == oid::O64::SENTINEL {
                    false
                } else {
                    slots.push(unsafe { slot.1.as_ref() });
                    true
                }
            })
            .count();

        d.field("slots", &slots);

        d.finish()
    }
}

#[allow(dead_code)]
#[cfg(test)]
mod tests {
    use crate::byte_encoding::{FromBytes, IntoBytes};

    use super::*;

    #[test]
    fn test_block_config() -> Result<()> {
        let config = BlockConfig::default();
        let bytes = config.into_bytes()?;
        let mut config2 = BlockConfig::from_bytes(&bytes)?;

        assert_eq!(config, config2);

        config2.block_capacity = 42;
        let bytes = config2.into_bytes()?;
        let config3 = BlockConfig::from_bytes(&bytes)?;

        assert_eq!(config2, config3);

        Ok(())
    }

    #[test]
    fn test_block_meta() -> Result<()> {
        let meta = BlockMeta::new(0, TableId::new(), None);
        let mut meta2 = BlockMeta::new(123, TableId::new(), None);
        let mut meta3 = BlockMeta::new(456, TableId::new(), None);
        meta2.init_from_bytes(&meta.into_bytes()?)?;

        assert_eq!(meta, meta2);

        meta2.length = 42;
        meta3.init_from_bytes(&meta2.into_bytes()?)?;

        assert_eq!(meta2, meta3);

        Ok(())
    }

    #[test]
    fn mvp() -> Result<()> {
        #[derive(Debug)]
        struct Item {
            pub a: usize,
            pub b: usize,
        }

        let table = TableId::new();
        let block = Block::new_anon(0, table, None)?;

        let r1 = RecordId::new(table);
        let r2 = RecordId::new(table);
        let r3 = RecordId::new(table);
        let r4 = RecordId::new(table);

        fn unwrap_insert_err<T: std::fmt::Debug>(err: InsertError<T>) -> anyhow::Error {
            anyhow::anyhow!("insert error: {:?}", err)
        }

        let _h1 = block
            .insert_one(r1, Item { a: 1, b: 2 })
            .map_err(unwrap_insert_err)?;

        let h2 = block
            .insert_one(r2, Item { a: 3, b: 4 })
            .map_err(unwrap_insert_err)?;

        let _h3 = block
            .insert_one(r3, Item { a: 5, b: 6 })
            .map_err(unwrap_insert_err)?;

        let (r2, i2) = h2.remove_self()?;

        let h4 = block
            .insert_one(r4, Item { a: 7, b: 8 })
            .map_err(unwrap_insert_err)?;

        let h2 = block.insert_one(r2, i2).map_err(unwrap_insert_err)?;

        let _ = h4.remove_self()?;
        let _ = h2.remove_self()?;

        println!("{:#?}", block);

        // block.foreach_slot(|slot| {
        //     println!("{:#?}", slot);
        // });

        Ok(())
    }
}
