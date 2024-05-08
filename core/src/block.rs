use std::{fs::File, sync::Arc};

use anyhow::Result;
use primitives::{shared_object::SharedObject, ThinIdx};

use crate::{
    block::inner::BlockInner,
    object_ids::{RecordId, TableId},
    slot::{SlotHandle, SlotTuple},
    store::result::InsertError,
};

pub use config::BlockConfig;
pub use meta::BlockMeta;

pub mod config;
pub mod inner;
pub mod meta;

pub enum InsertState<T: 'static> {
    Done(Vec<SlotHandle<T>>),
    Partial {
        errors: Vec<(usize, InsertError<T>)>,
        handles: Vec<(usize, SlotHandle<T>)>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
}

pub struct Block<T: 'static> {
    index: ThinIdx,
    pub(crate) inner: SharedObject<BlockInner<T>>,
}

impl<T> Clone for Block<T> {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            inner: self.inner.clone(),
        }
    }
}

impl<T> Block<T> {
    pub(crate) const SLOT_BYTE_COUNT: usize = BlockInner::<T>::SLOT_BYTE_COUNT;

    #[must_use]
    pub fn new(
        index: impl Into<ThinIdx>,
        table: TableId,
        file: Arc<File>,
        offset: usize,
    ) -> Result<Self> {
        let index = index.into();

        Ok(Self {
            index,
            inner: SharedObject::new(BlockInner::new(index, table, file, offset)?),
        })
    }

    #[must_use]
    pub fn new_anon(
        index: impl Into<ThinIdx>,
        table: TableId,
        config: Option<BlockConfig>,
    ) -> Result<Self> {
        let index = index.into();

        Ok(Self {
            index,
            inner: SharedObject::new(BlockInner::new_anon(index, table, config)?),
        })
    }

    pub fn index(&self) -> ThinIdx {
        self.index
    }

    pub fn next_available_index(&self) -> ThinIdx {
        self.inner
            .read_with(|inner| inner.meta.next_available_index())
    }

    pub fn len_as_bytes(&self) -> usize {
        self.inner.read_with(|inner| inner.len_as_bytes())
    }

    pub fn capacity_as_bytes(&self) -> usize {
        self.inner.read_with(|inner| inner.capacity_as_bytes())
    }

    pub fn has_gaps(&self) -> bool {
        self.inner.read_with(|inner| inner.has_gaps())
    }

    pub fn gap_count(&self) -> usize {
        self.inner.read_with(|inner| inner.meta.gap_count)
    }

    pub fn len(&self) -> usize {
        self.inner.read_with(|inner| inner.len())
    }

    pub fn capacity(&self) -> usize {
        self.inner.read_with(|inner| inner.capacity())
    }

    pub fn is_full(&self) -> bool {
        self.inner.read_with(|inner| inner.is_full())
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read_with(|inner| inner.is_empty())
    }

    pub fn sync_all(&self) -> Result<()> {
        self.inner.read_with(|inner| inner.sync_all())
    }

    #[must_use]
    pub fn insert_one(
        &self,
        record: Option<RecordId>,
        data: T,
    ) -> Result<SlotHandle<T>, InsertError<T>> {
        self.inner.write_with(|inner| {
            if let Some(record) = record {
                if inner.meta.table != record.table() {
                    return Err(InsertError::TableMismatch {
                        item: (Some(record), data),
                        iter: None,
                    });
                }
            }

            self.insert_one_with(inner, record, data)
        })
    }

    #[must_use]
    pub(super) fn insert_one_with(
        &self,
        inner: &mut BlockInner<T>,
        record: Option<RecordId>,
        data: T,
    ) -> Result<SlotHandle<T>, InsertError<T>> {
        let is_gap;
        let index;

        if inner.meta.gap_count > 0 {
            index = inner.meta.gap_tail.expect("gap count > 0");
            inner.meta.gap_count -= 1;
            is_gap = true;
        } else {
            index = ThinIdx::new_validated(inner.meta.length)?;
            inner.meta.length += 1;
            is_gap = false;
        }

        let mut new_tail = None;

        let mut slot = inner.slots_by_index[index].write();
        let slot_data = unsafe { slot.as_mut() };

        if is_gap {
            new_tail = Some(ThinIdx::new(unsafe { slot_data.previous_gap_unchecked() }));
        } else {
            slot_data.create_gap(ThinIdx::NIL);
        }

        if let Some(thin_record) = record.map(|r| r.into_thin()) {
            if inner.index_by_record.contains_key(&thin_record) {
                return Err(InsertError::AlreadyExists {
                    item: (record, data),
                    iter: None,
                });
            } else {
                inner.index_by_record.insert(thin_record, index);
            }
        }

        slot_data.fill_gap(record, data);

        if let Some(new_tail) = new_tail {
            inner.meta.gap_tail = Some(new_tail);
        }

        Ok(SlotHandle {
            block: self.clone(),
            idx: index.into_maybe_thin(),
        })
    }

    #[must_use]
    pub fn insert<I>(&self, iter: I, index_offset: usize) -> Result<InsertState<T>, InsertError<T>>
    where
        I: IntoIterator<Item = SlotTuple<T>> + 'static,
    {
        let mut iter = iter.into_iter();
        let (low, high) = iter.size_hint();

        if let Some(high) = high {
            if low == 0 && high == 0 {
                return Ok(InsertState::Done(Vec::new()));
            }
        }

        let inner = self.inner.upgradable();

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
        let mut index = index_offset;

        loop {
            match iter.next() {
                Some((record, data)) => match self.insert_one_with(&mut inner, record, data) {
                    Ok(handle) => {
                        handles.push((index, handle));
                    }
                    Err(err) => {
                        errors.push((index, err));
                    }
                },
                None => {
                    exhausted = true;
                    break;
                }
            }

            index += 1;

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
            Ok(InsertState::Done(
                handles.into_iter().map(|(_, h)| h).collect(),
            ))
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Block<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read_recursive();

        let mut d = f.debug_struct("Block");

        d.field("meta", &inner.meta);

        let mut slots = Vec::with_capacity(inner.meta.len());
        let mut needed = inner.meta.len();

        inner
            .slots_by_index
            .iter()
            .take_while(|slot| {
                let slot = slot.read();
                let slot = unsafe { slot.as_ref() };

                if !slot.is_gap() {
                    slots.push(unsafe { slot.data_unchecked() });
                    needed -= 1;
                }

                needed > 0
            })
            .count();

        d.field("slots", &slots);

        d.finish()
    }
}

#[allow(dead_code)]
#[cfg(test)]
mod tests {
    use primitives::byte_encoding::{FromBytes, IntoBytes};

    use super::*;

    #[test]
    fn test_block_config() -> Result<()> {
        let config = BlockConfig::default();
        let bytes = config.into_bytes()?;
        let mut config2 = BlockConfig::from_bytes(&bytes)?;

        assert_eq!(config, config2);

        config2.set_block_capacity(42)?;
        let bytes = config2.into_bytes()?;
        let config3 = BlockConfig::from_bytes(&bytes)?;

        assert_eq!(config2, config3);

        Ok(())
    }

    #[test]
    fn test_block_meta() -> Result<()> {
        let meta = BlockMeta::new(0usize, TableId::new(), None);
        let mut meta2 = BlockMeta::new(123usize, TableId::new(), None);
        let mut meta3 = BlockMeta::new(456usize, TableId::new(), None);
        meta2.init_from_bytes(&meta.into_bytes()?)?;

        assert_eq!(meta, meta2);

        meta2.length = 42;
        meta3.init_from_bytes(&meta2.into_bytes()?)?;

        assert_eq!(meta2, meta3);

        Ok(())
    }

    #[test]
    fn test_insert_and_remove() -> Result<()> {
        #[derive(Debug)]
        struct Item {
            pub a: usize,
            pub b: usize,
        }

        let table = TableId::new();
        let block = Block::new_anon(0usize, table, None)?;

        fn unwrap_insert_err<T: std::fmt::Debug>(err: InsertError<T>) -> anyhow::Error {
            anyhow::anyhow!("insert error: {:?}", err)
        }

        let _h1 = block
            .insert_one(None, Item { a: 1, b: 2 })
            .map_err(unwrap_insert_err)?;

        let h2 = block
            .insert_one(None, Item { a: 3, b: 4 })
            .map_err(unwrap_insert_err)?;

        let _h3 = block
            .insert_one(None, Item { a: 5, b: 6 })
            .map_err(unwrap_insert_err)?;

        let (r2, i2) = h2
            .remove_self()
            .ok_or_else(|| anyhow::anyhow!("first h2 remove failed"))?;

        let h4 = block
            .insert_one(None, Item { a: 7, b: 8 })
            .map_err(unwrap_insert_err)?;

        let h2 = block.insert_one(r2, i2).map_err(unwrap_insert_err)?;

        let _ = h4
            .remove_self()
            .ok_or_else(|| anyhow::anyhow!("h4 remove failed"))?;

        let _ = h2
            .remove_self()
            .ok_or_else(|| anyhow::anyhow!("second h2 remove failed"))?;

        println!("{:#?}", block);

        Ok(())
    }
}
