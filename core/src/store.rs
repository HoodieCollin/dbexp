use std::{num::NonZeroUsize, ops::RangeBounds};

use anyhow::Result;

use primitives::{byte_encoding::IntoBytes, shared_object::SharedObject};

use crate::{
    block::{self, Block, BlockConfig},
    object_ids::{RecordId, TableId},
    slot::{SlotHandle, SlotTuple},
};

use self::inner::StoreInner;

pub use self::{
    config::StoreConfig,
    meta::StoreMeta,
    record_store::{RecordStore, RecordStoreError},
    result::{BlockCreationError, InsertError, StoreError},
};

pub mod config;
pub mod inner;
pub mod meta;
pub mod record_store;
pub mod result;

#[derive(Debug)]
pub enum InsertState<T: 'static> {
    Done(Vec<SlotHandle<T>>),
    Partial {
        errors: Vec<(usize, InsertError<T>)>,
        handles: Vec<(usize, SlotHandle<T>)>,
    },
}

pub struct Store<T: 'static>(SharedObject<StoreInner<T>>);

impl<T> Clone for Store<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> Store<T> {
    pub fn new(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();
        let store = Self(SharedObject::new(StoreInner::new(table, Some(config))?));

        if config.persistance.is_empty() {
            store.load(..)?;
        }

        Ok(store)
    }

    fn _create_block(inner: &mut StoreInner<T>, idx: usize) -> Result<()> {
        let table = inner.meta.table;
        let block_capacity = inner.meta.config.block_capacity.get();

        if let Some(file) = inner.file.as_ref().cloned() {
            let block_capacity_as_bytes = block_capacity * Block::<T>::SLOT_BYTE_COUNT;
            let offset = StoreMeta::BYTE_COUNT + (idx * block_capacity_as_bytes);

            inner
                .blocks
                .insert(idx, block::Block::new(idx, table, file, offset)?);
        } else {
            inner.blocks.insert(
                idx,
                block::Block::new_anon(idx, table, Some(BlockConfig::new(block_capacity)?))?,
            );
        }

        let new_block_count = inner.blocks.len();

        inner.meta.block_count = NonZeroUsize::new(new_block_count).ok_or_else(|| {
            anyhow::anyhow!("block count should never be zero after creating a block")
        })?;

        Ok(())
    }

    pub fn load(&self, r: impl RangeBounds<usize>) -> Result<()> {
        let inner = self.0.upgradable();

        // short-circuit if all blocks are already loaded
        if inner.blocks.len() == inner.meta.block_count.get() {
            return Ok(());
        }

        let start = match r.start_bound() {
            std::ops::Bound::Included(&start) => start,
            std::ops::Bound::Excluded(&start) => start + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match r.end_bound() {
            std::ops::Bound::Included(&end) => end,
            std::ops::Bound::Excluded(&end) => end - 1,
            std::ops::Bound::Unbounded => usize::MAX,
        };

        let end = std::cmp::min(end, inner.meta.item_count);
        let block_capacity = inner.meta.config.block_capacity;

        let start_block_idx = start / block_capacity;
        let mut end_block_idx = end / block_capacity;

        if end % block_capacity == 0 && end > 0 {
            end_block_idx -= 1;
        }

        let mut inner = inner.upgrade();

        for idx in start_block_idx..=end_block_idx {
            if inner.blocks.contains_key(&idx) {
                continue;
            }

            Self::_create_block(&mut inner, idx)?;
        }

        Ok(())
    }

    pub fn insert_one(&self, record: RecordId, data: T) -> Result<SlotHandle<T>, StoreError<T>> {
        let mut inner = self.0.write();
        self._insert_one(&mut inner, (record, data))
    }

    pub fn _insert_one(
        &self,
        mut inner: &mut StoreInner<T>,
        tuple: SlotTuple<T>,
    ) -> Result<SlotHandle<T>, StoreError<T>> {
        // blocks should never be left in a full state... If it is filled during an insert, then a new block should be created

        let block = inner
            .blocks
            .get(&inner.meta.cur_block)
            .ok_or(StoreError::BlockNotFound)?;

        let mut block_inner = block.inner.write();

        let res = block._insert_one(&mut block_inner, tuple)?;

        if block_inner.is_full() {
            if let Some(idx) = block_inner.meta.take_next_block_idx() {
                inner.meta.cur_block = idx;
            } else {
                drop(block_inner);

                let idx = inner.meta.block_count.get();

                Self::_create_block(&mut inner, idx)
                    .map_err(|e| StoreError::BlockCreationError(BlockCreationError { error: e }))?;

                inner.meta.cur_block = idx;
            }
        }

        inner.meta.item_count += 1;

        Ok(res)
    }

    pub fn insert<I>(&self, iter: I) -> Result<InsertState<T>, StoreError<T>>
    where
        I: IntoIterator<Item = SlotTuple<T>> + 'static,
    {
        let mut iter: Box<dyn Iterator<Item = SlotTuple<T>>> = Box::new(iter.into_iter());
        let (low, high) = iter.size_hint();

        if let Some(high) = high {
            if low == 0 && high == 0 {
                return Err(StoreError::InsertError(InsertError::NoValues));
            }
        }

        let mut inner = self.0.write();
        let mut all_errors = Vec::new();
        let mut all_handles = Vec::with_capacity(high.unwrap_or(low));
        let mut idx = 0;

        loop {
            let block = inner
                .blocks
                .get(&inner.meta.cur_block)
                .ok_or(StoreError::BlockNotFound)?;

            match block.insert(iter.into_iter(), idx) {
                Ok(block::InsertState::Done(handles)) => {
                    inner.meta.item_count += handles.len();

                    return Ok(InsertState::Done(handles));
                }
                Ok(block::InsertState::Partial {
                    errors,
                    handles,
                    iter: rest,
                }) => {
                    idx += errors.len() + handles.len();

                    if !errors.is_empty() {
                        inner.meta.item_count += handles.len();

                        all_errors.extend(errors);
                        all_handles.extend(handles);
                        break;
                    } else {
                        iter = rest.expect("rest should be Some if errors is empty");
                        let mut block_inner = block.inner.write();

                        // NOTE: we know the block is full but there is still more data to insert
                        if let Some(idx) = block_inner.meta.take_next_block_idx() {
                            drop(block_inner);

                            inner.meta.item_count += handles.len();
                            inner.meta.cur_block = idx;
                        } else {
                            drop(block_inner);

                            let idx = inner.meta.block_count.get();

                            Self::_create_block(&mut inner, idx).map_err(|e| {
                                StoreError::BlockCreationError(BlockCreationError { error: e })
                            })?;

                            inner.meta.item_count += handles.len();
                            inner.meta.cur_block = idx;
                        }
                    }
                }
                Err(InsertError::BlockFull { .. }) => {
                    // this should never happen (current block should always have at least one slot available)
                    unreachable!("block is full")
                }
                Err(e) => {
                    return Err(StoreError::InsertError(e));
                }
            }
        }

        if !all_errors.is_empty() {
            Ok(InsertState::Partial {
                errors: all_errors,
                handles: all_handles,
            })
        } else {
            Ok(InsertState::Done(
                all_handles.into_iter().map(|(_, h)| h).collect(),
            ))
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Store<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.read_recursive();
        let mut d = f.debug_struct("Store");
        d.field("meta", &inner.meta);
        d.field("blocks", &inner.blocks);
        d.finish()
    }
}

#[allow(dead_code)]
#[cfg(test)]
mod test {
    use primitives::{byte_encoding::FromBytes, O64};
    use std::iter;

    use super::*;

    #[test]
    fn test_store_config() -> Result<()> {
        let config = StoreConfig::default();
        let bytes = config.into_bytes()?;
        let mut config2 = StoreConfig::from_bytes(&bytes)?;

        assert_eq!(config, config2);

        config2.block_capacity = NonZeroUsize::new(42).unwrap();
        let bytes = config2.into_bytes()?;
        let config3 = StoreConfig::from_bytes(&bytes)?;

        assert_eq!(config2, config3);

        Ok(())
    }

    #[test]
    fn test_store_meta() -> Result<()> {
        let meta = StoreMeta::default();
        let bytes = meta.into_bytes()?;
        let mut meta2 = StoreMeta::from_bytes(&bytes)?;

        assert_eq!(meta, meta2);

        meta2.item_count = 42;
        let bytes = meta2.into_bytes()?;
        let meta3 = StoreMeta::from_bytes(&bytes)?;

        assert_eq!(meta2, meta3);

        Ok(())
    }

    #[test]
    fn mvp() -> Result<()> {
        #[derive(Debug)]
        struct Item {
            pub a: O64,
            pub b: O64,
        }

        let table = TableId::new();
        let store = Store::<Item>::new(
            Some(table),
            Some(StoreConfig {
                block_capacity: NonZeroUsize::new(5).unwrap(),
                ..Default::default()
            }),
        )?;

        store
            .insert(
                iter::repeat_with(move || {
                    (
                        RecordId::new(table),
                        Item {
                            a: O64::new(),
                            b: O64::new(),
                        },
                    )
                })
                .take(15),
            )
            .map_err(StoreError::thread_safe)?;

        println!("{:#?}", store);

        Ok(())
    }
}
