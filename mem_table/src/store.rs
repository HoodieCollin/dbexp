use std::{
    alloc::Layout,
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    mem::size_of,
    ops::RangeBounds,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
};

use anyhow::Result;

use primitives::{shared_object::SharedObject, typed_arc::TypedArc};

use crate::{
    object_ids::{RecordId, TableId},
    ByteDecoder, ByteEncoder, FromBytes, InternalPath, IntoBytes,
};

use self::{
    block::{Block, BlockConfig, InsertError, InsertState, SlotTuple},
    slot::SlotHandle,
};

mod block;
mod slot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoreConfig {
    pub initial_block_count: usize,
    pub block_capacity: usize,
    pub persistance: InternalPath,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            initial_block_count: 1,
            block_capacity: 128,
            persistance: Default::default(),
        }
    }
}

impl IntoBytes for StoreConfig {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.initial_block_count)?;
        x.encode(self.block_capacity)?;
        x.encode_bytes(&self.persistance.into_vec()?)?;
        Ok(())
    }
}

impl FromBytes for StoreConfig {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.initial_block_count)?;
        x.decode(&mut this.block_capacity)?;
        x.delegate(&mut this.persistance)?;
        Ok(())
    }
}

impl StoreConfig {
    pub fn new(
        initial_block_count: usize,
        block_capacity: usize,
        persistance: Option<impl AsRef<Path>>,
    ) -> Result<Self> {
        let persistance = persistance
            .map(|x| InternalPath::new(x.as_ref()))
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            initial_block_count,
            block_capacity,
            persistance,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoreMeta {
    pub table: TableId,
    pub block_count: usize,
    pub item_count: usize,
    pub gap_count: usize,
    pub cur_block: usize,
    pub config: StoreConfig,
}

impl Default for StoreMeta {
    fn default() -> Self {
        let config = StoreConfig::default();

        Self {
            table: TableId::new(),
            block_count: config.initial_block_count,
            item_count: 0,
            gap_count: 0,
            cur_block: 0,
            config,
        }
    }
}

impl IntoBytes for StoreMeta {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.table)?;
        x.encode(self.block_count)?;
        x.encode(self.item_count)?;
        x.encode(self.gap_count)?;
        x.encode(self.cur_block)?;
        x.encode_bytes(&self.config.into_bytes()?)?;
        Ok(())
    }
}

impl FromBytes for StoreMeta {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.table)?;
        x.decode(&mut this.block_count)?;
        x.decode(&mut this.item_count)?;
        x.decode(&mut this.gap_count)?;
        x.decode(&mut this.cur_block)?;
        x.delegate(&mut this.config)?;
        Ok(())
    }
}

impl StoreMeta {
    pub fn new(table: Option<TableId>, config: Option<StoreConfig>) -> Self {
        let table = table.unwrap_or_else(|| TableId::new());
        let config = config.unwrap_or_default();

        Self {
            table,
            block_count: config.initial_block_count,
            item_count: 0,
            gap_count: 0,
            cur_block: 0,
            config,
        }
    }

    pub fn len_as_bytes<T: 'static>(&self) -> usize {
        self.item_count * Block::<T>::SLOT_BYTE_COUNT
    }

    pub fn capacity_as_bytes<T: 'static>(&self) -> usize {
        self.block_count * self.config.block_capacity * Block::<T>::SLOT_BYTE_COUNT
    }
}

pub struct StoreInner<T: 'static> {
    meta: StoreMeta,
    file: Option<TypedArc<File>>,
    blocks: HashMap<usize, block::Block<T>>,
}

impl<T> StoreInner<T> {
    pub fn new(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();

        if config.persistance.is_empty() {
            Self::new_memory_only(table, Some(config))
        } else {
            Self::new_persisted(table, Some(config))
        }
    }

    pub fn new_memory_only(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();

        if !config.persistance.is_empty() {
            eprintln!("WARNING: persistance path is ignored for memory-only store")
        }

        Ok(Self {
            meta: StoreMeta::new(table, Some(config)),
            file: None,
            blocks: HashMap::with_capacity(config.initial_block_count),
        })
    }

    pub fn new_persisted(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let table = table.unwrap_or_else(|| TableId::new());
        let config = config.unwrap_or_default();

        if config.persistance.is_empty() {
            anyhow::bail!("persistance path is required for persisted store");
        }

        let path = config.persistance.as_path();
        let parent_dir = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("path has no parent"))?;

        let (meta, file) = if !path.exists() {
            fs::create_dir_all(parent_dir)?;

            let meta = StoreMeta::new(Some(table), Some(config));

            let file = File::create_new(path)?;
            file.set_len(meta.capacity_as_bytes::<T>() as u64)?;
            file.write_all_at(&meta.into_bytes()?, 0)?;

            (meta, file)
        } else {
            let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;

            let fs_meta = file.metadata()?;

            if fs_meta.len() < StoreMeta::BYTE_COUNT as u64 {
                anyhow::bail!("file is too small");
            }

            let mut meta_bytes = [0u8; StoreMeta::BYTE_COUNT];
            file.read_exact_at(&mut meta_bytes, 0)?;

            let meta = StoreMeta::from_bytes(&meta_bytes)?;

            let expected_size = meta.capacity_as_bytes::<T>() as usize;
            let actual_len = (fs_meta.len() - StoreMeta::BYTE_COUNT as u64) as usize;

            if actual_len != expected_size {
                anyhow::bail!("file size does not match metadata");
            }

            (meta, file)
        };

        Ok(Self {
            meta,
            file: Some(TypedArc::new(file)),
            blocks: HashMap::with_capacity(meta.block_count),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub struct BlockCreationError {
    #[source]
    pub error: anyhow::Error,
}

impl std::fmt::Display for BlockCreationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.error)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError<T> {
    #[error(transparent)]
    BlockCreationError(#[from] BlockCreationError),
    #[error(transparent)]
    InsertError(#[from] InsertError<T>),
    #[error("block was not found??? (this should never happen)")]
    BlockNotFound,
}

impl<T> StoreError<T> {
    pub fn thread_safe(self) -> anyhow::Error {
        anyhow::Error::msg(self.to_string())
    }
}

pub struct Store<T: 'static>(SharedObject<StoreInner<T>>);

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
        let block_capacity = inner.meta.config.block_capacity;

        if let Some(file) = inner.file.as_ref().cloned() {
            let block_capacity_as_bytes = block_capacity * Block::<T>::SLOT_BYTE_COUNT;
            let offset = StoreMeta::BYTE_COUNT + (idx * block_capacity_as_bytes);

            inner
                .blocks
                .insert(idx, block::Block::new(idx, table, file, offset)?);
        } else {
            inner.blocks.insert(
                idx,
                block::Block::new_anon(idx, table, Some(BlockConfig::new(block_capacity)))?,
            );
        }

        let new_block_count = inner.blocks.len();
        inner.meta.block_count = new_block_count;

        Ok(())
    }

    pub fn load(&self, r: impl RangeBounds<usize>) -> Result<()> {
        let inner = self.0.upgradable();

        // short-circuit if all blocks are already loaded
        if inner.blocks.len() == inner.meta.block_count {
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

        let mut block_inner = block.0.write();

        let res = block._insert_one(&mut block_inner, tuple)?;

        if block_inner.is_full() {
            if let Some(idx) = block_inner.meta.take_next_block_idx() {
                inner.meta.cur_block = idx;
            } else {
                drop(block_inner);

                let idx = inner.meta.block_count;

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
                return Ok(InsertState::NoOp);
            }
        }

        let mut inner = self.0.write();
        let mut all_errors = Vec::new();
        let mut all_handles = Vec::with_capacity(high.unwrap_or(low));

        loop {
            let block = inner
                .blocks
                .get(&inner.meta.cur_block)
                .ok_or(StoreError::BlockNotFound)?;

            match block.insert(iter.into_iter()) {
                Ok(InsertState::NoOp) => {
                    // this should never happen (already checked above)
                    unreachable!("inserted no items")
                }
                Ok(InsertState::Done(handles)) => {
                    inner.meta.item_count += handles.len();

                    return Ok(InsertState::Done(handles));
                }
                Ok(InsertState::Partial {
                    errors,
                    handles,
                    iter: rest,
                }) => {
                    if !errors.is_empty() {
                        inner.meta.item_count += handles.len();

                        all_errors.extend(errors);
                        all_handles.extend(handles);
                        break;
                    } else {
                        iter = rest.expect("rest should be Some if errors is empty");
                        let mut block_inner = block.0.write();

                        // NOTE: we know the block is full but there is still more data to insert
                        if let Some(idx) = block_inner.meta.take_next_block_idx() {
                            drop(block_inner);

                            inner.meta.item_count += handles.len();
                            inner.meta.cur_block = idx;
                        } else {
                            drop(block_inner);

                            let idx = inner.meta.block_count;

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
                iter: None,
            })
        } else {
            Ok(InsertState::Done(all_handles))
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

#[cfg(test)]
mod test {
    use std::iter;

    use data_types::oid;

    use super::*;

    #[test]
    fn test_store_config() -> Result<()> {
        let config = StoreConfig::default();
        let bytes = config.into_bytes()?;
        let mut config2 = StoreConfig::from_bytes(&bytes)?;

        assert_eq!(config, config2);

        config2.block_capacity = 42;
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
            pub a: oid::O64,
            pub b: oid::O64,
        }

        let table = TableId::new();
        let store = Store::<Item>::new(
            Some(table),
            Some(StoreConfig {
                block_capacity: 10,
                ..Default::default()
            }),
        )?;

        store
            .insert(
                iter::repeat_with(move || {
                    (
                        RecordId::new(table),
                        Item {
                            a: oid::O64::new(),
                            b: oid::O64::new(),
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
