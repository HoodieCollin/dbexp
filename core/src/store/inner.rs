use std::{
    fs::{self, File},
    num::NonZeroUsize,
    ops::RangeBounds,
    os::unix::fs::FileExt,
    sync::Arc,
};

use anyhow::Result;

use indexmap::IndexMap;
use primitives::{
    byte_encoding::{FromBytes, IntoBytes},
    ThinIdx,
};

use crate::{
    block::{self, BlockConfig},
    object_ids::TableId,
    store::{Block, StoreConfig, StoreMeta},
};

pub struct StoreInner<T: 'static> {
    pub(crate) meta: StoreMeta,
    pub(super) file: Option<Arc<File>>,
    pub(crate) blocks: IndexMap<ThinIdx, Block<T>>,
}

impl<T> StoreInner<T> {
    #[must_use]
    pub fn new(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();

        if config.persistance.is_empty() {
            Self::new_memory_only(table, Some(config))
        } else {
            Self::new_persisted(table, Some(config))
        }
    }

    #[must_use]
    pub fn new_memory_only(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();

        if !config.persistance.is_empty() {
            eprintln!("WARNING: persistance path is ignored for memory-only store")
        }

        Ok(Self {
            meta: StoreMeta::new(table, Some(config)),
            file: None,
            blocks: IndexMap::with_capacity(config.initial_block_count.get()),
        })
    }

    #[must_use]
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
            file: Some(Arc::new(file)),
            blocks: IndexMap::with_capacity(meta.block_count.get()),
        })
    }

    pub fn meta(&self) -> &StoreMeta {
        &self.meta
    }

    pub fn blocks(&self) -> &IndexMap<ThinIdx, Block<T>> {
        &self.blocks
    }

    pub fn blocks_mut(&mut self) -> &mut IndexMap<ThinIdx, Block<T>> {
        &mut self.blocks
    }

    pub fn next_available_index(&self) -> ThinIdx {
        let block = self
            .blocks
            .get(&self.meta.cur_block)
            .expect("cur_block should always exist");

        block.index() * block.capacity() + block.next_available_index()
    }

    pub(crate) fn _create_block(&mut self, index: ThinIdx) -> Result<()> {
        let table = self.meta.table;
        let block_capacity = self.meta.config.block_capacity.get();

        if let Some(file) = self.file.as_ref().cloned() {
            let block_capacity_as_bytes = block_capacity * Block::<T>::SLOT_BYTE_COUNT;
            let offset = StoreMeta::BYTE_COUNT + (index * block_capacity_as_bytes);

            self.blocks
                .insert(index, block::Block::new(index, table, file, offset)?);
        } else {
            self.blocks.insert(
                index,
                block::Block::new_anon(index, table, Some(BlockConfig::new(block_capacity)?))?,
            );
        }

        let new_block_count = self.blocks.len();

        self.meta.block_count = NonZeroUsize::new(new_block_count).ok_or_else(|| {
            anyhow::anyhow!("block count should never be zero after creating a block")
        })?;

        Ok(())
    }

    pub(crate) fn _resolve_range(&self, r: impl RangeBounds<usize>) -> Result<(ThinIdx, ThinIdx)> {
        let start = ThinIdx::new_validated(match r.start_bound() {
            std::ops::Bound::Included(&start) => start,
            std::ops::Bound::Excluded(&start) => start + 1,
            std::ops::Bound::Unbounded => 0,
        })?;

        let end = ThinIdx::new_validated(match r.end_bound() {
            std::ops::Bound::Included(&end) => end,
            std::ops::Bound::Excluded(&end) => end - 1,
            std::ops::Bound::Unbounded => ThinIdx::MAX,
        })?;

        let end = std::cmp::min(end, self.meta.item_count.into());
        let block_capacity = self.meta.config.block_capacity;

        let start_block_index = start / block_capacity;
        let mut end_block_index = end / block_capacity;

        if end % block_capacity == 0 && end > 0 {
            end_block_index -= 1;
        }

        Ok((start_block_index, end_block_index))
    }

    pub(crate) fn _get_block_range(
        &self,
        start: ThinIdx,
        end_inclusive: ThinIdx,
    ) -> impl Iterator<Item = (ThinIdx, Option<&Block<T>>)> {
        (start..=end_inclusive).map(|index| (index, self.blocks.get(&index)))
    }
}
