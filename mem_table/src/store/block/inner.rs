use std::{
    alloc::Layout, collections::HashMap, fs::File, iter, os::unix::fs::FileExt, ptr::NonNull,
};

use anyhow::Result;
use data_types::oid;
use memmap2::{MmapMut, MmapOptions};
use parking_lot::RwLock;
use primitives::typed_arc::TypedArc;

use crate::{
    byte_encoding::{FromBytes, IntoBytes},
    object_ids::{TableId, ThinRecordId},
    store::{
        block::{BlockConfig, BlockMeta},
        slot::SlotData,
    },
};

pub struct BlockInner<T: 'static> {
    pub(in crate::store) meta: BlockMeta,
    pub(in crate::store) data: MmapMut,
    pub(in crate::store) slots_by_index: Vec<RwLock<(oid::O64, NonNull<SlotData<T>>)>>,
    pub(in crate::store) index_by_record: HashMap<ThinRecordId, usize>,
}

impl<T> Drop for BlockInner<T> {
    fn drop(&mut self) {
        match self.sync_all() {
            Ok(_) => {}
            Err(err) => {
                eprintln!("WARNING: failed to flush block data: {:?}", err);
            }
        }
    }
}

impl<T> BlockInner<T> {
    pub const SLOT_LAYOUT: Layout = Layout::new::<SlotData<T>>();
    pub const SLOT_BYTE_COUNT: usize = Self::SLOT_LAYOUT.size();

    const fn _check_layout() {
        #[cfg(debug_assertions)]
        {
            assert!(Layout::new::<T>().size() >= std::mem::size_of::<usize>());
        }
    }

    pub fn new(idx: usize, table: TableId, file: TypedArc<File>, offset: usize) -> Result<Self> {
        Self::_check_layout();

        let fs_meta = file.metadata()?;

        let end = offset + BlockMeta::BYTE_COUNT;

        if (fs_meta.len() as usize) < end {
            anyhow::bail!("file is too small");
        }

        let meta = {
            let mut meta_bytes = [0u8; BlockMeta::BYTE_COUNT];
            file.read_exact_at(&mut meta_bytes, offset as u64)?;

            let mut this = BlockMeta::new(idx, table, None);
            this.init_from_bytes(&meta_bytes)?;
            this
        };
        let block_capacity = meta.config.block_capacity;
        let content_len = meta.config.block_capacity * Self::SLOT_BYTE_COUNT;

        let data = unsafe {
            MmapOptions::new()
                .offset(BlockMeta::BYTE_COUNT as u64)
                .len(content_len)
                .map_mut(&*file)?
        };

        let slots_by_index = iter::repeat_with(|| ())
            .enumerate()
            .map(|(idx, _)| {
                let offset = idx * Self::SLOT_BYTE_COUNT;

                unsafe {
                    let ptr = data.as_ptr().add(offset) as *mut SlotData<T>;
                    RwLock::new((oid::O64::SENTINEL, NonNull::new_unchecked(ptr)))
                }
            })
            .take(block_capacity)
            .collect::<Vec<_>>();

        let index_by_record = HashMap::with_capacity(block_capacity);

        Ok(Self {
            data,
            meta,
            slots_by_index,
            index_by_record,
        })
    }

    pub fn new_anon(idx: usize, table: TableId, config: Option<BlockConfig>) -> Result<Self> {
        Self::_check_layout();

        let meta = BlockMeta::new(idx, table, config);

        let block_capacity = meta.config.block_capacity;
        let data = MmapMut::map_anon(block_capacity * Self::SLOT_BYTE_COUNT)?;

        let slots_by_index = iter::repeat_with(|| ())
            .enumerate()
            .map(|(idx, _)| {
                let offset = idx * Self::SLOT_BYTE_COUNT;

                unsafe {
                    let ptr = data.as_ptr().add(offset) as *mut SlotData<T>;
                    RwLock::new((oid::O64::SENTINEL, NonNull::new_unchecked(ptr)))
                }
            })
            .take(block_capacity)
            .collect::<Vec<_>>();

        let index_by_record = HashMap::with_capacity(block_capacity);

        Ok(Self {
            data,
            meta,
            slots_by_index,
            index_by_record,
        })
    }

    pub fn len(&self) -> usize {
        self.meta.len()
    }

    pub fn capacity(&self) -> usize {
        self.meta.config.block_capacity
    }

    pub fn len_as_bytes(&self) -> usize {
        self.len() * Self::SLOT_BYTE_COUNT
    }

    pub fn capacity_as_bytes(&self) -> usize {
        self.capacity() * Self::SLOT_BYTE_COUNT
    }

    pub fn has_gaps(&self) -> bool {
        self.meta.gap_count > 0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.capacity()
    }

    pub fn sync_all(&self) -> Result<()> {
        self.data.flush()?;
        Ok(())
    }
}
