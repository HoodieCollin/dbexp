use std::{
    alloc::Layout, collections::HashMap, fs::File, iter, os::unix::fs::FileExt, ptr::NonNull,
};

use anyhow::Result;
use data_types::oid;
use memmap2::{MmapMut, MmapOptions};
use parking_lot::RwLock;
use primitives::{
    shared_object::{SharedObject, SharedObjectWriteGuard},
    typed_arc::TypedArc,
};

use super::slot::{SlotData, SlotHandle, GAP_HEAD};
use crate::{
    object_ids::{RecordId, TableId, ThinRecordId},
    ByteDecoder, FromBytes, IntoBytes,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockConfig {
    pub block_capacity: usize,
}

impl Default for BlockConfig {
    fn default() -> Self {
        Self {
            block_capacity: 128,
        }
    }
}

impl IntoBytes for BlockConfig {
    fn encode_bytes(&self, x: &mut crate::ByteEncoder<'_>) -> Result<()> {
        x.encode(self.block_capacity)?;
        Ok(())
    }
}

impl FromBytes for BlockConfig {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.block_capacity)?;
        Ok(())
    }
}

impl BlockConfig {
    pub fn new(block_capacity: usize) -> Self {
        Self { block_capacity }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockMeta {
    pub idx: usize,
    pub length: usize,
    pub gap_tail: usize,
    pub gap_count: usize,
    pub next_block: usize,
    pub table: TableId,
    pub config: BlockConfig,
}

impl std::fmt::Debug for BlockMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("BlockMeta");

        d.field("idx", &self.idx).field("length", &self.length);

        if self.gap_tail != GAP_HEAD {
            d.field("gap_tail", &self.gap_tail);
        } else {
            d.field("gap_tail", &Option::<usize>::None);
        }

        d.field("gap_count", &self.gap_count);

        if self.next_block != GAP_HEAD {
            d.field("next_block", &self.next_block);
        } else {
            d.field("next_block", &Option::<usize>::None);
        }

        d.field("config", &self.config).finish()
    }
}

impl IntoBytes for BlockMeta {
    fn encode_bytes(&self, x: &mut crate::ByteEncoder<'_>) -> Result<()> {
        x.encode(self.idx)?;
        x.encode(self.length)?;
        x.encode(self.gap_tail)?;
        x.encode(self.gap_count)?;
        x.encode(self.next_block)?;
        x.encode(self.table)?;
        x.encode_bytes(&self.config.into_bytes()?)?;
        Ok(())
    }
}

impl FromBytes for BlockMeta {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.idx)?;
        x.decode(&mut this.length)?;
        x.decode(&mut this.gap_tail)?;
        x.decode(&mut this.gap_count)?;
        x.decode(&mut this.next_block)?;
        x.decode(&mut this.table)?;
        x.delegate(&mut this.config)?;
        Ok(())
    }
}

impl BlockMeta {
    pub fn new(idx: usize, table: TableId, config: Option<BlockConfig>) -> Self {
        Self {
            idx,
            length: 0,
            gap_tail: GAP_HEAD,
            gap_count: 0,
            next_block: GAP_HEAD,
            table,
            config: config.unwrap_or_default(),
        }
    }

    pub fn len(&self) -> usize {
        self.length - self.gap_count
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_full(&self) -> bool {
        self.len() == self.config.block_capacity
    }

    pub(super) fn take_next_block_idx(&mut self) -> Option<usize> {
        let idx = self.next_block;

        if idx == GAP_HEAD {
            None
        } else {
            self.next_block = GAP_HEAD;
            Some(idx)
        }
    }
}

pub struct BlockInner<T: 'static> {
    pub(super) meta: BlockMeta,
    pub(super) data: MmapMut,
    pub(super) slots_by_index: Vec<RwLock<(oid::O64, NonNull<SlotData<T>>)>>,
    pub(super) index_by_record: HashMap<ThinRecordId, usize>,
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

pub type SlotTuple<T> = (RecordId, T);

pub enum InsertState<T: 'static> {
    NoOp,
    Done(Vec<SlotHandle<T>>),
    Partial {
        errors: Vec<InsertError<T>>,
        handles: Vec<SlotHandle<T>>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
}

#[derive(thiserror::Error)]
pub enum InsertError<T> {
    #[error("record table mismatch")]
    TableMismatch {
        item: SlotTuple<T>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
    #[error("record already exists")]
    AlreadyExists {
        item: SlotTuple<T>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
    #[error("block is full")]
    BlockFull {
        item: Option<SlotTuple<T>>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
}

impl<T> std::fmt::Debug for InsertError<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct ItemDetail<U> {
            record: RecordId,
            data: U,
        }

        impl<U: std::fmt::Debug> std::fmt::Debug for ItemDetail<U> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_map()
                    .entry(&"record", &self.record)
                    .entry(&"data", &self.data)
                    .finish()
            }
        }

        let mut d = f.debug_struct("InsertError");

        d.field("error", &self.to_string());

        match self {
            Self::TableMismatch { item, .. } | Self::AlreadyExists { item, .. } => {
                d.field(
                    "item",
                    &ItemDetail {
                        record: item.0,
                        data: &item.1,
                    },
                );
            }
            Self::BlockFull { item, .. } => {
                if let Some((record, data)) = item {
                    d.field(
                        "item",
                        &ItemDetail {
                            record: *record,
                            data,
                        },
                    );
                } else {
                    d.field("item", &Option::<ItemDetail<T>>::None);
                }
            }
        }

        d.finish_non_exhaustive()
    }
}

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
