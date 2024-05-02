use anyhow::Result;
use primitives::byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes};

use crate::{
    object_ids::TableId,
    store::{block::Block, config::StoreConfig},
};

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