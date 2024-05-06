use anyhow::Result;
use primitives::{
    byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
    impl_access_bytes_for_into_bytes_type,
};

use crate::{block::config::BlockConfig, object_ids::TableId, slot::GAP_HEAD};

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

impl_access_bytes_for_into_bytes_type!(BlockMeta);

impl IntoBytes for BlockMeta {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
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
        self.len() == self.config.block_capacity()
    }

    pub fn block_capacity(&self) -> usize {
        self.config.block_capacity()
    }

    #[must_use]
    pub(crate) fn take_next_block_idx(&mut self) -> Option<usize> {
        let idx = self.next_block;

        if idx == GAP_HEAD {
            None
        } else {
            self.next_block = GAP_HEAD;
            Some(idx)
        }
    }
}
