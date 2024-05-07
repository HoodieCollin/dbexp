use anyhow::Result;
use primitives::{
    byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
    impl_access_bytes_for_into_bytes_type, ThinIdx,
};

use crate::{block::config::BlockConfig, object_ids::TableId};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockMeta {
    pub index: ThinIdx,
    pub length: usize,
    pub gap_tail: Option<ThinIdx>,
    pub gap_count: usize,
    pub next_block: Option<ThinIdx>,
    pub table: TableId,
    pub config: BlockConfig,
}

impl std::fmt::Debug for BlockMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("BlockMeta");

        d.field("index", &self.index).field("length", &self.length);

        if let Some(gap_tail) = self.gap_tail {
            d.field("gap_tail", &gap_tail);
        } else {
            d.field("gap_tail", &Option::<ThinIdx>::None);
        }

        d.field("gap_count", &self.gap_count);

        if let Some(next_block) = self.next_block {
            d.field("next_block", &next_block);
        } else {
            d.field("next_block", &Option::<ThinIdx>::None);
        }

        d.field("config", &self.config).finish()
    }
}

impl_access_bytes_for_into_bytes_type!(BlockMeta);

impl IntoBytes for BlockMeta {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.index)?;
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
        x.decode(&mut this.index)?;
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
    pub fn new(index: impl Into<ThinIdx>, table: TableId, config: Option<BlockConfig>) -> Self {
        Self {
            index: index.into(),
            length: 0,
            gap_tail: ThinIdx::NIL,
            gap_count: 0,
            next_block: ThinIdx::NIL,
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

    pub fn next_available_index(&self) -> ThinIdx {
        if self.gap_count > 0 {
            self.gap_tail.unwrap()
        } else {
            self.index + 1
        }
    }

    #[must_use]
    pub(crate) fn take_next_block_index(&mut self) -> Option<ThinIdx> {
        if let Some(index) = self.next_block {
            self.next_block = ThinIdx::NIL;
            Some(index)
        } else {
            None
        }
    }
}
