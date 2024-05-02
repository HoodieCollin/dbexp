use std::num::NonZeroUsize;

use anyhow::Result;

use primitives::byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockConfig {
    block_capacity: NonZeroUsize,
}

impl Default for BlockConfig {
    fn default() -> Self {
        Self {
            block_capacity: unsafe { NonZeroUsize::new_unchecked(128) },
        }
    }
}

impl IntoBytes for BlockConfig {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
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
    pub fn new(block_capacity: usize) -> Result<Self> {
        let block_capacity = NonZeroUsize::new(block_capacity)
            .ok_or_else(|| anyhow::anyhow!("Block capacity must be greater than zero"))?;

        Ok(Self { block_capacity })
    }

    pub fn block_capacity(&self) -> usize {
        self.block_capacity.get()
    }

    pub fn set_block_capacity(&mut self, block_capacity: usize) -> Result<()> {
        self.block_capacity = NonZeroUsize::new(block_capacity)
            .ok_or_else(|| anyhow::anyhow!("Block capacity must be greater than zero"))?;

        Ok(())
    }
}
