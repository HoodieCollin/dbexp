use anyhow::Result;

use crate::byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes};

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
    pub fn new(block_capacity: usize) -> Self {
        Self { block_capacity }
    }
}
