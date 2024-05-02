use std::path::Path;

use anyhow::Result;
use primitives::byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes};

use crate::internal_path::InternalPath;

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
