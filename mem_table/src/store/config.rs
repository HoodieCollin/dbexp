use std::{num::NonZeroUsize, path::Path};

use anyhow::Result;
use primitives::{
    byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
    impl_access_bytes_for_into_bytes_type,
};

use crate::internal_path::InternalPath;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StoreConfig {
    pub initial_block_count: NonZeroUsize,
    pub block_capacity: NonZeroUsize,
    pub persistance: InternalPath,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            initial_block_count: unsafe { NonZeroUsize::new_unchecked(1) },
            block_capacity: unsafe { NonZeroUsize::new_unchecked(128) },
            persistance: Default::default(),
        }
    }
}

impl_access_bytes_for_into_bytes_type!(StoreConfig);

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
        let initial_block_count = NonZeroUsize::new(initial_block_count)
            .ok_or_else(|| anyhow::anyhow!("Initial block count must be greater than zero"))?;

        let block_capacity = NonZeroUsize::new(block_capacity)
            .ok_or_else(|| anyhow::anyhow!("Block capacity must be greater than zero"))?;

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
