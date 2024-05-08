use std::{num::NonZeroUsize, path::Path};

use anyhow::Result;
use primitives::{
    byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
    impl_access_bytes_for_into_bytes_type, InternalPath,
};

use crate::store::StoreConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VarcapConfig {
    pub initial_slot_capacity: NonZeroUsize,
    pub initial_block_count: NonZeroUsize,
    pub block_capacity: NonZeroUsize,
    pub persistance: InternalPath,
}

impl_access_bytes_for_into_bytes_type!(VarcapConfig);

impl IntoBytes for VarcapConfig {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.initial_slot_capacity)?;
        x.encode(self.initial_block_count)?;
        x.encode(self.block_capacity)?;
        x.encode_bytes(&self.persistance.into_vec()?)?;
        Ok(())
    }
}

impl FromBytes for VarcapConfig {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.initial_slot_capacity)?;
        x.decode(&mut this.initial_block_count)?;
        x.decode(&mut this.block_capacity)?;
        x.delegate(&mut this.persistance)?;
        Ok(())
    }
}

impl From<VarcapConfig> for StoreConfig {
    fn from(value: VarcapConfig) -> Self {
        Self {
            initial_block_count: value.initial_block_count,
            block_capacity: value.block_capacity,
            persistance: value.persistance,
        }
    }
}

impl VarcapConfig {
    #[must_use]
    pub fn new(
        initial_slot_capacity: usize,
        initial_block_count: usize,
        block_capacity: usize,
        persistance: Option<impl AsRef<Path>>,
    ) -> Result<Self> {
        let initial_slot_capacity = NonZeroUsize::new(initial_slot_capacity)
            .ok_or_else(|| anyhow::anyhow!("Initial slot capacity must be greater than zero"))?;

        let initial_block_count = NonZeroUsize::new(initial_block_count)
            .ok_or_else(|| anyhow::anyhow!("Initial block count must be greater than zero"))?;

        let block_capacity = NonZeroUsize::new(block_capacity)
            .ok_or_else(|| anyhow::anyhow!("Block capacity must be greater than zero"))?;

        let persistance = persistance
            .map(|x| InternalPath::new(x.as_ref()))
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            initial_slot_capacity,
            initial_block_count,
            block_capacity,
            persistance,
        })
    }
}
