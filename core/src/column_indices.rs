use std::num::NonZeroUsize;

use anyhow::Result;
use primitives::{
    byte_encoding::{AccessBytes, ByteDecoder, ByteEncoder, FromBytes, IntoBytes, ScalarFromBytes},
    Idx, ThinIdx,
};

use crate::{slot::SlotHandle, values::DataValue};

pub const MAX_COLUMNS: usize = 32;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct CellIdx {
    pub block: ThinIdx,
    pub row: Idx,
}

impl AccessBytes for CellIdx {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        f(&self.into_array())
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        Ok(Some(f(&mut self.into_array())?))
    }
}

impl ScalarFromBytes for CellIdx {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl std::fmt::Debug for CellIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellIndex")
            .field("block", &self.block)
            .field("row", &self.row)
            .finish()
    }
}

impl From<SlotHandle<DataValue>> for CellIdx {
    fn from(handle: SlotHandle<DataValue>) -> Self {
        Self {
            block: handle.block.index(),
            row: handle.idx,
        }
    }
}

impl CellIdx {
    pub const INVALID: Self = Self {
        block: ThinIdx::INVALID,
        row: Idx::INVALID,
    };

    pub fn new(block: ThinIdx, row: Idx) -> Self {
        Self { block, row }
    }

    pub fn into_array(&self) -> [u8; 16] {
        let mut bytes = [0; 16];

        bytes[..8].copy_from_slice(&self.block.into_array());
        bytes[8..].copy_from_slice(&self.row.into_array());

        bytes
    }

    pub fn from_array(bytes: [u8; 16]) -> Option<Self> {
        let block = ThinIdx::from_array(bytes[..8].try_into().unwrap())?;
        let row = Idx::from_array(bytes[8..].try_into().unwrap())?;

        Some(Self { block, row })
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 16]>) -> Result<Self> {
        match bytes.try_into() {
            Ok(bytes) => {
                let block = ThinIdx::try_from_array(&bytes[..8])?;
                let row = Idx::try_from_array(&bytes[8..])?;

                Ok(Self { block, row })
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn block(&self) -> ThinIdx {
        self.block
    }

    pub fn row(&self) -> Idx {
        self.row
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(16))]
pub struct ColumnIndices(NonZeroUsize, [Option<CellIdx>; MAX_COLUMNS]);

impl IntoBytes for ColumnIndices {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.0.get() as u64)?;
        x.skip(8)?;
        x.encode_bytes(self.raw_buckets_as_bytes())?;

        Ok(())
    }
}

impl FromBytes for ColumnIndices {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        let mut count_bytes = [0u8; 8];
        x.read_exact(&mut count_bytes)?;

        this.0 = NonZeroUsize::new(u64::from_ne_bytes(count_bytes) as usize)
            .ok_or_else(|| anyhow::anyhow!("invalid count"))?;

        x.skip(8)?;

        unsafe {
            let buckets = this.raw_buckets_as_bytes_mut();
            x.read_exact(buckets)?;
        }

        Ok(())
    }
}

impl std::fmt::Debug for ColumnIndices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_list();

        for i in 0..self.0.get() {
            if let Some(cell) = self.1[i] {
                d.entry(&cell);
            } else {
                d.entry(&"None");
            }
        }

        d.finish()
    }
}

impl ColumnIndices {
    pub const ITEM_BYTES: usize = 16;
    pub const BYTES: usize = Self::ITEM_BYTES + (Self::ITEM_BYTES * MAX_COLUMNS);
    pub const INVALID: Self = Self(NonZeroUsize::MAX, [None; MAX_COLUMNS]);

    pub fn new(count: NonZeroUsize) -> Self {
        Self(count, [None; MAX_COLUMNS])
    }

    pub(self) fn raw_buckets_as_bytes(&self) -> &[u8] {
        let count = self.0.get();
        let ptr = self.1.as_ptr() as *const u8;

        unsafe { std::slice::from_raw_parts(ptr, count * Self::ITEM_BYTES) }
    }

    pub(self) unsafe fn raw_buckets_as_bytes_mut(&mut self) -> &mut [u8] {
        let count = self.0.get();
        let ptr = self.1.as_mut_ptr() as *mut u8;

        std::slice::from_raw_parts_mut(ptr, count * Self::ITEM_BYTES)
    }

    #[must_use]
    pub fn replace(&mut self, column: usize, value: CellIdx) -> Result<()> {
        if column >= self.0.get() {
            anyhow::bail!("column index out of bounds");
        }

        unsafe {
            self.1.get_unchecked_mut(column).replace(value);
        }

        Ok(())
    }

    pub fn get(&self, column: usize) -> Option<CellIdx> {
        if column >= self.0.get() {
            return None;
        }

        self.1.get(column).copied().flatten()
    }

    pub fn count(&self) -> usize {
        self.0.get()
    }

    pub fn buckets(&self) -> &[Option<CellIdx>] {
        &self.1[..self.0.get()]
    }
}
