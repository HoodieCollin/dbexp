use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    byte_encoding::{ByteEncoder, IntoBytes, ScalarFromBytes},
    DataValue, Number, Timestamp, O16, O32, O64,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u32)]
pub enum DataType {
    O16 = 1,
    O32,
    O64,
    Bool,
    Number,
    Timestamp,
    Text(u32),
    Bytes(u32),
}

crate::impl_access_bytes_for_into_bytes_type!(DataType);

impl IntoBytes for DataType {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        match self {
            Self::O16 => x.encode(1u64)?,
            Self::O32 => x.encode(2u64)?,
            Self::O64 => x.encode(3u64)?,
            Self::Bool => x.encode(4u64)?,
            Self::Number => x.encode(5u64)?,
            Self::Timestamp => x.encode(6u64)?,
            Self::Text(size) => {
                x.encode(7u32)?;
                x.encode(*size)?;
            }
            Self::Bytes(size) => {
                x.encode(8u32)?;
                x.encode(*size)?;
            }
        }

        Ok(())
    }
}

impl ScalarFromBytes for DataType {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            anyhow::bail!("invalid byte length")
        }

        Ok(match u8::from_bytes(&bytes[0..1])? {
            1 => Self::O16,
            2 => Self::O32,
            3 => Self::O64,
            4 => Self::Bool,
            5 => Self::Number,
            6 => Self::Timestamp,
            7 => {
                let size = u32::from_bytes(&bytes[5..])?;
                Self::Text(size)
            }
            8 => {
                let size = u32::from_bytes(&bytes[5..])?;
                Self::Bytes(size)
            }
            _ => anyhow::bail!("invalid data type"),
        })
    }
}

impl DataType {
    #[inline(always)]
    pub fn into_value(self) -> DataValue {
        DataValue::Nil(self.into())
    }

    pub fn byte_count(self) -> usize {
        use std::mem::size_of;

        match self {
            Self::O16 => size_of::<O16>(),
            Self::O32 => size_of::<O32>(),
            Self::O64 => size_of::<O64>(),
            Self::Bool => 1,
            Self::Number => Number::BYTE_COUNT,
            Self::Timestamp => size_of::<Timestamp>(),
            Self::Text(size) => size as usize,
            Self::Bytes(size) => size as usize,
        }
    }

    #[must_use]
    pub fn write_zeros(self, dest: &mut [u8]) -> Result<usize> {
        let count = self.byte_count();

        if dest.len() < count {
            anyhow::bail!("buffer is too small to receive {:?}", self)
        }

        unsafe {
            std::ptr::write_bytes(dest.as_mut_ptr(), 0, count);
        }

        Ok(count)
    }
}

/// A wrapper around `DataType` that represents an expected type. The inner `DataType`
/// should never be changed once set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ExpectedType(DataType);

impl ExpectedType {
    pub fn new(ty: DataType) -> Self {
        Self(ty)
    }

    pub fn check(self, val: impl Into<ExpectedType>) -> bool {
        self == val.into()
    }

    pub fn into_inner(self) -> DataType {
        self.0
    }
}

crate::impl_access_bytes_for_into_bytes_type!(ExpectedType);

impl IntoBytes for ExpectedType {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        self.0.encode_bytes(x)
    }
}

impl ScalarFromBytes for ExpectedType {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self(DataType::from_bytes(bytes)?))
    }
}

impl From<DataType> for ExpectedType {
    fn from(ty: DataType) -> Self {
        ExpectedType(ty)
    }
}

impl From<&DataValue> for ExpectedType {
    fn from(val: &DataValue) -> Self {
        val.get_type()
    }
}

impl std::ops::Deref for ExpectedType {
    type Target = DataType;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<DataType> for ExpectedType {
    fn as_ref(&self) -> &DataType {
        &self.0
    }
}
