use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    byte_encoding::{ByteEncoder, IntoBytes, ScalarFromBytes},
    Number, Timestamp, O16, O32, O64,
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

const O16_DESC: &[u8] = &1u32.to_ne_bytes();
const O32_DESC: &[u8] = &2u32.to_ne_bytes();
const O64_DESC: &[u8] = &3u32.to_ne_bytes();
const BOOL_DESC: &[u8] = &4u32.to_ne_bytes();
const NUMBER_DESC: &[u8] = &5u32.to_ne_bytes();
const TIMESTAMP_DESC: &[u8] = &6u32.to_ne_bytes();
const TEXT_DESC: &[u8] = &7u32.to_ne_bytes();
const BYTES_DESC: &[u8] = &8u32.to_ne_bytes();

crate::impl_access_bytes_for_into_bytes_type!(DataType);

impl IntoBytes for DataType {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode_bytes(&self.into_array())
    }
}

impl ScalarFromBytes for DataType {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl DataType {
    pub fn into_array(self) -> [u8; 8] {
        let mut bytes = [0; 8];
        match self {
            Self::O16 => bytes[..4].copy_from_slice(O16_DESC),
            Self::O32 => bytes[..4].copy_from_slice(O32_DESC),
            Self::O64 => bytes[..4].copy_from_slice(O64_DESC),
            Self::Bool => bytes[..4].copy_from_slice(BOOL_DESC),
            Self::Number => bytes[..4].copy_from_slice(NUMBER_DESC),
            Self::Timestamp => bytes[..4].copy_from_slice(TIMESTAMP_DESC),
            Self::Text(size) => {
                bytes[..4].copy_from_slice(TEXT_DESC);
                bytes[4..].copy_from_slice(&size.to_ne_bytes());
            }
            Self::Bytes(size) => {
                bytes[..4].copy_from_slice(BYTES_DESC);
                bytes[4..].copy_from_slice(&size.to_ne_bytes());
            }
        }

        bytes
    }

    #[must_use]
    pub fn from_array(bytes: [u8; 8]) -> Option<Self> {
        match &bytes[..4] {
            O16_DESC => Some(Self::O16),
            O32_DESC => Some(Self::O32),
            O64_DESC => Some(Self::O64),
            BOOL_DESC => Some(Self::Bool),
            NUMBER_DESC => Some(Self::Number),
            TIMESTAMP_DESC => Some(Self::Timestamp),
            TEXT_DESC => {
                let size = u32::from_ne_bytes(bytes[4..].try_into().unwrap());
                Some(Self::Text(size))
            }
            BYTES_DESC => {
                let size = u32::from_ne_bytes(bytes[4..].try_into().unwrap());
                Some(Self::Bytes(size))
            }
            _ => None,
        }
    }

    #[must_use]
    pub fn try_from_array(bytes: impl TryInto<[u8; 8]>) -> Result<Self> {
        match bytes.try_into() {
            Ok(bytes) => {
                Self::from_array(bytes).ok_or_else(|| anyhow::anyhow!("invalid discriminator"))
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
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

    pub fn into_array(self) -> [u8; 8] {
        self.0.into_array()
    }

    #[must_use]
    pub fn from_array(bytes: [u8; 8]) -> Option<Self> {
        DataType::from_array(bytes).map(Self)
    }

    #[must_use]
    pub fn try_from_array(bytes: impl TryInto<[u8; 8]>) -> Result<Self> {
        DataType::try_from_array(bytes).map(Self)
    }
}
