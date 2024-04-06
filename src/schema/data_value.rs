use anyhow::Result;

use super::data_type::DataType;
use crate::decimal::Decimal;
use crate::ratio::{R128, R16, R32, R64};
use crate::timestamp::Timestamp;
use crate::uid::Uid;

#[derive(Debug, Default, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum DataValue {
    #[default]
    Nil,
    Uid(Uid),
    Timestamp(Timestamp),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    R16(R16),
    R32(R32),
    R64(R64),
    R128(R128),
    Decimal(Decimal),
    String(String),
}

impl DataValue {
    pub fn unwrap_uid(self) -> Result<Uid> {
        match self {
            Self::Uid(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_timestamp(self) -> Result<Timestamp> {
        match self {
            Self::Timestamp(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_u8(self) -> Result<u8> {
        match self {
            Self::U8(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_u16(self) -> Result<u16> {
        match self {
            Self::U16(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_u32(self) -> Result<u32> {
        match self {
            Self::U32(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_u64(self) -> Result<u64> {
        match self {
            Self::U64(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_u128(self) -> Result<u128> {
        match self {
            Self::U128(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_i8(self) -> Result<i8> {
        match self {
            Self::I8(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_i16(self) -> Result<i16> {
        match self {
            Self::I16(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_i32(self) -> Result<i32> {
        match self {
            Self::I32(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_i64(self) -> Result<i64> {
        match self {
            Self::I64(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_i128(self) -> Result<i128> {
        match self {
            Self::I128(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_r16(self) -> Result<R16> {
        match self {
            Self::R16(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_r32(self) -> Result<R32> {
        match self {
            Self::R32(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_r64(self) -> Result<R64> {
        match self {
            Self::R64(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_r128(self) -> Result<R128> {
        match self {
            Self::R128(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_decimal(self) -> Result<Decimal> {
        match self {
            Self::Decimal(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn unwrap_string(self) -> Result<String> {
        match self {
            Self::String(value) => Ok(value),
            _ => anyhow::bail!("invalid type"),
        }
    }

    pub fn check_type(&self, ty: DataType) -> Result<()> {
        match ty {
            DataType::Nil => match self {
                Self::Nil => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::Uid => match self {
                Self::Uid(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::Timestamp => match self {
                Self::Timestamp(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::U8 => match self {
                Self::U8(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::U16 => match self {
                Self::U16(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::U32 => match self {
                Self::U32(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::U64 => match self {
                Self::U64(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::U128 => match self {
                Self::U128(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::I8 => match self {
                Self::I8(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::I16 => match self {
                Self::I16(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::I32 => match self {
                Self::I32(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::I64 => match self {
                Self::I64(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::I128 => match self {
                Self::I128(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::R16 => match self {
                Self::R16(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::R32 => match self {
                Self::R32(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::R64 => match self {
                Self::R64(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::R128 => match self {
                Self::R128(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::Decimal => match self {
                Self::Decimal(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
            DataType::String => match self {
                Self::String(_) => Ok(()),
                _ => anyhow::bail!("invalid type"),
            },
        }
    }

    pub fn size_as_bytes(&self) -> usize {
        match self {
            Self::Nil => 0,
            Self::Uid(_) => 16,
            Self::Timestamp(_) => 8,
            Self::U8(_) => 1,
            Self::U16(_) => 2,
            Self::U32(_) => 4,
            Self::U64(_) => 8,
            Self::U128(_) => 16,
            Self::I8(_) => 1,
            Self::I16(_) => 2,
            Self::I32(_) => 4,
            Self::I64(_) => 8,
            Self::I128(_) => 16,
            Self::R16(_) => 2,
            Self::R32(_) => 4,
            Self::R64(_) => 8,
            Self::R128(_) => 16,
            Self::Decimal(_) => 16,
            Self::String(_) => panic!("not implemented"),
        }
    }

    pub fn copy_to(&self, bytes: &mut [u8]) -> Result<()> {
        match self {
            Self::Nil => {}
            Self::Uid(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::Timestamp(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::U8(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::U16(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::U32(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::U64(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::U128(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::I8(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::I16(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::I32(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::I64(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::I128(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::R16(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::R32(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::R64(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::R128(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::Decimal(value) => {
                let data = value.to_be_bytes();
                bytes.copy_from_slice(&data);
            }
            Self::String(_) => {
                panic!("not implemented")
            }
        }

        Ok(())
    }

    pub fn copy_from(&mut self, bytes: &[u8]) -> Result<()> {
        if self.size_as_bytes() != bytes.len() {
            anyhow::bail!("invalid size");
        }

        match self {
            Self::Nil => {}
            Self::Uid(value) => {
                *value = Uid::from_be_bytes(bytes.try_into()?);
            }
            Self::Timestamp(value) => {
                *value = Timestamp::from_be_bytes(bytes.try_into()?)?;
            }
            Self::U8(value) => {
                *value = u8::from_be_bytes(bytes.try_into()?);
            }
            Self::U16(value) => {
                *value = u16::from_be_bytes(bytes.try_into()?);
            }
            Self::U32(value) => {
                *value = u32::from_be_bytes(bytes.try_into()?);
            }
            Self::U64(value) => {
                *value = u64::from_be_bytes(bytes.try_into()?);
            }
            Self::U128(value) => {
                *value = u128::from_be_bytes(bytes.try_into()?);
            }
            Self::I8(value) => {
                *value = i8::from_be_bytes(bytes.try_into()?);
            }
            Self::I16(value) => {
                *value = i16::from_be_bytes(bytes.try_into()?);
            }
            Self::I32(value) => {
                *value = i32::from_be_bytes(bytes.try_into()?);
            }
            Self::I64(value) => {
                *value = i64::from_be_bytes(bytes.try_into()?);
            }
            Self::I128(value) => {
                *value = i128::from_be_bytes(bytes.try_into()?);
            }
            Self::R16(value) => {
                *value = R16::from_be_bytes(bytes.try_into()?);
            }
            Self::R32(value) => {
                *value = R32::from_be_bytes(bytes.try_into()?);
            }
            Self::R64(value) => {
                *value = R64::from_be_bytes(bytes.try_into()?);
            }
            Self::R128(value) => {
                *value = R128::from_be_bytes(bytes.try_into()?);
            }
            Self::Decimal(value) => {
                *value = Decimal::from_be_bytes(bytes.try_into()?);
            }
            Self::String(_) => {
                panic!("not implemented")
            }
        }

        Ok(())
    }
}
