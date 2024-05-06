// pub mod math;

use anyhow::Result;

use crate::{
    number::Builtin, Bytes, DataType, ExpectedType, Number, Text, Timestamp, O16, O32, O64,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataValue {
    Nil(ExpectedType),
    O16(O16),
    O32(O32),
    O64(O64),
    Bool(bool),
    Number(Number),
    Timestamp(Timestamp),
    Text(Text),
    Bytes(Bytes),
}

unsafe impl Send for DataValue {}
unsafe impl Sync for DataValue {}

impl DataValue {
    pub fn is_nil(&self) -> bool {
        match self {
            DataValue::Nil(_) => true,
            _ => false,
        }
    }

    pub fn get_type(&self) -> ExpectedType {
        match self {
            DataValue::Nil(expected) => *expected,
            DataValue::O16(_) => ExpectedType::new(DataType::O16),
            DataValue::O32(_) => ExpectedType::new(DataType::O32),
            DataValue::O64(_) => ExpectedType::new(DataType::O64),
            DataValue::Bool(_) => ExpectedType::new(DataType::Bool),
            DataValue::Number(_) => ExpectedType::new(DataType::Number),
            DataValue::Timestamp(_) => ExpectedType::new(DataType::Timestamp),
            DataValue::Text(val) => ExpectedType::new(DataType::Text(val.capacity() as u32)),
            DataValue::Bytes(val) => ExpectedType::new(DataType::Bytes(val.capacity() as u32)),
        }
    }

    #[must_use]
    pub fn write_to(&self, dest: &mut [u8]) -> Result<()> {
        use std::ptr;

        match self {
            DataValue::Nil(ty) => {
                let _ = ty.write_zeros(dest)?;
                return Ok(());
            }
            _ => {}
        };

        unsafe {
            ptr::write(dest.as_mut_ptr(), 1u8);
        }

        let dest = &mut dest[1..];

        match self {
            DataValue::Nil(_) => unreachable!(),
            DataValue::O16(val) => {
                let arr = val.into_array();
                dest.copy_from_slice(&arr);
            }
            DataValue::O32(val) => {
                let arr = val.into_array();
                dest.copy_from_slice(&arr);
            }
            DataValue::O64(val) => {
                let arr = val.into_array();
                dest.copy_from_slice(&arr);
            }
            DataValue::Bool(val) => {
                dest[0] = *val as u8;
            }
            DataValue::Number(val) => {
                let arr = val.into_array();
                dest.copy_from_slice(&arr);
            }
            DataValue::Timestamp(val) => {
                let arr = val.into_array();
                dest.copy_from_slice(&arr);
            }
            DataValue::Text(val) => unsafe {
                ptr::copy_nonoverlapping(val.as_ptr(), dest.as_mut_ptr(), val.len() as usize);
                ptr::write_bytes(
                    dest.as_mut_ptr().add(val.len() as usize),
                    0,
                    val.available() as usize,
                );
            },
            DataValue::Bytes(val) => unsafe {
                ptr::copy_nonoverlapping(val.as_ptr(), dest.as_mut_ptr(), val.len() as usize);
                ptr::write_bytes(
                    dest.as_mut_ptr().add(val.len() as usize),
                    0,
                    val.available() as usize,
                );
            },
        }

        Ok(())
    }

    #[must_use]
    pub fn try_integer_from_number<T: Builtin>(x: T) -> Result<Self> {
        Ok(DataValue::Number(Number::try_from_builtin(x)?))
    }

    #[must_use]
    pub fn try_integer_from_str(s: &str) -> Result<Self> {
        Ok(DataValue::Number(Number::try_from_str(s)?))
    }

    /// Tries to replace the current value with the given value. If the value is not of the
    /// expected type, an error is returned.
    ///
    /// This is useful during arithmetic operations where the result is expected to be of the
    /// same type as the left operand.
    #[must_use]
    pub fn try_replace(&mut self, value: DataValue) -> Result<DataValue> {
        let expected_ty = self.get_type();

        if !expected_ty.check(&value) {
            anyhow::bail!(
                "expected value of type {:?} but got {:?}",
                expected_ty,
                value.get_type()
            );
        }

        Ok(std::mem::replace(self, value))
    }

    #[must_use]
    pub fn try_from_any<T: Into<ExpectedType>, V: std::any::Any>(ty: T, value: V) -> Result<Self> {
        let expected_ty: ExpectedType = ty.into();
        let type_name = std::any::type_name::<V>();
        let value = &value as &dyn std::any::Any;

        match expected_ty.into_inner() {
            DataType::O16 => {
                if let Some(val) = value.downcast_ref::<O16>() {
                    return Ok(DataValue::O16(*val));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    if val.len() != DataType::O16.byte_count() {
                        anyhow::bail!("invalid slice length")
                    }

                    let mut arr = [0u8; 2];
                    arr.copy_from_slice(val);

                    return Ok(DataValue::O16(O16::try_from_array(arr)?));
                }
            }
            DataType::O32 => {
                if let Some(val) = value.downcast_ref::<O32>() {
                    return Ok(DataValue::O32(*val));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    if val.len() != DataType::O32.byte_count() {
                        anyhow::bail!("invalid slice length")
                    }

                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(val);

                    return Ok(DataValue::O32(O32::try_from_array(arr)?));
                }
            }
            DataType::O64 => {
                if let Some(val) = value.downcast_ref::<O64>() {
                    return Ok(DataValue::O64(*val));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    if val.len() != DataType::O64.byte_count() {
                        anyhow::bail!("invalid slice length")
                    }

                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(val);

                    return Ok(DataValue::O64(O64::try_from_array(arr)?));
                }
            }
            DataType::Bool => {
                if let Some(val) = value.downcast_ref::<bool>() {
                    return Ok(DataValue::Bool(*val));
                } else if let Some(val) = value.downcast_ref::<Number>() {
                    return Ok(DataValue::Bool(val.is_zero()));
                } else if let Some(val) = value.downcast_ref::<Text>() {
                    return Ok(DataValue::Bool(!val.is_empty()));
                } else if let Some(val) = value.downcast_ref::<Bytes>() {
                    return Ok(DataValue::Bool(!val.is_empty()));
                } else if let Some(val) = value.downcast_ref::<&str>() {
                    return Ok(DataValue::Bool(!val.is_empty()));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    return Ok(DataValue::Bool(!val.is_empty()));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    return Ok(DataValue::Bool(!val.is_empty()));
                } else if let Some(val) = value.downcast_ref::<Vec<u8>>() {
                    return Ok(DataValue::Bool(!val.is_empty()));
                } else if let Some(val) = value.downcast_ref::<i8>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<i16>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<i32>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<i64>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<i128>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<isize>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<u8>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<u16>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<u32>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<u64>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<u128>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<usize>() {
                    return Ok(DataValue::Bool(*val != 0));
                } else if let Some(val) = value.downcast_ref::<f32>() {
                    return Ok(DataValue::Bool(val.as_i128()? != 0));
                } else if let Some(val) = value.downcast_ref::<f64>() {
                    return Ok(DataValue::Bool(val.as_i128()? != 0));
                }
            }
            DataType::Number => {
                if let Some(val) = value.downcast_ref::<Number>() {
                    return Ok(DataValue::Number(*val));
                } else if let Some(val) = value.downcast_ref::<Text>() {
                    return Ok(DataValue::Number(Number::try_from_str(val.as_str())?));
                } else if let Some(val) = value.downcast_ref::<Bytes>() {
                    return Ok(DataValue::Number(Number::try_from_slice(val)?));
                } else if let Some(val) = value.downcast_ref::<&str>() {
                    return Ok(DataValue::Number(Number::try_from_str(*val)?));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    return Ok(DataValue::Number(Number::try_from_str(val.as_str())?));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    return Ok(DataValue::Number(Number::try_from_slice(val)?));
                } else if let Some(val) = value.downcast_ref::<Vec<u8>>() {
                    return Ok(DataValue::Number(Number::try_from_slice(&val)?));
                } else if let Some(val) = value.downcast_ref::<i8>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<i16>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<i32>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<i64>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<i128>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<isize>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<u8>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<u16>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<u32>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<u64>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<u128>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<usize>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<f32>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                } else if let Some(val) = value.downcast_ref::<f64>() {
                    return Ok(DataValue::Number(Number::try_from_builtin(*val)?));
                }
            }
            DataType::Timestamp => {
                if let Some(val) = value.downcast_ref::<Number>() {
                    return Ok(DataValue::Timestamp(match *val {
                        Number::Integer(i) => Timestamp::try_from_number(i)?,
                        Number::Unsigned(u) => Timestamp::try_from_number(u)?,
                        _ => {
                            anyhow::bail!("expected integer or unsigned number")
                        }
                    }));
                } else if let Some(val) = value.downcast_ref::<Text>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_str(val)?));
                } else if let Some(val) = value.downcast_ref::<Bytes>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_slice(val)?));
                } else if let Some(val) = value.downcast_ref::<Timestamp>() {
                    return Ok(DataValue::Timestamp(*val));
                } else if let Some(val) = value.downcast_ref::<&str>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_str(*val)?));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_str(val.as_str())?));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_slice(val)?));
                } else if let Some(val) = value.downcast_ref::<Vec<u8>>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_slice(&val)?));
                } else if let Some(val) = value.downcast_ref::<i8>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i16>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i32>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i64>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i128>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<isize>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u8>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u16>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u32>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u64>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u128>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<usize>() {
                    return Ok(DataValue::Timestamp(Timestamp::try_from_number(*val)?));
                }
            }
            DataType::Text(cap) => {
                let cap = cap as usize;

                if let Some(val) = value.downcast_ref::<Text>() {
                    if val.capacity() != cap {
                        anyhow::bail!(
                            "expected text capacity of {} but got {}",
                            cap,
                            val.capacity()
                        );
                    }

                    return Ok(DataValue::Text(val.clone()));
                } else if let Some(val) = value.downcast_ref::<&str>() {
                    return Ok(DataValue::Text(Text::try_from_str(val, cap)?));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    return Ok(DataValue::Text(Text::try_from_str(val, cap)?));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    return Ok(DataValue::Text(Text::try_from_slice(val, cap)?));
                } else if let Some(val) = value.downcast_ref::<Vec<u8>>() {
                    return Ok(DataValue::Text(Text::try_from_slice(&val, cap)?));
                } else if let Some(val) = value.downcast_ref::<Number>() {
                    return Ok(DataValue::Text(Text::try_from_str(&val.to_string(), cap)?));
                } else if let Some(val) = value.downcast_ref::<Timestamp>() {
                    return Ok(DataValue::Text(Text::try_from_i128(val.as_i128(), cap)?));
                }
            }
            DataType::Bytes(cap) => {
                let cap = cap as usize;

                if let Some(val) = value.downcast_ref::<Bytes>() {
                    if val.capacity() != cap {
                        anyhow::bail!(
                            "expected bytes capacity of {} but got {}",
                            cap,
                            val.capacity()
                        );
                    }

                    return Ok(DataValue::Bytes(val.clone()));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    return Ok(DataValue::Bytes(Bytes::try_from_slice(val, cap)?));
                } else if let Some(val) = value.downcast_ref::<Vec<u8>>() {
                    return Ok(DataValue::Bytes(Bytes::try_from_slice(&val, cap)?));
                }
            }
        }

        anyhow::bail!(
            "expected value of type {:?} but got {}",
            expected_ty,
            type_name
        )
    }

    #[must_use]
    pub fn try_cast(&self, ty: impl Into<ExpectedType>) -> Result<Self> {
        let expected_ty: ExpectedType = ty.into();
        let ty = expected_ty.into_inner();

        if self.get_type() == expected_ty {
            return Ok(self.clone());
        }

        match self {
            Self::Bool(x) => match ty {
                DataType::Bool => Ok(Self::Bool(*x)),
                _ => anyhow::bail!("cannot cast bool to {:?}", ty),
            },
            Self::Number(x) => match ty {
                DataType::Bool => Ok(Self::Bool(x.is_zero())),
                DataType::Number => Ok(Self::Number(*x)),
                DataType::Text(cap) => Ok(Self::Text(Text::try_from_str(
                    &x.to_string(),
                    cap as usize,
                )?)),
                DataType::Bytes(cap) => Ok(Self::Bytes(Bytes::try_from_slice(
                    &x.to_string().as_bytes(),
                    cap as usize,
                )?)),
                DataType::Timestamp => Ok(Self::Timestamp(match *x {
                    Number::Integer(i) => Timestamp::try_from_number(i)?,
                    Number::Unsigned(u) => Timestamp::try_from_number(u)?,
                    _ => {
                        anyhow::bail!(
                            "expected integer or unsigned number while casting to timestamp"
                        )
                    }
                })),
                _ => anyhow::bail!("cannot cast number to {:?}", ty),
            },
            Self::Timestamp(x) => match ty {
                DataType::Number => Ok(Self::Number(Number::try_from_builtin(x.as_i128())?)),
                DataType::Text(cap) => {
                    Ok(Self::Text(Text::try_from_i128(x.as_i128(), cap as usize)?))
                }
                DataType::Timestamp => Ok(Self::Timestamp(*x)),
                _ => anyhow::bail!("cannot cast timestamp to {:?}", ty),
            },
            Self::Text(x) => match ty {
                DataType::Number => Ok(Self::Number(Number::try_from_str(x.as_str())?)),
                DataType::Text(cap) => Ok(Self::Text(Text::try_from_str(x, cap as usize)?)),
                DataType::Bytes(cap) => Ok(Self::Bytes(Bytes::try_from_slice(
                    x.as_bytes(),
                    cap as usize,
                )?)),
                _ => anyhow::bail!("cannot cast text to {:?}", ty),
            },
            Self::Bytes(x) => match ty {
                DataType::Text(cap) => Ok(Self::Text(Text::try_from_slice(
                    x.as_slice(),
                    cap as usize,
                )?)),
                DataType::Bytes(cap) => Ok(Self::Bytes(Bytes::try_from_slice(
                    x.as_slice(),
                    cap as usize,
                )?)),
                _ => anyhow::bail!("cannot cast bytes to {:?}", ty),
            },
            _ => anyhow::bail!("cannot cast {:?} to {:?}", self, ty),
        }
    }
}

impl From<O16> for DataValue {
    fn from(value: O16) -> Self {
        DataValue::O16(value)
    }
}

impl From<O32> for DataValue {
    fn from(value: O32) -> Self {
        DataValue::O32(value)
    }
}

impl From<O64> for DataValue {
    fn from(value: O64) -> Self {
        DataValue::O64(value)
    }
}

impl From<bool> for DataValue {
    fn from(value: bool) -> Self {
        DataValue::Bool(value)
    }
}

impl From<u8> for DataValue {
    fn from(value: u8) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("u8 always fits"))
    }
}

impl From<u16> for DataValue {
    fn from(value: u16) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("u16 always fits"))
    }
}

impl From<u32> for DataValue {
    fn from(value: u32) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("u32 always fits"))
    }
}

impl From<u64> for DataValue {
    fn from(value: u64) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("u64 always fits"))
    }
}

impl TryFrom<u128> for DataValue {
    type Error = anyhow::Error;

    fn try_from(value: u128) -> Result<Self> {
        Ok(DataValue::Number(Number::try_from_builtin(value)?))
    }
}

impl From<usize> for DataValue {
    fn from(value: usize) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("usize always fits"))
    }
}

impl From<i8> for DataValue {
    fn from(value: i8) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("i8 always fits"))
    }
}

impl From<i16> for DataValue {
    fn from(value: i16) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("i16 always fits"))
    }
}

impl From<i32> for DataValue {
    fn from(value: i32) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("i32 always fits"))
    }
}

impl From<i64> for DataValue {
    fn from(value: i64) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("i64 always fits"))
    }
}

impl From<i128> for DataValue {
    fn from(value: i128) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("i128 always fits"))
    }
}

impl From<isize> for DataValue {
    fn from(value: isize) -> Self {
        DataValue::Number(Number::try_from_builtin(value).expect("isize always fits"))
    }
}

impl From<Number> for DataValue {
    fn from(value: Number) -> Self {
        DataValue::Number(value)
    }
}

impl TryFrom<f32> for DataValue {
    type Error = anyhow::Error;

    fn try_from(value: f32) -> Result<Self> {
        Ok(DataValue::Number(Number::try_from_builtin(value)?))
    }
}

impl TryFrom<f64> for DataValue {
    type Error = anyhow::Error;

    fn try_from(value: f64) -> Result<Self> {
        Ok(DataValue::Number(Number::try_from_builtin(value)?))
    }
}

impl From<Timestamp> for DataValue {
    fn from(value: Timestamp) -> Self {
        DataValue::Timestamp(value)
    }
}

impl From<Text> for DataValue {
    fn from(value: Text) -> Self {
        DataValue::Text(value)
    }
}

impl From<Bytes> for DataValue {
    fn from(value: Bytes) -> Self {
        DataValue::Bytes(value)
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use IntSize::{X16, X8};

//     #[test]
//     fn test_casting() -> Result<()> {
//         let alloc = Arc::new(Bump::new());

//         let value = DataValue::try_integer_from_number(42i8)?;
//         let sized_up = value.try_cast(DataType::Integer(X16), &alloc)?;

//         match sized_up {
//             DataValue::Integer(i) => {
//                 assert_eq!(i.size(), X16);
//                 assert_eq!(i.as_i128(), 42);
//             }
//             _ => anyhow::bail!("expected integer"),
//         }

//         let value = DataValue::try_integer_from_number(42i64)?;
//         let sized_down = value.try_cast(DataType::Integer(X8), &alloc)?;

//         match sized_down {
//             DataValue::Integer(i) => {
//                 assert_eq!(i.size(), X8);
//                 assert_eq!(i.as_i128(), 42);
//             }
//             _ => anyhow::bail!("expected integer"),
//         }

//         Ok(())
//     }

//     #[test]
//     fn test_from_any() -> Result<()> {
//         let alloc = Arc::new(Bump::new());

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42i8, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42i8)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42i16, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42i16)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42i32, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42i32)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42i64, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42i64)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42i128, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42i128)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42isize, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42isize)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42u8, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42u8)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42u16, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42u16)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42u32, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42u32)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42u64, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42u64)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42u128, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42u128)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &42usize, &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin(42usize)?)
//         );

//         let value = DataValue::try_from_any(DataType::Integer(X8), &"42", &alloc)?;
//         assert_eq!(
//             value,
//             DataValue::Number(Number::try_from_builtin("42")?)
//         );

//         Ok(())
//     }
// }