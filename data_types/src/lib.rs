use std::{
    any::{type_name, Any},
    fmt::Debug,
    ops,
    sync::Arc,
};

use anyhow::Result;
use bumpalo::Bump;
use integer::IntSize;

pub mod bytes;
pub mod decimal;
pub mod integer;
pub mod oid;
pub mod ratio;
pub mod text;
pub mod timestamp;
pub mod uid;

//
mod math;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataType {
    Bool,
    Integer(IntSize),
    Ratio,
    Uid,
    O16,
    O32,
    Decimal,
    Timestamp,
    Text(u32),
    Bytes(u32),
}

impl DataType {
    #[inline(always)]
    pub fn into_value(self) -> DataValue {
        DataValue::Nil(self.into())
    }
}

/// A wrapper around `DataType` that represents an expected type. The inner `DataType`
/// should never be changed once set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl ops::Deref for ExpectedType {
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DataValue {
    Nil(ExpectedType),
    Bool(bool),
    Integer(integer::Integer),
    Ratio(ratio::Ratio),
    Uid(uid::Uid),
    O16(oid::O16),
    O32(oid::O32),
    Decimal(decimal::Decimal),
    Timestamp(timestamp::Timestamp),
    Text(text::Text),
    Bytes(bytes::Bytes),
}

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
            DataValue::Bool(_) => ExpectedType(DataType::Bool),
            DataValue::Integer(i) => ExpectedType(DataType::Integer(i.size())),
            DataValue::Ratio(_) => ExpectedType(DataType::Ratio),
            DataValue::Uid(_) => ExpectedType(DataType::Uid),
            DataValue::O16(_) => ExpectedType(DataType::O16),
            DataValue::O32(_) => ExpectedType(DataType::O32),
            DataValue::Decimal(_) => ExpectedType(DataType::Decimal),
            DataValue::Timestamp(_) => ExpectedType(DataType::Timestamp),
            DataValue::Text(val) => ExpectedType(DataType::Text(val.capacity() as u32)),
            DataValue::Bytes(val) => ExpectedType(DataType::Bytes(val.capacity() as u32)),
        }
    }

    pub fn try_integer_from_number<T: integer::Number>(value: T) -> Result<Self> {
        Ok(DataValue::Integer(integer::Integer::try_from_number(
            value,
        )?))
    }

    pub fn try_integer_from_str(value: &str) -> Result<Self> {
        Ok(DataValue::Integer(integer::Integer::try_from_str(value)?))
    }

    pub fn try_ratio_from_number<T: ratio::Number>(value: T) -> Result<Self> {
        Ok(DataValue::Ratio(ratio::Ratio::try_from_number(value)?))
    }

    pub fn try_ratio_from_str(value: &str) -> Result<Self> {
        Ok(DataValue::Ratio(ratio::Ratio::try_from_str(value)?))
    }

    pub fn try_decimal_from_number<T: decimal::Number>(value: T) -> Result<Self> {
        Ok(DataValue::Decimal(decimal::Decimal::try_from_number(
            value,
        )?))
    }

    pub fn try_decimal_from_str(value: &str) -> Result<Self> {
        Ok(DataValue::Decimal(decimal::Decimal::try_from_str(value)?))
    }

    /// Tries to replace the current value with the given value. If the value is not of the
    /// expected type, an error is returned.
    ///
    /// This is useful during arithmetic operations where the result is expected to be of the
    /// same type as the left operand.
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

    pub fn try_from_any<T: Into<ExpectedType>, V: Any>(
        ty: T,
        value: &V,
        alloc: &Arc<Bump>,
    ) -> Result<Self> {
        let expected_ty: ExpectedType = ty.into();
        let type_name = type_name::<V>();
        let value = value as &dyn Any;

        if let Some(val) = value.downcast_ref::<DataValue>() {
            if !expected_ty.check(val) {
                anyhow::bail!(
                    "expected value of type {:?} but got {:?}",
                    expected_ty,
                    val.get_type()
                );
            }

            return Ok(val.clone());
        }

        match expected_ty.into_inner() {
            DataType::Bool => {
                if let Some(val) = value.downcast_ref::<bool>() {
                    return Ok(DataValue::Bool(*val));
                }
            }
            DataType::Integer(size) => {
                if let Some(val) = value.downcast_ref::<integer::Integer>() {
                    if val.size() != size {
                        anyhow::bail!(
                            "expected integer size of {:?} but got {:?}",
                            size,
                            val.size()
                        );
                    }

                    return Ok(DataValue::Integer(*val));
                } else if let Some(val) = value.downcast_ref::<&str>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_str(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_str(val.as_str())?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<i8>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<i16>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<i32>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<i64>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<i128>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<isize>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<u8>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<u16>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<u32>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<u64>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<u128>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                } else if let Some(val) = value.downcast_ref::<usize>() {
                    return Ok(DataValue::Integer(
                        integer::Integer::try_from_number(*val)?.try_to_fit(size)?,
                    ));
                }
            }
            DataType::Ratio => {
                if let Some(val) = value.downcast_ref::<ratio::Ratio>() {
                    return Ok(DataValue::Ratio(*val));
                } else if let Some(val) = value.downcast_ref::<&str>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_str(val)?));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_str(val.as_str())?));
                } else if let Some(val) = value.downcast_ref::<f32>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<f64>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i8>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i16>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i32>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i64>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i128>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<isize>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u8>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u16>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u32>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u64>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u128>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<usize>() {
                    return Ok(DataValue::Ratio(ratio::Ratio::try_from_number(*val)?));
                }
            }
            DataType::Uid => {
                if let Some(val) = value.downcast_ref::<uid::Uid>() {
                    return Ok(DataValue::Uid(*val));
                }
            }
            DataType::O16 => {
                if let Some(val) = value.downcast_ref::<oid::O16>() {
                    return Ok(DataValue::O16(*val));
                }
            }
            DataType::O32 => {
                if let Some(val) = value.downcast_ref::<oid::O32>() {
                    return Ok(DataValue::O32(*val));
                }
            }
            DataType::Decimal => {
                if let Some(val) = value.downcast_ref::<decimal::Decimal>() {
                    return Ok(DataValue::Decimal(*val));
                } else if let Some(val) = value.downcast_ref::<&str>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_str(val)?));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_str(
                        val.as_str(),
                    )?));
                } else if let Some(val) = value.downcast_ref::<f32>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<f64>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i8>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i16>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i32>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i64>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<i128>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<isize>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u8>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u16>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u32>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u64>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<u128>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                } else if let Some(val) = value.downcast_ref::<usize>() {
                    return Ok(DataValue::Decimal(decimal::Decimal::try_from_number(*val)?));
                }
            }
            DataType::Timestamp => {
                if let Some(val) = value.downcast_ref::<timestamp::Timestamp>() {
                    return Ok(DataValue::Timestamp(*val));
                } else if let Some(val) = value.downcast_ref::<i64>() {
                    return Ok(DataValue::Timestamp(timestamp::Timestamp::from_integer(
                        *val,
                    )?));
                }

                // TODO: other integer types
                // TODO: from strings
            }
            DataType::Text(cap) => {
                if let Some(val) = value.downcast_ref::<text::Text>() {
                    if val.capacity() != cap as usize {
                        anyhow::bail!(
                            "expected text capacity of {} but got {}",
                            cap,
                            val.capacity()
                        );
                    }

                    return Ok(DataValue::Text(val.clone()));
                } else if let Some(val) = value.downcast_ref::<&str>() {
                    return Ok(DataValue::Text(text::Text::from_str(val, cap, alloc)?));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    return Ok(DataValue::Text(text::Text::from_str(val, cap, alloc)?));
                }
            }
            DataType::Bytes(cap) => {
                if let Some(val) = value.downcast_ref::<bytes::Bytes>() {
                    if val.capacity() != cap as usize {
                        anyhow::bail!(
                            "expected bytes capacity of {} but got {}",
                            cap,
                            val.capacity()
                        );
                    }

                    return Ok(DataValue::Bytes(val.clone()));
                } else if let Some(val) = value.downcast_ref::<&[u8]>() {
                    return Ok(DataValue::Bytes(bytes::Bytes::from_slice(val, cap, alloc)?));
                } else if let Some(val) = value.downcast_ref::<Vec<u8>>() {
                    return Ok(DataValue::Bytes(bytes::Bytes::from_slice(
                        &val, cap, alloc,
                    )?));
                }
            }
        }

        anyhow::bail!(
            "expected value of type {:?} but got {}",
            expected_ty,
            type_name
        )
    }

    pub fn is_integer(&self) -> bool {
        match self {
            DataValue::Integer(_) => true,
            DataValue::Ratio(r) => r.is_integer(),
            DataValue::Decimal(d) => d.is_integer(),
            _ => false,
        }
    }

    pub fn is_float(&self) -> bool {
        match self {
            DataValue::Ratio(r) => !r.is_integer(),
            DataValue::Decimal(d) => !d.is_integer(),
            _ => false,
        }
    }

    pub fn try_cast(&self, ty: impl Into<ExpectedType>) -> Result<Self> {
        use DataType::{
            Bool as BoolTy, Decimal as DecimalTy, Integer as IntegerTy, Ratio as RatioTy,
        };
        use DataValue::{Bool, Bytes, Decimal, Integer, Ratio, Text, Timestamp};

        let expected_ty: ExpectedType = ty.into();
        let ty = expected_ty.into_inner();

        if self.get_type() == expected_ty {
            return Ok(self.clone());
        }

        match ty {
            BoolTy => match self {
                Bool(_) => Ok(self.clone()),
                Integer(i) => Ok(Bool(i.into_inner() != 0)),
                Ratio(r) => Ok(Bool(r.is_integer() && r.numer() != &0)),
                Decimal(d) => Ok(Bool(d.is_integer() && d.to_integer() != 0)),
                Text(t) => Ok(Bool(!t.is_empty())),
                Bytes(b) => Ok(Bool(!b.is_empty())),
                _ => anyhow::bail!("cannot cast {:?} to bool", self),
            },
            IntegerTy(size) => match self {
                Integer(i) => Ok(Self::Integer(i.try_to_fit(size)?)),
                Ratio(r) => Ok(Self::Integer(
                    integer::Integer::try_from_number(r.to_integer())?.try_to_fit(size)?,
                )),
                Decimal(d) => Ok(Self::Integer(
                    integer::Integer::try_from_number(d.to_integer())?.try_to_fit(size)?,
                )),
                Timestamp(t) => Ok(Self::Integer(
                    integer::Integer::try_from_number(t.to_integer())?.try_to_fit(size)?,
                )),
                _ => anyhow::bail!("cannot cast {:?} to integer", self),
            },
            RatioTy => match self {
                Integer(i) => Self::try_ratio_from_number(i.into_inner()),
                Ratio(_) => Ok(self.clone()),
                Decimal(d) => Self::try_ratio_from_str(&d.to_string()),
                _ => anyhow::bail!("cannot cast {:?} to ratio", self),
            },
            DecimalTy => match self {
                Integer(i) => Self::try_decimal_from_number(i.into_inner()),
                Ratio(r) => Self::try_decimal_from_str(&r.to_string()),
                Decimal(_) => Ok(self.clone()),
                _ => anyhow::bail!("cannot cast {:?} to decimal", self),
            },
            _ => anyhow::bail!("cannot cast {:?} to {:?}", self, ty),
        }
    }
}

impl From<bool> for DataValue {
    fn from(value: bool) -> Self {
        DataValue::Bool(value)
    }
}

impl From<u8> for DataValue {
    fn from(value: u8) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("u8 always fits"))
    }
}

impl From<u16> for DataValue {
    fn from(value: u16) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("u16 always fits"))
    }
}

impl From<u32> for DataValue {
    fn from(value: u32) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("u32 always fits"))
    }
}

impl From<u64> for DataValue {
    fn from(value: u64) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("u64 always fits"))
    }
}

impl TryFrom<u128> for DataValue {
    type Error = anyhow::Error;

    fn try_from(value: u128) -> Result<Self> {
        Ok(DataValue::Integer(integer::Integer::try_from_number(
            value,
        )?))
    }
}

impl From<usize> for DataValue {
    fn from(value: usize) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("usize always fits"))
    }
}

impl From<i8> for DataValue {
    fn from(value: i8) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("i8 always fits"))
    }
}

impl From<i16> for DataValue {
    fn from(value: i16) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("i16 always fits"))
    }
}

impl From<i32> for DataValue {
    fn from(value: i32) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("i32 always fits"))
    }
}

impl From<i64> for DataValue {
    fn from(value: i64) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("i64 always fits"))
    }
}

impl From<i128> for DataValue {
    fn from(value: i128) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("i128 always fits"))
    }
}

impl From<isize> for DataValue {
    fn from(value: isize) -> Self {
        DataValue::Integer(integer::Integer::try_from_number(value).expect("isize always fits"))
    }
}

impl From<ratio::Ratio> for DataValue {
    fn from(value: ratio::Ratio) -> Self {
        DataValue::Ratio(value)
    }
}

impl TryFrom<f32> for DataValue {
    type Error = anyhow::Error;

    fn try_from(value: f32) -> Result<Self> {
        Ok(DataValue::Ratio(ratio::Ratio::try_from_number(value)?))
    }
}

impl TryFrom<f64> for DataValue {
    type Error = anyhow::Error;

    fn try_from(value: f64) -> Result<Self> {
        Ok(DataValue::Ratio(ratio::Ratio::try_from_number(value)?))
    }
}

impl From<uid::Uid> for DataValue {
    fn from(value: uid::Uid) -> Self {
        DataValue::Uid(value)
    }
}

impl From<oid::O16> for DataValue {
    fn from(value: oid::O16) -> Self {
        DataValue::O16(value)
    }
}

impl From<oid::O32> for DataValue {
    fn from(value: oid::O32) -> Self {
        DataValue::O32(value)
    }
}

impl From<decimal::Decimal> for DataValue {
    fn from(value: decimal::Decimal) -> Self {
        DataValue::Decimal(value)
    }
}

impl From<timestamp::Timestamp> for DataValue {
    fn from(value: timestamp::Timestamp) -> Self {
        DataValue::Timestamp(value)
    }
}

impl From<text::Text> for DataValue {
    fn from(value: text::Text) -> Self {
        DataValue::Text(value)
    }
}

impl From<bytes::Bytes> for DataValue {
    fn from(value: bytes::Bytes) -> Self {
        DataValue::Bytes(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_casting() -> Result<()> {
        let value = DataValue::try_integer_from_number(42i8)?;
        let sized_up = value.try_cast(DataType::Integer(IntSize::X16))?;

        match sized_up {
            DataValue::Integer(i) => {
                assert_eq!(i.size(), IntSize::X16);
                assert_eq!(i.into_inner(), 42);
            }
            _ => anyhow::bail!("expected integer"),
        }

        let value = DataValue::try_integer_from_number(42i64)?;
        let sized_down = value.try_cast(DataType::Integer(IntSize::X8))?;

        match sized_down {
            DataValue::Integer(i) => {
                assert_eq!(i.size(), IntSize::X8);
                assert_eq!(i.into_inner(), 42);
            }
            _ => anyhow::bail!("expected integer"),
        }

        Ok(())
    }

    #[test]
    fn test_from_any() -> Result<()> {
        let alloc = Arc::new(Bump::new());

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42i8, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42i8)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42i16, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42i16)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42i32, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42i32)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42i64, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42i64)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42i128, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42i128)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42isize, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42isize)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42u8, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42u8)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42u16, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42u16)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42u32, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42u32)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42u64, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42u64)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42u128, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42u128)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &42usize, &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_number(42usize)?)
        );

        let value = DataValue::try_from_any(DataType::Integer(IntSize::X8), &"42", &alloc)?;
        assert_eq!(
            value,
            DataValue::Integer(integer::Integer::try_from_str("42")?)
        );

        Ok(())
    }
}
