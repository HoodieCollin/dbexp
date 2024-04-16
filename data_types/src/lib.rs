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
        alloc: Option<&Arc<Bump>>,
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
                }
            }
            DataType::Ratio => {
                if let Some(val) = value.downcast_ref::<ratio::Ratio>() {
                    return Ok(DataValue::Ratio(*val));
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
                }
            }
            DataType::Timestamp => {
                if let Some(val) = value.downcast_ref::<timestamp::Timestamp>() {
                    return Ok(DataValue::Timestamp(*val));
                }
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
                    let alloc = alloc.ok_or_else(|| anyhow::anyhow!("missing allocator"))?;

                    return Ok(DataValue::Text(text::Text::from_str(val, cap, alloc)?));
                } else if let Some(val) = value.downcast_ref::<String>() {
                    let alloc = alloc.ok_or_else(|| anyhow::anyhow!("missing allocator"))?;

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
                    let alloc = alloc.ok_or_else(|| anyhow::anyhow!("missing allocator"))?;

                    return Ok(DataValue::Bytes(bytes::Bytes::from_slice(val, cap, alloc)?));
                } else if let Some(val) = value.downcast_ref::<Vec<u8>>() {
                    let alloc = alloc.ok_or_else(|| anyhow::anyhow!("missing allocator"))?;

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
