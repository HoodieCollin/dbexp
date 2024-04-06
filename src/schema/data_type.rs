use std::any::type_name;

use anyhow::Result;

use super::data_value::DataValue;
use crate::decimal::Decimal;
use crate::ratio::{R128, R16, R32, R64};
use crate::timestamp::Timestamp;
use crate::uid::Uid;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Copy, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    #[default]
    Nil,
    Uid,
    Timestamp,
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    R16,
    R32,
    R64,
    R128,
    Decimal,
    String,
}

impl DataType {
    pub fn from_type<T>() -> Self {
        if type_name::<T>() == type_name::<Uid>() {
            return Self::Uid;
        }

        if type_name::<T>() == type_name::<Timestamp>() {
            return Self::Timestamp;
        }

        if type_name::<T>() == type_name::<i128>() {
            return Self::I128;
        }

        if type_name::<T>() == type_name::<i64>() {
            return Self::I64;
        }

        if type_name::<T>() == type_name::<i32>() {
            return Self::I32;
        }

        if type_name::<T>() == type_name::<i16>() {
            return Self::I16;
        }

        if type_name::<T>() == type_name::<i8>() {
            return Self::I8;
        }

        if type_name::<T>() == type_name::<u128>() {
            return Self::U128;
        }

        if type_name::<T>() == type_name::<u64>() {
            return Self::U64;
        }

        if type_name::<T>() == type_name::<u32>() {
            return Self::U32;
        }

        if type_name::<T>() == type_name::<u16>() {
            return Self::U16;
        }

        if type_name::<T>() == type_name::<u8>() {
            return Self::U8;
        }

        if type_name::<T>() == type_name::<R128>() {
            return Self::R128;
        }

        if type_name::<T>() == type_name::<R64>() {
            return Self::R64;
        }

        if type_name::<T>() == type_name::<R32>() {
            return Self::R32;
        }

        if type_name::<T>() == type_name::<R16>() {
            return Self::R16;
        }

        if type_name::<T>() == type_name::<Decimal>() {
            return Self::Decimal;
        }

        if type_name::<T>() == type_name::<String>() {
            return Self::String;
        }

        Self::Nil
    }

    pub fn size_as_bytes(&self) -> usize {
        match self {
            Self::Nil => 0,
            Self::Uid => 16,
            Self::Timestamp => 8,
            Self::U8 => 1,
            Self::U16 => 2,
            Self::U32 => 4,
            Self::U64 => 8,
            Self::U128 => 16,
            Self::I8 => 1,
            Self::I16 => 2,
            Self::I32 => 4,
            Self::I64 => 8,
            Self::I128 => 16,
            Self::R16 => 2,
            Self::R32 => 4,
            Self::R64 => 8,
            Self::R128 => 16,
            Self::Decimal => 16,
            Self::String => panic!("not implemented"),
        }
    }

    pub fn new_value(&self) -> DataValue {
        match self {
            Self::Nil => DataValue::Nil,
            Self::Uid => DataValue::Uid(Uid::default()),
            Self::Timestamp => DataValue::Timestamp(Timestamp::default()),
            Self::U8 => DataValue::U8(0),
            Self::U16 => DataValue::U16(0),
            Self::U32 => DataValue::U32(0),
            Self::U64 => DataValue::U64(0),
            Self::U128 => DataValue::U128(0),
            Self::I8 => DataValue::I8(0),
            Self::I16 => DataValue::I16(0),
            Self::I32 => DataValue::I32(0),
            Self::I64 => DataValue::I64(0),
            Self::I128 => DataValue::I128(0),
            Self::R16 => DataValue::R16(R16::default()),
            Self::R32 => DataValue::R32(R32::default()),
            Self::R64 => DataValue::R64(R64::default()),
            Self::R128 => DataValue::R128(R128::default()),
            Self::Decimal => DataValue::Decimal(Decimal::default()),
            Self::String => DataValue::String(String::new()),
        }
    }

    pub fn new_value_from(&self, bytes: &[u8]) -> Result<DataValue> {
        let mut value = self.new_value();
        value.copy_from(bytes)?;
        Ok(value)
    }
}
