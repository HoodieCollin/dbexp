use std::{any::TypeId, ops};

use anyhow::Result;
use fraction::FromPrimitive;

pub trait Number: Copy + 'static {
    fn is_integer(&self) -> bool {
        TypeId::of::<Self>() != TypeId::of::<f32>() && TypeId::of::<Self>() != TypeId::of::<f64>()
    }
}

impl Number for i8 {}
impl Number for i16 {}
impl Number for i32 {}
impl Number for i64 {}
impl Number for i128 {}
impl Number for isize {}
impl Number for u8 {}
impl Number for u16 {}
impl Number for u32 {}
impl Number for u64 {}
impl Number for u128 {}
impl Number for usize {}
impl Number for f32 {}
impl Number for f64 {}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Decimal(rust_decimal::Decimal);

impl Decimal {
    pub fn new() -> Self {
        Self(rust_decimal::Decimal::ZERO)
    }

    pub fn from_raw_decimal(decimal: rust_decimal::Decimal) -> Self {
        Self(decimal)
    }

    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        Self(rust_decimal::Decimal::deserialize(
            u128::from_le_bytes(bytes).to_ne_bytes(),
        ))
    }

    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(rust_decimal::Decimal::deserialize(
            u128::from_be_bytes(bytes).to_ne_bytes(),
        ))
    }

    pub fn to_le_bytes(&self) -> [u8; 16] {
        u128::from_ne_bytes(self.0.serialize()).to_le_bytes()
    }

    pub fn to_be_bytes(&self) -> [u8; 16] {
        u128::from_ne_bytes(self.0.serialize()).to_be_bytes()
    }

    pub fn to_integer(&self) -> i128 {
        self.0.round().mantissa()
    }

    pub fn try_from_number<T: Number>(n: T) -> Result<Self> {
        union Transmute<N: Number> {
            n: N,
            i8: i8,
            i16: i16,
            i32: i32,
            i64: i64,
            i128: i128,
            isize: isize,
            u8: u8,
            u16: u16,
            u32: u32,
            u64: u64,
            u128: u128,
            usize: usize,
            f32: f32,
            f64: f64,
        }

        if n.is_integer() {
            let inner = unsafe {
                let val = Transmute { n };

                match TypeId::of::<T>() {
                    t if t == TypeId::of::<i8>() => rust_decimal::Decimal::from(val.i8),
                    t if t == TypeId::of::<i16>() => rust_decimal::Decimal::from(val.i16),
                    t if t == TypeId::of::<i32>() => rust_decimal::Decimal::from(val.i32),
                    t if t == TypeId::of::<i64>() => rust_decimal::Decimal::from(val.i64),
                    t if t == TypeId::of::<i128>() => rust_decimal::Decimal::from(val.i128),
                    t if t == TypeId::of::<isize>() => rust_decimal::Decimal::from(val.isize),
                    t if t == TypeId::of::<u8>() => rust_decimal::Decimal::from(val.u8),
                    t if t == TypeId::of::<u16>() => rust_decimal::Decimal::from(val.u16),
                    t if t == TypeId::of::<u32>() => rust_decimal::Decimal::from(val.u32),
                    t if t == TypeId::of::<u64>() => rust_decimal::Decimal::from(val.u64),
                    t if t == TypeId::of::<u128>() => rust_decimal::Decimal::from(val.u128),
                    t if t == TypeId::of::<usize>() => rust_decimal::Decimal::from(val.usize),
                    _ => unreachable!(),
                }
            };

            Ok(Self(inner))
        } else {
            let inner = unsafe {
                let val = Transmute { n };

                match TypeId::of::<T>() {
                    t if t == TypeId::of::<f32>() => rust_decimal::Decimal::from_f32(val.f32),
                    t if t == TypeId::of::<f64>() => rust_decimal::Decimal::from_f64(val.f64),
                    _ => unreachable!(),
                }
            };

            Ok(Self(inner.ok_or_else(|| {
                anyhow::anyhow!("Failed to convert float to decimal")
            })?))
        }
    }

    pub fn try_from_str(s: &str) -> Result<Self> {
        if let Ok(inner) = rust_decimal::Decimal::from_scientific(s) {
            return Ok(Self(inner));
        }

        rust_decimal::Decimal::from_str_exact(s)
            .map(Self)
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    pub fn into_inner(self) -> rust_decimal::Decimal {
        self.0
    }
}

impl ops::Deref for Decimal {
    type Target = rust_decimal::Decimal;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for Decimal {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::fmt::Debug for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for Decimal {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Decimal {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map(Decimal)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}
