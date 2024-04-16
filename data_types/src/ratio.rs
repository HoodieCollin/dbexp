use std::{any::TypeId, ops};

use anyhow::Result;
use fraction::FromPrimitive;

pub trait Number: Copy + 'static {}

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
pub struct Ratio(fraction::Ratio<i64>);

impl Ratio {
    pub fn new() -> Self {
        Self(fraction::Ratio::new_raw(0, 1))
    }

    /// ## Safety
    /// This function is unsafe because it does not check if the ratio is valid.
    #[inline(always)]
    pub unsafe fn from_raw_ratio(ratio: fraction::Ratio<i64>) -> Self {
        Self(ratio)
    }

    #[inline(always)]
    pub unsafe fn from_parts(numer: i64, denom: i64) -> Self {
        Self(fraction::Ratio::new_raw(numer, denom))
    }

    #[inline(always)]
    pub fn into_parts(self) -> (i64, i64) {
        (*self.0.numer(), *self.0.denom())
    }

    #[inline(always)]
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

        let inner = unsafe {
            let val = Transmute { n };

            match TypeId::of::<T>() {
                t if t == TypeId::of::<i8>() => fraction::Ratio::from_i8(val.i8),
                t if t == TypeId::of::<i16>() => fraction::Ratio::from_i16(val.i16),
                t if t == TypeId::of::<i32>() => fraction::Ratio::from_i32(val.i32),
                t if t == TypeId::of::<i64>() => fraction::Ratio::from_i64(val.i64),
                t if t == TypeId::of::<i128>() => fraction::Ratio::from_i128(val.i128),
                t if t == TypeId::of::<isize>() => fraction::Ratio::from_isize(val.isize),
                t if t == TypeId::of::<u8>() => fraction::Ratio::from_u8(val.u8),
                t if t == TypeId::of::<u16>() => fraction::Ratio::from_u16(val.u16),
                t if t == TypeId::of::<u32>() => fraction::Ratio::from_u32(val.u32),
                t if t == TypeId::of::<u64>() => fraction::Ratio::from_u64(val.u64),
                t if t == TypeId::of::<u128>() => fraction::Ratio::from_u128(val.u128),
                t if t == TypeId::of::<usize>() => fraction::Ratio::from_usize(val.usize),
                t if t == TypeId::of::<f32>() => fraction::Ratio::from_f32(val.f32),
                t if t == TypeId::of::<f64>() => fraction::Ratio::from_f64(val.f64),
                _ => anyhow::bail!("Failed to convert number to ratio"),
            }
        };

        inner
            .ok_or_else(|| anyhow::anyhow!("Failed to convert integer to ratio"))
            .map(Ratio)
    }

    pub fn try_from_str(s: &str) -> Result<Self> {
        s.parse()
            .map(Ratio)
            .map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    pub fn into_inner(self) -> fraction::Ratio<i64> {
        self.0
    }

    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        Self(fraction::Ratio::new(
            i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        ))
    }

    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(fraction::Ratio::new(
            i64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            i64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        ))
    }

    pub fn to_le_bytes(&self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&self.0.numer().to_le_bytes());
        bytes[8..16].copy_from_slice(&self.0.denom().to_le_bytes());
        bytes
    }

    pub fn to_be_bytes(&self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&self.0.numer().to_be_bytes());
        bytes[8..16].copy_from_slice(&self.0.denom().to_be_bytes());
        bytes
    }
}

impl ops::Deref for Ratio {
    type Target = fraction::Ratio<i64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for Ratio {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<fraction::Ratio<i64>> for Ratio {
    fn as_ref(&self) -> &fraction::Ratio<i64> {
        &self.0
    }
}

impl AsMut<fraction::Ratio<i64>> for Ratio {
    fn as_mut(&mut self) -> &mut fraction::Ratio<i64> {
        &mut self.0
    }
}

impl std::str::FromStr for Ratio {
    type Err = fraction::ParseRatioError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Ratio)
    }
}

impl std::fmt::Debug for Ratio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for Ratio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for Ratio {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Ratio {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map(Ratio)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}
