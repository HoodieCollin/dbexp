use anyhow::Result;
use serde::{Deserialize, Serialize};

const NAN: u8 = 1;
const POS_INFINITY: u8 = 2;
const NEG_INFINITY: u8 = 3;
const FLOAT: u8 = 4;
const INTEGER: u8 = 5;
const UNSIGNED: u8 = 6;

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct U24([u8; 3]);

impl std::fmt::Debug for U24 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_usize())
    }
}

impl std::fmt::Display for U24 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_usize())
    }
}

impl PartialEq<usize> for U24 {
    fn eq(&self, other: &usize) -> bool {
        self.into_usize() == *other
    }
}

impl PartialEq<U24> for usize {
    fn eq(&self, other: &U24) -> bool {
        *self == other.into_usize()
    }
}

impl PartialEq<u32> for U24 {
    fn eq(&self, other: &u32) -> bool {
        self.into_u32() == *other
    }
}

impl PartialEq<U24> for u32 {
    fn eq(&self, other: &U24) -> bool {
        *self == other.into_u32()
    }
}

impl PartialOrd for U24 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.into_usize().partial_cmp(&other.into_usize())
    }
}

impl Ord for U24 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.into_usize().cmp(&other.into_usize())
    }
}

impl std::hash::Hash for U24 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.into_usize().hash(state)
    }
}

impl Serialize for U24 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.into_u32())
    }
}

impl<'de> Deserialize<'de> for U24 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let x = usize::deserialize(deserializer)?;
        Ok(Self::new(x).map_err(serde::de::Error::custom)?)
    }
}

#[inline(always)]
fn u32_to_3_bytes(x: u32) -> [u8; 3] {
    let bytes = x.to_ne_bytes();
    let mut buf = [0; 3];

    #[cfg(target_endian = "little")]
    {
        buf.copy_from_slice(&bytes[..3]);
    }

    #[cfg(target_endian = "big")]
    {
        buf.copy_from_slice(&bytes[1..]);
    }

    buf
}

#[inline(always)]
fn u24_bytes_to_u32(bytes: [u8; 3]) -> u32 {
    let mut buf = [0; 4];

    #[cfg(target_endian = "little")]
    {
        buf[1..].copy_from_slice(&bytes);
    }

    #[cfg(target_endian = "big")]
    {
        buf[..3].copy_from_slice(&bytes);
    }

    u32::from_ne_bytes(buf)
}

impl U24 {
    pub const MIN: usize = 0;
    pub const MAX: usize = 0xFFFFFF;
    pub const BITS: usize = 24;
    pub const BYTE_COUNT: usize = 3;

    pub fn new(x: usize) -> Result<Self> {
        if x > Self::MAX {
            anyhow::bail!("Value is too large for U24");
        }

        Ok(Self(u32_to_3_bytes(x as u32)))
    }

    pub fn into_u32(self) -> u32 {
        u24_bytes_to_u32(self.0)
    }

    pub fn into_usize(self) -> usize {
        self.into_u32() as usize
    }

    pub fn into_array(self) -> [u8; 3] {
        self.0
    }

    pub fn from_array(bytes: [u8; 3]) -> Option<Self> {
        let x = u24_bytes_to_u32(bytes);

        if x > Self::MAX as u32 {
            None
        } else {
            Some(Self(bytes))
        }
    }
}

/// Invariant: NaN, Infinity, and -Infinity are not valid numbers. Float will never be NaN, Infinity, or -Infinity.
#[derive(Debug, Clone, Copy)]
pub enum Number {
    NaN,
    Infinity(bool),
    Float(f64),
    Integer(i64),
    Unsigned(u64),
}

impl std::hash::Hash for Number {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Number::NaN => NAN.hash(state),
            Number::Infinity(sign) => {
                if *sign {
                    POS_INFINITY.hash(state);
                } else {
                    NEG_INFINITY.hash(state);
                }
            }
            Number::Float(f) => {
                FLOAT.hash(state);
                f.to_bits().hash(state);
            }
            Number::Integer(i) => {
                INTEGER.hash(state);
                i.hash(state);
            }
            Number::Unsigned(u) => {
                UNSIGNED.hash(state);
                u.hash(state);
            }
        }
    }
}

impl Number {
    pub const BYTE_COUNT: usize = 9;

    pub fn try_from_str(s: &str) -> Result<Self> {
        if let Ok(f) = s.parse::<f64>() {
            Ok(Number::Float(f))
        } else if let Ok(i) = s.parse::<i64>() {
            Ok(Number::Integer(i))
        } else if let Ok(u) = s.parse::<u64>() {
            Ok(Number::Unsigned(u))
        } else {
            Err(anyhow::anyhow!("Invalid number: {}", s))
        }
    }

    pub fn try_as_hcl_number(self) -> Option<hcl::Number> {
        match self {
            Number::NaN => None,
            Number::Infinity(..) => None,
            Number::Float(f) => hcl::Number::from_f64(f),
            Number::Integer(i) => Some(hcl::Number::from(i)),
            Number::Unsigned(u) => Some(hcl::Number::from(u)),
        }
    }

    pub fn into_array(self) -> [u8; 9] {
        let mut buf = [0; Self::BYTE_COUNT];

        match self {
            Number::NaN => buf[0] = NAN,
            Number::Infinity(sign) => buf[0] = if sign { POS_INFINITY } else { NEG_INFINITY },
            Number::Float(f) => {
                buf[0] = FLOAT;
                buf[1..].copy_from_slice(&f.to_bits().to_ne_bytes());
            }
            Number::Integer(i) => {
                buf[0] = INTEGER;
                buf[1..].copy_from_slice(&i.to_ne_bytes());
            }
            Number::Unsigned(u) => {
                buf[0] = UNSIGNED;
                buf[1..].copy_from_slice(&u.to_ne_bytes());
            }
        }

        buf
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::BYTE_COUNT {
            anyhow::bail!("Invalid number length");
        }

        Ok(match bytes[0] {
            NAN => Number::NaN,
            POS_INFINITY => Number::Infinity(true),
            NEG_INFINITY => Number::Infinity(false),
            FLOAT => {
                let bits = u64::from_ne_bytes(bytes[1..].try_into()?);
                Number::Float(f64::from_bits(bits))
            }
            INTEGER => {
                let i = i64::from_ne_bytes(bytes[1..].try_into()?);
                Number::Integer(i)
            }
            UNSIGNED => {
                let u = u64::from_ne_bytes(bytes[1..].try_into()?);
                Number::Unsigned(u)
            }
            _ => anyhow::bail!("Invalid number type"),
        })
    }

    pub fn try_from_builtin<T: Builtin>(x: T) -> Result<Self> {
        Ok(match T::KIND {
            NumKind::I8 => Number::Integer(x.as_i64()?),
            NumKind::I16 => Number::Integer(x.as_i64()?),
            NumKind::I32 => Number::Integer(x.as_i64()?),
            NumKind::I64 => Number::Integer(x.as_i64()?),
            NumKind::I128 => Number::Integer(x.as_i64()?),
            NumKind::ISize => Number::Integer(x.as_i64()?),
            NumKind::U8 => Number::Unsigned(x.as_u64()?),
            NumKind::U16 => Number::Unsigned(x.as_u64()?),
            NumKind::U32 => Number::Unsigned(x.as_u64()?),
            NumKind::U64 => Number::Unsigned(x.as_u64()?),
            NumKind::U128 => Number::Unsigned(x.as_u64()?),
            NumKind::USize => Number::Unsigned(x.as_u64()?),
            NumKind::F32 => Number::Float(unsafe { x.assume_f32() as f64 }),
            NumKind::F64 => Number::Float(unsafe { x.assume_f64() }),
        })
    }

    pub fn is_zero(&self) -> bool {
        match self {
            Number::Float(f) => *f == 0.0,
            Number::Integer(i) => *i == 0,
            Number::Unsigned(u) => *u == 0,
            _ => false,
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            Number::NaN => false,
            Number::Infinity(..) => false,
            _ => true,
        }
    }
}

impl std::fmt::Display for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Number::NaN => write!(f, "NaN"),
            Number::Infinity(true) => write!(f, "Infinity"),
            Number::Infinity(false) => write!(f, "-Infinity"),
            Number::Float(x) => {
                let mut buf = ryu::Buffer::new();
                write!(f, "{}", buf.format(*x))
            }
            Number::Integer(i) => {
                let mut buf = itoa::Buffer::new();
                write!(f, "{}", buf.format(*i))
            }
            Number::Unsigned(u) => {
                let mut buf = itoa::Buffer::new();
                write!(f, "{}", buf.format(*u))
            }
        }
    }
}

impl From<f64> for Number {
    fn from(f: f64) -> Self {
        if f.is_nan() {
            Number::NaN
        } else if f.is_infinite() {
            Number::Infinity(f.is_sign_positive())
        } else {
            Number::Float(f)
        }
    }
}

impl From<i64> for Number {
    fn from(i: i64) -> Self {
        Number::Integer(i)
    }
}

impl From<u64> for Number {
    fn from(u: u64) -> Self {
        Number::Unsigned(u)
    }
}

impl From<Number> for f64 {
    fn from(n: Number) -> Self {
        match n {
            Number::NaN => f64::NAN,
            Number::Infinity(sign) => {
                if sign {
                    f64::INFINITY
                } else {
                    f64::NEG_INFINITY
                }
            }
            Number::Float(f) => f,
            Number::Integer(i) => i as f64,
            Number::Unsigned(u) => u as f64,
        }
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        let a = self.try_as_hcl_number();
        let b = other.try_as_hcl_number();

        if a.is_none() || b.is_none() {
            return false;
        }

        a.unwrap() == b.unwrap()
    }
}

impl Eq for Number {}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let a = self.try_as_hcl_number();
        let b = other.try_as_hcl_number();

        if a.is_none() || b.is_none() {
            return None;
        }

        a.unwrap().partial_cmp(&b.unwrap())
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum NumKind {
    I8,
    I16,
    I32,
    I64,
    I128,
    ISize,
    U8,
    U16,
    U32,
    U64,
    U128,
    USize,
    F32,
    F64,
}

pub trait Builtin: Copy + 'static {
    const KIND: NumKind;
    unsafe fn assume_i8(self) -> i8 {
        panic!("Assuming i8 on non-i8 type")
    }
    unsafe fn assume_i16(self) -> i16 {
        panic!("Assuming i16 on non-i16 type")
    }
    unsafe fn assume_i32(self) -> i32 {
        panic!("Assuming i32 on non-i32 type")
    }
    unsafe fn assume_i64(self) -> i64 {
        panic!("Assuming i64 on non-i64 type")
    }
    unsafe fn assume_i128(self) -> i128 {
        panic!("Assuming i128 on non-i128 type")
    }
    unsafe fn assume_isize(self) -> isize {
        panic!("Assuming isize on non-isize type")
    }
    unsafe fn assume_u8(self) -> u8 {
        panic!("Assuming u8 on non-u8 type")
    }
    unsafe fn assume_u16(self) -> u16 {
        panic!("Assuming u16 on non-u16 type")
    }
    unsafe fn assume_u32(self) -> u32 {
        panic!("Assuming u32 on non-u32 type")
    }
    unsafe fn assume_u64(self) -> u64 {
        panic!("Assuming u64 on non-u64 type")
    }
    unsafe fn assume_u128(self) -> u128 {
        panic!("Assuming u128 on non-u128 type")
    }
    unsafe fn assume_usize(self) -> usize {
        panic!("Assuming usize on non-usize type")
    }
    unsafe fn assume_f32(self) -> f32 {
        panic!("Assuming f32 on non-f32 type")
    }
    unsafe fn assume_f64(self) -> f64 {
        panic!("Assuming f64 on non-f64 type")
    }

    fn as_u64(self) -> Result<u64> {
        Ok(match Self::KIND {
            NumKind::I8 => unsafe {
                let val = self.assume_i8();
                if val.is_negative() {
                    anyhow::bail!("Value is negative");
                }

                val as u64
            },
            NumKind::I16 => unsafe {
                let val = self.assume_i16();
                if val.is_negative() {
                    anyhow::bail!("Value is negative");
                }

                val as u64
            },
            NumKind::I32 => unsafe {
                let val = self.assume_i32();
                if val.is_negative() {
                    anyhow::bail!("Value is negative");
                }

                val as u64
            },
            NumKind::I64 => unsafe {
                let val = self.assume_i64();
                if val.is_negative() {
                    anyhow::bail!("Value is negative");
                }

                val as u64
            },
            NumKind::ISize => unsafe {
                let val = self.assume_isize();

                if val.is_negative() {
                    anyhow::bail!("Value is negative");
                }

                val as u64
            },
            NumKind::I128 => unsafe {
                let val = self.assume_i128();

                if val.is_negative() {
                    anyhow::bail!("Value is negative");
                }

                if val > u64::MAX as i128 {
                    anyhow::bail!("Value is too large for u64");
                }

                val as u64
            },
            NumKind::U8 => unsafe { u64::from(self.assume_u8()) },
            NumKind::U16 => unsafe { u64::from(self.assume_u16()) },
            NumKind::U32 => unsafe { u64::from(self.assume_u32()) },
            NumKind::U64 => unsafe { self.assume_u64() },
            NumKind::USize => unsafe { self.assume_usize() as u64 },
            NumKind::U128 => unsafe {
                let val = self.assume_u128();

                if val > u64::MAX as u128 {
                    anyhow::bail!("Value is too large for u64");
                }

                val as u64
            },
            NumKind::F32 => unsafe {
                let val = self.assume_f32();

                if val.is_infinite() || val.is_nan() {
                    anyhow::bail!("Value is not a finite number");
                }

                val as u64
            },
            NumKind::F64 => unsafe {
                let val = self.assume_f64();

                if val.is_infinite() || val.is_nan() {
                    anyhow::bail!("Value is not a finite number");
                }

                val as u64
            },
        })
    }

    fn as_i64(self) -> Result<i64> {
        Ok(match Self::KIND {
            NumKind::I8 => unsafe { i64::from(self.assume_i8()) },
            NumKind::I16 => unsafe { i64::from(self.assume_i16()) },
            NumKind::I32 => unsafe { i64::from(self.assume_i32()) },
            NumKind::I64 => unsafe { self.assume_i64() },
            NumKind::ISize => unsafe { self.assume_isize() as i64 },
            NumKind::I128 => unsafe {
                let val = self.assume_i128();

                if val > i64::MAX as i128 || val < i64::MIN as i128 {
                    anyhow::bail!("Value is out of range for i64");
                }

                val as i64
            },
            NumKind::U8 => unsafe { i64::from(self.assume_u8()) },
            NumKind::U16 => unsafe { i64::from(self.assume_u16()) },
            NumKind::U32 => unsafe { i64::from(self.assume_u32()) },
            NumKind::U64 => unsafe {
                let val = self.assume_u64();

                if val > i64::MAX as u64 {
                    anyhow::bail!("Value is too large for i64");
                }

                val as i64
            },
            NumKind::USize => unsafe {
                let val = self.assume_usize();

                if val > i64::MAX as usize {
                    anyhow::bail!("Value is too large for i64");
                }

                val as i64
            },
            NumKind::U128 => unsafe {
                let val = self.assume_u128();

                if val > i64::MAX as u128 {
                    anyhow::bail!("Value is too large for i64");
                }

                val as i64
            },
            NumKind::F32 => unsafe {
                let val = self.assume_f32();

                if val.is_infinite() || val.is_nan() {
                    anyhow::bail!("Value is not a finite number");
                }

                val as i64
            },
            NumKind::F64 => unsafe {
                let val = self.assume_f64();

                if val.is_infinite() || val.is_nan() {
                    anyhow::bail!("Value is not a finite number");
                }

                val as i64
            },
        })
    }

    fn as_i128(self) -> Result<i128> {
        Ok(match Self::KIND {
            NumKind::I8 => unsafe { i128::from(self.assume_i8()) },
            NumKind::I16 => unsafe { i128::from(self.assume_i16()) },
            NumKind::I32 => unsafe { i128::from(self.assume_i32()) },
            NumKind::I64 => unsafe { i128::from(self.assume_i64()) },
            NumKind::ISize => unsafe { i128::from(self.assume_isize() as i64) },
            NumKind::I128 => unsafe { self.assume_i128() },
            NumKind::U8 => unsafe { i128::from(self.assume_u8()) },
            NumKind::U16 => unsafe { i128::from(self.assume_u16()) },
            NumKind::U32 => unsafe { i128::from(self.assume_u32()) },
            NumKind::U64 => unsafe { i128::from(self.assume_u64()) },
            NumKind::USize => unsafe { i128::from(self.assume_usize() as i64) },
            NumKind::U128 => unsafe {
                let val = self.assume_u128();

                if val > i64::MAX as u128 {
                    anyhow::bail!("Value is too large for integer");
                }

                i128::from(val as i64)
            },
            NumKind::F32 => unsafe {
                let val = self.assume_f32();

                if val.is_infinite() || val.is_nan() {
                    anyhow::bail!("Value is not a finite number");
                }

                i128::from(val as i64)
            },
            NumKind::F64 => unsafe {
                let val = self.assume_f64();

                if val.is_infinite() || val.is_nan() {
                    anyhow::bail!("Value is not a finite number");
                }

                i128::from(val as i64)
            },
        })
    }
}
impl Builtin for i8 {
    const KIND: NumKind = NumKind::I8;
    unsafe fn assume_i8(self) -> i8 {
        self
    }
}
impl Builtin for i16 {
    const KIND: NumKind = NumKind::I16;
    unsafe fn assume_i16(self) -> i16 {
        self
    }
}
impl Builtin for i32 {
    const KIND: NumKind = NumKind::I32;
    unsafe fn assume_i32(self) -> i32 {
        self
    }
}
impl Builtin for i64 {
    const KIND: NumKind = NumKind::I64;
    unsafe fn assume_i64(self) -> i64 {
        self
    }
}
impl Builtin for i128 {
    const KIND: NumKind = NumKind::I128;
    unsafe fn assume_i128(self) -> i128 {
        self
    }
}
impl Builtin for isize {
    const KIND: NumKind = NumKind::ISize;
    unsafe fn assume_isize(self) -> isize {
        self
    }
}
impl Builtin for u8 {
    const KIND: NumKind = NumKind::U8;
    unsafe fn assume_u8(self) -> u8 {
        self
    }
}
impl Builtin for u16 {
    const KIND: NumKind = NumKind::U16;
    unsafe fn assume_u16(self) -> u16 {
        self
    }
}
impl Builtin for u32 {
    const KIND: NumKind = NumKind::U32;
    unsafe fn assume_u32(self) -> u32 {
        self
    }
}
impl Builtin for u64 {
    const KIND: NumKind = NumKind::U64;
    unsafe fn assume_u64(self) -> u64 {
        self
    }
}
impl Builtin for u128 {
    const KIND: NumKind = NumKind::U128;
    unsafe fn assume_u128(self) -> u128 {
        self
    }
}
impl Builtin for usize {
    const KIND: NumKind = NumKind::USize;
    unsafe fn assume_usize(self) -> usize {
        self
    }
}
impl Builtin for f32 {
    const KIND: NumKind = NumKind::F32;
    unsafe fn assume_f32(self) -> f32 {
        self
    }
}
impl Builtin for f64 {
    const KIND: NumKind = NumKind::F64;
    unsafe fn assume_f64(self) -> f64 {
        self
    }
}
