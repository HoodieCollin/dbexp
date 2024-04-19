use std::ptr;

use anyhow::Result;

use crate::number::{IntSize, NumKind, Number};

#[derive(Debug, Clone, Copy)]
pub enum Integer {
    X8(i8),
    X16(i16),
    X32(i32),
    X64(i64),
}

impl Integer {
    pub fn new(size: IntSize, initial: Option<i128>) -> Result<Self> {
        Ok(match size {
            IntSize::X8 => Self::X8(initial.unwrap_or(0).try_into()?),
            IntSize::X16 => Self::X16(initial.unwrap_or(0).try_into()?),
            IntSize::X32 => Self::X32(initial.unwrap_or(0).try_into()?),
            IntSize::X64 => Self::X64(initial.unwrap_or(0).try_into()?),
        })
    }

    pub fn new_default(size: IntSize) -> Self {
        match size {
            IntSize::X8 => Self::X8(0),
            IntSize::X16 => Self::X16(0),
            IntSize::X32 => Self::X32(0),
            IntSize::X64 => Self::X64(0),
        }
    }

    #[inline(always)]
    pub unsafe fn from_array(data: [u8; 8], size: IntSize) -> Self {
        match size {
            IntSize::X8 => Self::X8(data[0] as i8),
            IntSize::X16 => Self::X16(ptr::read_unaligned(data[..2].as_ptr() as *const _)),
            IntSize::X32 => Self::X32(ptr::read_unaligned(data[..4].as_ptr() as *const _)),
            IntSize::X64 => Self::X64(ptr::read_unaligned(data[..8].as_ptr() as *const _)),
        }
    }

    #[inline(always)]
    pub fn into_array(self) -> ([u8; 8], IntSize) {
        let mut data = [0; 8];
        match self {
            Self::X8(val) => unsafe {
                data.as_mut_ptr()
                    .copy_from_nonoverlapping(&val as *const _ as _, 1);

                (data, IntSize::X8)
            },
            Self::X16(val) => unsafe {
                data.as_mut_ptr()
                    .copy_from_nonoverlapping(&val as *const _ as _, 2);

                (data, IntSize::X16)
            },
            Self::X32(val) => unsafe {
                data.as_mut_ptr()
                    .copy_from_nonoverlapping(&val as *const _ as _, 4);

                (data, IntSize::X32)
            },
            Self::X64(val) => unsafe {
                data.as_mut_ptr()
                    .copy_from_nonoverlapping(&val as *const _ as _, 8);

                (data, IntSize::X64)
            },
        }
    }

    #[inline(always)]
    pub fn try_to_fit(&self, size: IntSize) -> Result<Self> {
        Ok(match size {
            IntSize::X8 => Self::X8(self.as_i8()?),
            IntSize::X16 => Self::X16(self.as_i16()?),
            IntSize::X32 => Self::X32(self.as_i32()?),
            IntSize::X64 => Self::X64(self.as_i64()?),
        })
    }

    #[inline(always)]
    pub fn try_from_number<T: Number>(n: T) -> Result<Self> {
        let normalized = n.as_i128()?;

        let new: Integer;
        let size = IntSize::for_value(normalized);

        match T::KIND {
            NumKind::I8 => unsafe {
                new = Self::X8(n.assume_i8());
            },
            NumKind::I16 => unsafe {
                new = Self::X16(n.assume_i16());
            },
            NumKind::I32 => unsafe {
                new = Self::X32(n.assume_i32());
            },
            NumKind::I64 => unsafe {
                new = Self::X64(n.assume_i64());
            },
            NumKind::ISize => unsafe {
                new = Self::new(size, Some(n.assume_isize() as i128))?;
            },
            NumKind::I128 => unsafe {
                new = Self::new(size, Some(n.assume_i128()))?;
            },
            NumKind::U8 => unsafe {
                new = Self::new(size, Some(n.assume_u8() as i128))?;
            },
            NumKind::U16 => unsafe {
                new = Self::new(size, Some(n.assume_u16() as i128))?;
            },
            NumKind::U32 => unsafe {
                new = Self::new(size, Some(n.assume_u32() as i128))?;
            },
            NumKind::U64 => unsafe {
                new = Self::new(size, Some(n.assume_u64() as i128))?;
            },
            NumKind::USize => unsafe {
                new = Self::new(size, Some(n.assume_usize() as i128))?;
            },
            NumKind::U128 => unsafe {
                new = Self::new(size, Some(n.assume_u128() as i128))?;
            },
            NumKind::F32 => {
                new = Self::new(size, Some(normalized))?;
            }
            NumKind::F64 => {
                new = Self::new(size, Some(normalized))?;
            }
        }

        Ok(new)
    }

    pub fn try_from_str(s: &str) -> Result<Self> {
        Self::try_from_number(s.parse::<i128>()?)
    }

    #[inline(always)]
    pub fn size(&self) -> IntSize {
        match self {
            Self::X8(_) => IntSize::X8,
            Self::X16(_) => IntSize::X16,
            Self::X32(_) => IntSize::X32,
            Self::X64(_) => IntSize::X64,
        }
    }

    #[inline(always)]
    pub fn as_i8(&self) -> Result<i8> {
        Ok(match self {
            Self::X8(val) => *val,
            Self::X16(val) => (*val).try_into()?,
            Self::X32(val) => (*val).try_into()?,
            Self::X64(val) => (*val).try_into()?,
        })
    }

    #[inline(always)]
    pub fn as_i8_mut(&mut self) -> Result<&mut i8> {
        Ok(match self {
            Self::X8(val) => val,
            _ => anyhow::bail!("Integer is not i8"),
        })
    }

    #[inline(always)]
    pub fn as_i16(&self) -> Result<i16> {
        Ok(match self {
            Self::X8(val) => (*val).try_into()?,
            Self::X16(val) => *val,
            Self::X32(val) => (*val).try_into()?,
            Self::X64(val) => (*val).try_into()?,
        })
    }

    #[inline(always)]
    pub fn as_i16_mut(&mut self) -> Result<&mut i16> {
        Ok(match self {
            Self::X16(val) => val,
            _ => anyhow::bail!("Integer is not i16"),
        })
    }

    #[inline(always)]
    pub fn as_i32(&self) -> Result<i32> {
        Ok(match self {
            Self::X8(val) => (*val).try_into()?,
            Self::X16(val) => (*val).try_into()?,
            Self::X32(val) => *val,
            Self::X64(val) => (*val).try_into()?,
        })
    }

    #[inline(always)]
    pub fn as_i32_mut(&mut self) -> Result<&mut i32> {
        Ok(match self {
            Self::X32(val) => val,
            _ => anyhow::bail!("Integer is not i32"),
        })
    }

    #[inline(always)]
    pub fn as_i64(&self) -> Result<i64> {
        Ok(match self {
            Self::X8(val) => (*val).try_into()?,
            Self::X16(val) => (*val).try_into()?,
            Self::X32(val) => (*val).try_into()?,
            Self::X64(val) => *val,
        })
    }

    #[inline(always)]
    pub fn as_i64_mut(&mut self) -> Result<&mut i64> {
        Ok(match self {
            Self::X64(val) => val,
            _ => anyhow::bail!("Integer is not i64"),
        })
    }

    #[inline(always)]
    pub fn as_i128(&self) -> i128 {
        match self {
            Self::X8(val) => i128::from(*val),
            Self::X16(val) => i128::from(*val),
            Self::X32(val) => i128::from(*val),
            Self::X64(val) => i128::from(*val),
        }
    }

    pub fn try_from_slice(bytes: &[u8], size: IntSize) -> Result<Self> {
        if size.byte_count() != bytes.len() {
            anyhow::bail!("Slice length does not match integer size");
        }

        unsafe { Ok(Self::from_slice_unchecked(bytes, size)) }
    }

    pub unsafe fn from_slice_unchecked(bytes: &[u8], size: IntSize) -> Self {
        let mut data = [0; 8];
        data.as_mut_ptr()
            .copy_from_nonoverlapping(bytes.as_ptr() as _, size.byte_count());

        Self::from_array(data, size)
    }

    #[inline(always)]
    pub fn is_zero(&self) -> bool {
        match self {
            Self::X8(val) => *val == 0,
            Self::X16(val) => *val == 0,
            Self::X32(val) => *val == 0,
            Self::X64(val) => *val == 0,
        }
    }

    #[inline(always)]
    pub fn is_negative(&self) -> bool {
        match self {
            Self::X8(val) => *val < 0,
            Self::X16(val) => *val < 0,
            Self::X32(val) => *val < 0,
            Self::X64(val) => *val < 0,
        }
    }
}

impl std::fmt::Display for Integer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::X8(val) => write!(f, "{}", val),
            Self::X16(val) => write!(f, "{}", val),
            Self::X32(val) => write!(f, "{}", val),
            Self::X64(val) => write!(f, "{}", val),
        }
    }
}

impl serde::Serialize for Integer {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::X8(val) => serializer.serialize_i8(*val),
            Self::X16(val) => serializer.serialize_i16(*val),
            Self::X32(val) => serializer.serialize_i32(*val),
            Self::X64(val) => serializer.serialize_i64(*val),
        }
    }
}

impl PartialEq for Integer {
    fn eq(&self, other: &Self) -> bool {
        let a = match *self {
            Self::X8(val) => val as i128,
            Self::X16(val) => val as i128,
            Self::X32(val) => val as i128,
            Self::X64(val) => val as i128,
        };

        let b = match *other {
            Self::X8(val) => val as i128,
            Self::X16(val) => val as i128,
            Self::X32(val) => val as i128,
            Self::X64(val) => val as i128,
        };

        a == b
    }
}

impl Eq for Integer {}

impl std::hash::Hash for Integer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::X8(val) => val.hash(state),
            Self::X16(val) => val.hash(state),
            Self::X32(val) => val.hash(state),
            Self::X64(val) => val.hash(state),
        }
    }
}

impl PartialOrd for Integer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let a = match *self {
            Self::X8(val) => val as i128,
            Self::X16(val) => val as i128,
            Self::X32(val) => val as i128,
            Self::X64(val) => val as i128,
        };

        let b = match *other {
            Self::X8(val) => val as i128,
            Self::X16(val) => val as i128,
            Self::X32(val) => val as i128,
            Self::X64(val) => val as i128,
        };

        a.partial_cmp(&b)
    }
}

impl Ord for Integer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let a = match *self {
            Self::X8(val) => val as i128,
            Self::X16(val) => val as i128,
            Self::X32(val) => val as i128,
            Self::X64(val) => val as i128,
        };

        let b = match *other {
            Self::X8(val) => val as i128,
            Self::X16(val) => val as i128,
            Self::X32(val) => val as i128,
            Self::X64(val) => val as i128,
        };

        a.cmp(&b)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_integer() -> Result<()> {
        let mut integer = Integer::new(IntSize::X8, None)?;
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), true);
        assert_eq!(integer.is_negative(), false);

        *integer.as_i8_mut()? = i8::MAX;
        assert_eq!(integer.as_i128(), i8::MAX as i128);

        let mut integer = Integer::new(IntSize::X16, None)?;
        assert_eq!(integer.size(), IntSize::X16);
        assert_eq!(integer.is_zero(), true);
        assert_eq!(integer.is_negative(), false);

        *integer.as_i16_mut()? = i16::MAX;
        assert_eq!(integer.as_i128(), i16::MAX as i128);

        let mut integer = Integer::new(IntSize::X32, None)?;
        assert_eq!(integer.size(), IntSize::X32);
        assert_eq!(integer.is_zero(), true);
        assert_eq!(integer.is_negative(), false);

        *integer.as_i32_mut()? = i32::MAX;
        assert_eq!(integer.as_i128(), i32::MAX as i128);

        let mut integer = Integer::new(IntSize::X64, None)?;
        assert_eq!(integer.size(), IntSize::X64);
        assert_eq!(integer.is_zero(), true);
        assert_eq!(integer.is_negative(), false);

        *integer.as_i64_mut()? = i64::MAX;
        assert_eq!(integer.as_i128(), i64::MAX as i128);

        let integer = Integer::try_from_number(42isize).unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.as_i128(), 42);

        let integer = Integer::try_from_number(42u8).unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.is_negative(), false);
        assert_eq!(integer.as_i128(), 42);

        let integer = Integer::try_from_number(42u16).unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.is_negative(), false);
        assert_eq!(integer.as_i128(), 42);

        let integer = Integer::try_from_number(42u32).unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.is_negative(), false);
        assert_eq!(integer.as_i128(), 42);

        let integer = Integer::try_from_number(42u64).unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.is_negative(), false);
        assert_eq!(integer.as_i128(), 42);

        let integer = Integer::try_from_number(42u128).unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.as_i128(), 42);

        let integer = Integer::try_from_number(42usize).unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.as_i128(), 42);

        let integer = Integer::try_from_str("42").unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.as_i128(), 42);

        let integer = Integer::try_from_str("-42").unwrap();
        assert_eq!(integer.size(), IntSize::X8);
        assert_eq!(integer.is_zero(), false);
        assert_eq!(integer.is_negative(), true);

        Ok(())
    }
}
