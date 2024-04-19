use std::mem::size_of;

use anyhow::Result;
use serde::{Deserialize, Serialize};

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

pub trait Number: Copy + 'static {
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
impl Number for i8 {
    const KIND: NumKind = NumKind::I8;
    unsafe fn assume_i8(self) -> i8 {
        self
    }
}
impl Number for i16 {
    const KIND: NumKind = NumKind::I16;
    unsafe fn assume_i16(self) -> i16 {
        self
    }
}
impl Number for i32 {
    const KIND: NumKind = NumKind::I32;
    unsafe fn assume_i32(self) -> i32 {
        self
    }
}
impl Number for i64 {
    const KIND: NumKind = NumKind::I64;
    unsafe fn assume_i64(self) -> i64 {
        self
    }
}
impl Number for i128 {
    const KIND: NumKind = NumKind::I128;
    unsafe fn assume_i128(self) -> i128 {
        self
    }
}
impl Number for isize {
    const KIND: NumKind = NumKind::ISize;
    unsafe fn assume_isize(self) -> isize {
        self
    }
}
impl Number for u8 {
    const KIND: NumKind = NumKind::U8;
    unsafe fn assume_u8(self) -> u8 {
        self
    }
}
impl Number for u16 {
    const KIND: NumKind = NumKind::U16;
    unsafe fn assume_u16(self) -> u16 {
        self
    }
}
impl Number for u32 {
    const KIND: NumKind = NumKind::U32;
    unsafe fn assume_u32(self) -> u32 {
        self
    }
}
impl Number for u64 {
    const KIND: NumKind = NumKind::U64;
    unsafe fn assume_u64(self) -> u64 {
        self
    }
}
impl Number for u128 {
    const KIND: NumKind = NumKind::U128;
    unsafe fn assume_u128(self) -> u128 {
        self
    }
}
impl Number for usize {
    const KIND: NumKind = NumKind::USize;
    unsafe fn assume_usize(self) -> usize {
        self
    }
}
impl Number for f32 {
    const KIND: NumKind = NumKind::F32;
    unsafe fn assume_f32(self) -> f32 {
        self
    }
}
impl Number for f64 {
    const KIND: NumKind = NumKind::F64;
    unsafe fn assume_f64(self) -> f64 {
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum IntSize {
    X8,
    X16,
    X32,
    X64,
}

impl IntSize {
    pub fn ptr_size() -> Self {
        match size_of::<usize>() {
            4 => Self::X32,
            8 => Self::X64,
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub fn byte_count(&self) -> usize {
        match self {
            Self::X8 => 1,
            Self::X16 => 2,
            Self::X32 => 4,
            Self::X64 => 8,
        }
    }

    #[inline(always)]
    pub fn for_number<T: Number>() -> Self {
        match size_of::<T>() {
            1 => Self::X8,
            2 => Self::X16,
            4 => Self::X32,
            // all u128 and i128 values are limited to i64::MAX
            8 | 16 => Self::X64,
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    pub fn for_value(value: i128) -> Self {
        if value >= i8::MIN as i128 && value <= i8::MAX as i128 {
            Self::X8
        } else if value >= i16::MIN as i128 && value <= i16::MAX as i128 {
            Self::X16
        } else if value >= i32::MIN as i128 && value <= i32::MAX as i128 {
            Self::X32
        } else {
            Self::X64
        }
    }

    #[inline(always)]
    pub fn size_up(self) -> Result<Self> {
        Ok(match self {
            Self::X8 => Self::X16,
            Self::X16 => Self::X32,
            Self::X32 => Self::X64,
            Self::X64 => anyhow::bail!("Integer size is already at maximum"),
        })
    }
}
