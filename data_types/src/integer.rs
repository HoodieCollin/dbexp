use std::{any::TypeId, mem::MaybeUninit};

use anyhow::Result;

pub trait Number: Copy + 'static {
    fn has_sign(&self) -> bool;
}

impl Number for i8 {
    fn has_sign(&self) -> bool {
        true
    }
}
impl Number for i16 {
    fn has_sign(&self) -> bool {
        true
    }
}
impl Number for i32 {
    fn has_sign(&self) -> bool {
        true
    }
}
impl Number for i64 {
    fn has_sign(&self) -> bool {
        true
    }
}
impl Number for i128 {
    fn has_sign(&self) -> bool {
        true
    }
}
impl Number for isize {
    fn has_sign(&self) -> bool {
        true
    }
}
impl Number for u8 {
    fn has_sign(&self) -> bool {
        false
    }
}
impl Number for u16 {
    fn has_sign(&self) -> bool {
        false
    }
}
impl Number for u32 {
    fn has_sign(&self) -> bool {
        false
    }
}
impl Number for u64 {
    fn has_sign(&self) -> bool {
        false
    }
}
impl Number for u128 {
    fn has_sign(&self) -> bool {
        false
    }
}
impl Number for usize {
    fn has_sign(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IntSize {
    X8,
    X16,
    X32,
    X64,
    X128,
}

impl IntSize {
    #[inline(always)]
    fn byte_count(&self) -> usize {
        match self {
            Self::X8 => 1,
            Self::X16 => 2,
            Self::X32 => 4,
            Self::X64 => 8,
            Self::X128 => 16,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Integer {
    data: [u8; 16],
    size: IntSize,
}

impl Integer {
    pub fn new(size: IntSize) -> Self {
        Self {
            data: unsafe {
                let mut buf = MaybeUninit::<[u8; 16]>::uninit();
                buf.as_mut_ptr().write_bytes(0, size.byte_count());
                buf.assume_init()
            },
            size,
        }
    }

    #[inline(always)]
    pub unsafe fn from_parts(data: [u8; 16], size: IntSize) -> Self {
        Self { data, size }
    }

    #[inline(always)]
    pub fn into_parts(self) -> ([u8; 16], IntSize) {
        (self.data, self.size)
    }

    #[inline(always)]
    pub fn try_to_fit(&self, size: IntSize) -> Result<Self> {
        unsafe {
            match size {
                IntSize::X8 => {
                    let val: i8 = self.into_inner().try_into()?;
                    Ok(Self::from_slice_unchecked(&val.to_ne_bytes(), size))
                }
                IntSize::X16 => {
                    let val: i16 = self.into_inner().try_into()?;
                    Ok(Self::from_slice_unchecked(&val.to_ne_bytes(), size))
                }
                IntSize::X32 => {
                    let val: i32 = self.into_inner().try_into()?;
                    Ok(Self::from_slice_unchecked(&val.to_ne_bytes(), size))
                }
                IntSize::X64 => {
                    let val: i64 = self.into_inner().try_into()?;
                    Ok(Self::from_slice_unchecked(&val.to_ne_bytes(), size))
                }
                IntSize::X128 => {
                    let val: i128 = self.into_inner().try_into()?;
                    Ok(Self::from_slice_unchecked(&val.to_ne_bytes(), size))
                }
            }
        }
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
        }

        let mut new: Integer;

        unsafe {
            if n.has_sign() {
                new = Self::new(match std::mem::size_of::<T>() {
                    1 => IntSize::X8,
                    2 => IntSize::X16,
                    4 => IntSize::X32,
                    8 => IntSize::X64,
                    16 => IntSize::X128,
                    _ => unreachable!(),
                });

                match TypeId::of::<T>() {
                    t if t == TypeId::of::<i8>() => {
                        *new.as_i8_mut_unchecked() = Transmute { n }.i8;
                    }
                    t if t == TypeId::of::<i16>() => {
                        *new.as_i16_mut_unchecked() = Transmute { n }.i16;
                    }
                    t if t == TypeId::of::<i32>() => {
                        *new.as_i32_mut_unchecked() = Transmute { n }.i32;
                    }
                    t if t == TypeId::of::<i64>() => {
                        *new.as_i64_mut_unchecked() = Transmute { n }.i64;
                    }
                    t if t == TypeId::of::<i128>() => {
                        *new.as_i128_mut_unchecked() = Transmute { n }.i128;
                    }
                    t if t == TypeId::of::<isize>() => {
                        *new.as_isize_mut_unchecked() = Transmute { n }.isize;
                    }
                    _ => unreachable!(),
                }
            } else {
                match TypeId::of::<T>() {
                    t if t == TypeId::of::<u8>() => {
                        let val = Transmute { n }.u8;

                        if i8::MAX as u8 > val {
                            new = Self::new(IntSize::X8);
                            *new.as_i8_mut_unchecked() = val as i8;
                        } else {
                            new = Self::new(IntSize::X16);
                            *new.as_i16_mut_unchecked() = val as i16;
                        }
                    }
                    t if t == TypeId::of::<u16>() => {
                        let val = Transmute { n }.u16;

                        if i16::MAX as u16 > val {
                            new = Self::new(IntSize::X16);
                            *new.as_i16_mut_unchecked() = val as i16;
                        } else {
                            new = Self::new(IntSize::X32);
                            *new.as_i32_mut_unchecked() = val as i32;
                        }
                    }
                    t if t == TypeId::of::<u32>() => {
                        let val = Transmute { n }.u32;

                        if i32::MAX as u32 > val {
                            new = Self::new(IntSize::X32);
                            *new.as_i32_mut_unchecked() = val as i32;
                        } else {
                            new = Self::new(IntSize::X64);
                            *new.as_i64_mut_unchecked() = val as i64;
                        }
                    }
                    t if t == TypeId::of::<u64>() => {
                        let val = Transmute { n }.u64;

                        if i64::MAX as u64 > val {
                            new = Self::new(IntSize::X64);
                            *new.as_i64_mut_unchecked() = val as i64;
                        } else {
                            new = Self::new(IntSize::X128);
                            *new.as_i128_mut_unchecked() = val as i128;
                        }
                    }
                    t if t == TypeId::of::<u128>() => {
                        let val = Transmute { n }.u128;

                        if i128::MAX as u128 > val {
                            new = Self::new(IntSize::X128);
                            *new.as_i128_mut_unchecked() = val as i128;
                        } else {
                            anyhow::bail!("Value is too large for integer");
                        }
                    }
                    t if t == TypeId::of::<usize>() => {
                        let val = Transmute { n }.usize;

                        if i128::MAX as usize > val {
                            new = Self::new(IntSize::X128);
                            *new.as_i128_mut_unchecked() = val as i128;
                        } else {
                            new = Self::new(IntSize::X64);
                            *new.as_i64_mut_unchecked() = val as i64;
                        }
                    }
                    _ => unreachable!(),
                }
            }

            Ok(new)
        }
    }

    pub fn try_from_str(s: &str) -> Result<Self> {
        Self::try_from_number(s.parse::<u128>()?)
    }

    #[inline(always)]
    pub fn size(&self) -> IntSize {
        self.size
    }

    #[inline(always)]
    pub unsafe fn as_i8_unchecked(&self) -> i8 {
        *self.data.as_ptr() as i8
    }

    #[inline(always)]
    pub unsafe fn as_i8_mut_unchecked(&mut self) -> &mut i8 {
        &mut *(self.data.as_mut_ptr() as *mut i8)
    }

    #[inline(always)]
    pub unsafe fn as_i16_unchecked(&self) -> i16 {
        *(self.data.as_ptr() as *const i16)
    }

    #[inline(always)]
    pub unsafe fn as_i16_mut_unchecked(&mut self) -> &mut i16 {
        &mut *(self.data.as_mut_ptr() as *mut i16)
    }

    #[inline(always)]
    pub unsafe fn as_i32_unchecked(&self) -> i32 {
        *(self.data.as_ptr() as *const i32)
    }

    #[inline(always)]
    pub unsafe fn as_i32_mut_unchecked(&mut self) -> &mut i32 {
        &mut *(self.data.as_mut_ptr() as *mut i32)
    }

    #[inline(always)]
    pub unsafe fn as_i64_unchecked(&self) -> i64 {
        *(self.data.as_ptr() as *const i64)
    }

    #[inline(always)]
    pub unsafe fn as_i64_mut_unchecked(&mut self) -> &mut i64 {
        &mut *(self.data.as_mut_ptr() as *mut i64)
    }

    #[inline(always)]
    pub unsafe fn as_i128_unchecked(&self) -> i128 {
        *(self.data.as_ptr() as *const i128)
    }

    #[inline(always)]
    pub unsafe fn as_i128_mut_unchecked(&mut self) -> &mut i128 {
        &mut *(self.data.as_mut_ptr() as *mut i128)
    }

    #[inline(always)]
    pub unsafe fn as_isize_mut_unchecked(&mut self) -> &mut isize {
        &mut *(self.data.as_mut_ptr() as *mut isize)
    }

    #[inline(always)]
    pub unsafe fn as_usize_mut_unchecked(&mut self) -> &mut usize {
        &mut *(self.data.as_mut_ptr() as *mut usize)
    }

    pub fn into_inner(self) -> i128 {
        match self.size {
            IntSize::X8 => unsafe { self.as_i8_unchecked() as i128 },
            IntSize::X16 => unsafe { self.as_i16_unchecked() as i128 },
            IntSize::X32 => unsafe { self.as_i32_unchecked() as i128 },
            IntSize::X64 => unsafe { self.as_i64_unchecked() as i128 },
            IntSize::X128 => unsafe { self.as_i128_unchecked() },
        }
    }

    pub fn from_slice(bytes: &[u8], size: IntSize) -> Result<Self> {
        if size.byte_count() != bytes.len() {
            anyhow::bail!("Slice length does not match integer size");
        }

        unsafe { Ok(Self::from_slice_unchecked(bytes, size)) }
    }

    pub unsafe fn from_slice_unchecked(bytes: &[u8], size: IntSize) -> Self {
        Self {
            data: {
                let mut buf = MaybeUninit::<[u8; 16]>::uninit();
                buf.as_mut_ptr()
                    .copy_from_nonoverlapping(bytes.as_ptr() as _, size.byte_count());

                buf.assume_init()
            },
            size,
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.size.byte_count()) }
    }

    #[inline(always)]
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_mut_ptr(), self.size.byte_count()) }
    }

    #[inline(always)]
    pub fn is_zero(&self) -> bool {
        match self.size {
            IntSize::X8 => unsafe { self.as_i8_unchecked() == 0 },
            IntSize::X16 => unsafe { self.as_i16_unchecked() == 0 },
            IntSize::X32 => unsafe { self.as_i32_unchecked() == 0 },
            IntSize::X64 => unsafe { self.as_i64_unchecked() == 0 },
            IntSize::X128 => unsafe { self.as_i128_unchecked() == 0 },
        }
    }

    #[inline(always)]
    pub fn is_negative(&self) -> bool {
        match self.size {
            IntSize::X8 => unsafe { self.as_i8_unchecked() < 0 },
            IntSize::X16 => unsafe { self.as_i16_unchecked() < 0 },
            IntSize::X32 => unsafe { self.as_i32_unchecked() < 0 },
            IntSize::X64 => unsafe { self.as_i64_unchecked() < 0 },
            IntSize::X128 => unsafe { self.as_i128_unchecked() < 0 },
        }
    }
}

impl std::fmt::Debug for Integer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            match self.size {
                IntSize::X8 => f.debug_tuple("I8").field(&self.as_i8_unchecked()).finish(),
                IntSize::X16 => f
                    .debug_tuple("I16")
                    .field(&self.as_i16_unchecked())
                    .finish(),
                IntSize::X32 => f
                    .debug_tuple("I32")
                    .field(&self.as_i32_unchecked())
                    .finish(),
                IntSize::X64 => f
                    .debug_tuple("I64")
                    .field(&self.as_i64_unchecked())
                    .finish(),
                IntSize::X128 => f
                    .debug_tuple("I128")
                    .field(&self.as_i128_unchecked())
                    .finish(),
            }
        }
    }
}

impl std::fmt::Display for Integer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe {
            match self.size {
                IntSize::X8 => write!(f, "{}", self.as_i8_unchecked()),
                IntSize::X16 => write!(f, "{}", self.as_i16_unchecked()),
                IntSize::X32 => write!(f, "{}", self.as_i32_unchecked()),
                IntSize::X64 => write!(f, "{}", self.as_i64_unchecked()),
                IntSize::X128 => write!(f, "{}", self.as_i128_unchecked()),
            }
        }
    }
}

impl serde::Serialize for Integer {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        unsafe {
            match self.size {
                IntSize::X8 => serializer.serialize_i8(self.as_i8_unchecked()),
                IntSize::X16 => serializer.serialize_i16(self.as_i16_unchecked()),
                IntSize::X32 => serializer.serialize_i32(self.as_i32_unchecked()),
                IntSize::X64 => serializer.serialize_i64(self.as_i64_unchecked()),
                IntSize::X128 => serializer.serialize_i128(self.as_i128_unchecked()),
            }
        }
    }
}

impl PartialEq for Integer {
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            match self.size {
                IntSize::X8 => match other.size {
                    IntSize::X8 => self.as_i8_unchecked() == other.as_i8_unchecked(),
                    IntSize::X16 => self.as_i8_unchecked() as i16 == other.as_i16_unchecked(),
                    IntSize::X32 => self.as_i8_unchecked() as i32 == other.as_i32_unchecked(),
                    IntSize::X64 => self.as_i8_unchecked() as i64 == other.as_i64_unchecked(),
                    IntSize::X128 => self.as_i8_unchecked() as i128 == other.as_i128_unchecked(),
                },
                IntSize::X16 => match other.size {
                    IntSize::X8 => self.as_i16_unchecked() == other.as_i8_unchecked() as i16,
                    IntSize::X16 => self.as_i16_unchecked() == other.as_i16_unchecked(),
                    IntSize::X32 => self.as_i16_unchecked() as i32 == other.as_i32_unchecked(),
                    IntSize::X64 => self.as_i16_unchecked() as i64 == other.as_i64_unchecked(),
                    IntSize::X128 => self.as_i16_unchecked() as i128 == other.as_i128_unchecked(),
                },
                IntSize::X32 => match other.size {
                    IntSize::X8 => self.as_i32_unchecked() == other.as_i8_unchecked() as i32,
                    IntSize::X16 => self.as_i32_unchecked() == other.as_i16_unchecked() as i32,
                    IntSize::X32 => self.as_i32_unchecked() == other.as_i32_unchecked(),
                    IntSize::X64 => self.as_i32_unchecked() as i64 == other.as_i64_unchecked(),
                    IntSize::X128 => self.as_i32_unchecked() as i128 == other.as_i128_unchecked(),
                },
                IntSize::X64 => match other.size {
                    IntSize::X8 => self.as_i64_unchecked() == other.as_i8_unchecked() as i64,
                    IntSize::X16 => self.as_i64_unchecked() == other.as_i16_unchecked() as i64,
                    IntSize::X32 => self.as_i64_unchecked() == other.as_i32_unchecked() as i64,
                    IntSize::X64 => self.as_i64_unchecked() == other.as_i64_unchecked(),
                    IntSize::X128 => self.as_i64_unchecked() as i128 == other.as_i128_unchecked(),
                },
                IntSize::X128 => match other.size {
                    IntSize::X8 => self.as_i128_unchecked() == other.as_i8_unchecked() as i128,
                    IntSize::X16 => self.as_i128_unchecked() == other.as_i16_unchecked() as i128,
                    IntSize::X32 => self.as_i128_unchecked() == other.as_i32_unchecked() as i128,
                    IntSize::X64 => self.as_i128_unchecked() == other.as_i64_unchecked() as i128,
                    IntSize::X128 => self.as_i128_unchecked() == other.as_i128_unchecked(),
                },
            }
        }
    }
}

impl Eq for Integer {}

impl std::hash::Hash for Integer {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        unsafe {
            match self.size {
                IntSize::X8 => self.as_i8_unchecked().hash(state),
                IntSize::X16 => self.as_i16_unchecked().hash(state),
                IntSize::X32 => self.as_i32_unchecked().hash(state),
                IntSize::X64 => self.as_i64_unchecked().hash(state),
                IntSize::X128 => self.as_i128_unchecked().hash(state),
            }
        }
    }
}

impl PartialOrd for Integer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        unsafe {
            match self.size {
                IntSize::X8 => match other.size {
                    IntSize::X8 => (self.as_i8_unchecked()).partial_cmp(&other.as_i8_unchecked()),
                    IntSize::X16 => {
                        (self.as_i8_unchecked() as i16).partial_cmp(&other.as_i16_unchecked())
                    }
                    IntSize::X32 => {
                        (self.as_i8_unchecked() as i32).partial_cmp(&other.as_i32_unchecked())
                    }
                    IntSize::X64 => {
                        (self.as_i8_unchecked() as i64).partial_cmp(&other.as_i64_unchecked())
                    }
                    IntSize::X128 => {
                        (self.as_i8_unchecked() as i128).partial_cmp(&other.as_i128_unchecked())
                    }
                },
                IntSize::X16 => match other.size {
                    IntSize::X8 => {
                        (self.as_i16_unchecked()).partial_cmp(&(other.as_i8_unchecked() as i16))
                    }
                    IntSize::X16 => {
                        (self.as_i16_unchecked()).partial_cmp(&other.as_i16_unchecked())
                    }
                    IntSize::X32 => {
                        (self.as_i16_unchecked() as i32).partial_cmp(&other.as_i32_unchecked())
                    }
                    IntSize::X64 => {
                        (self.as_i16_unchecked() as i64).partial_cmp(&other.as_i64_unchecked())
                    }
                    IntSize::X128 => {
                        (self.as_i16_unchecked() as i128).partial_cmp(&other.as_i128_unchecked())
                    }
                },
                IntSize::X32 => {
                    match other.size {
                        IntSize::X8 => {
                            (self.as_i32_unchecked()).partial_cmp(&(other.as_i8_unchecked() as i32))
                        }
                        IntSize::X16 => (self.as_i32_unchecked())
                            .partial_cmp(&(other.as_i16_unchecked() as i32)),
                        IntSize::X32 => {
                            (self.as_i32_unchecked()).partial_cmp(&other.as_i32_unchecked())
                        }
                        IntSize::X64 => {
                            (self.as_i32_unchecked() as i64).partial_cmp(&other.as_i64_unchecked())
                        }
                        IntSize::X128 => (self.as_i32_unchecked() as i128)
                            .partial_cmp(&other.as_i128_unchecked()),
                    }
                }
                IntSize::X64 => {
                    match other.size {
                        IntSize::X8 => {
                            (self.as_i64_unchecked()).partial_cmp(&(other.as_i8_unchecked() as i64))
                        }
                        IntSize::X16 => (self.as_i64_unchecked())
                            .partial_cmp(&(other.as_i16_unchecked() as i64)),
                        IntSize::X32 => (self.as_i64_unchecked())
                            .partial_cmp(&(other.as_i32_unchecked() as i64)),
                        IntSize::X64 => {
                            (self.as_i64_unchecked()).partial_cmp(&other.as_i64_unchecked())
                        }
                        IntSize::X128 => (self.as_i64_unchecked() as i128)
                            .partial_cmp(&other.as_i128_unchecked()),
                    }
                }
                IntSize::X128 => {
                    match other.size {
                        IntSize::X8 => (self.as_i128_unchecked())
                            .partial_cmp(&(other.as_i8_unchecked() as i128)),
                        IntSize::X16 => (self.as_i128_unchecked())
                            .partial_cmp(&(other.as_i16_unchecked() as i128)),
                        IntSize::X32 => (self.as_i128_unchecked())
                            .partial_cmp(&(other.as_i32_unchecked() as i128)),
                        IntSize::X64 => (self.as_i128_unchecked())
                            .partial_cmp(&(other.as_i64_unchecked() as i128)),
                        IntSize::X128 => {
                            (self.as_i128_unchecked()).partial_cmp(&other.as_i128_unchecked())
                        }
                    }
                }
            }
        }
    }
}

impl Ord for Integer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        unsafe {
            match self.size {
                IntSize::X8 => match other.size {
                    IntSize::X8 => (self.as_i8_unchecked()).cmp(&other.as_i8_unchecked()),
                    IntSize::X16 => (self.as_i8_unchecked() as i16).cmp(&other.as_i16_unchecked()),
                    IntSize::X32 => (self.as_i8_unchecked() as i32).cmp(&other.as_i32_unchecked()),
                    IntSize::X64 => (self.as_i8_unchecked() as i64).cmp(&other.as_i64_unchecked()),
                    IntSize::X128 => {
                        (self.as_i8_unchecked() as i128).cmp(&other.as_i128_unchecked())
                    }
                },
                IntSize::X16 => match other.size {
                    IntSize::X8 => (self.as_i16_unchecked()).cmp(&(other.as_i8_unchecked() as i16)),
                    IntSize::X16 => (self.as_i16_unchecked()).cmp(&other.as_i16_unchecked()),
                    IntSize::X32 => (self.as_i16_unchecked() as i32).cmp(&other.as_i32_unchecked()),
                    IntSize::X64 => (self.as_i16_unchecked() as i64).cmp(&other.as_i64_unchecked()),
                    IntSize::X128 => {
                        (self.as_i16_unchecked() as i128).cmp(&other.as_i128_unchecked())
                    }
                },
                IntSize::X32 => match other.size {
                    IntSize::X8 => (self.as_i32_unchecked()).cmp(&(other.as_i8_unchecked() as i32)),
                    IntSize::X16 => {
                        (self.as_i32_unchecked()).cmp(&(other.as_i16_unchecked() as i32))
                    }
                    IntSize::X32 => (self.as_i32_unchecked()).cmp(&other.as_i32_unchecked()),
                    IntSize::X64 => (self.as_i32_unchecked() as i64).cmp(&other.as_i64_unchecked()),
                    IntSize::X128 => {
                        (self.as_i32_unchecked() as i128).cmp(&other.as_i128_unchecked())
                    }
                },
                IntSize::X64 => match other.size {
                    IntSize::X8 => (self.as_i64_unchecked()).cmp(&(other.as_i8_unchecked() as i64)),
                    IntSize::X16 => {
                        (self.as_i64_unchecked()).cmp(&(other.as_i16_unchecked() as i64))
                    }
                    IntSize::X32 => {
                        (self.as_i64_unchecked()).cmp(&(other.as_i32_unchecked() as i64))
                    }
                    IntSize::X64 => (self.as_i64_unchecked()).cmp(&other.as_i64_unchecked()),
                    IntSize::X128 => {
                        (self.as_i64_unchecked() as i128).cmp(&other.as_i128_unchecked())
                    }
                },
                IntSize::X128 => match other.size {
                    IntSize::X8 => {
                        (self.as_i128_unchecked()).cmp(&(other.as_i8_unchecked() as i128))
                    }
                    IntSize::X16 => {
                        (self.as_i128_unchecked()).cmp(&(other.as_i16_unchecked() as i128))
                    }
                    IntSize::X32 => {
                        (self.as_i128_unchecked()).cmp(&(other.as_i32_unchecked() as i128))
                    }
                    IntSize::X64 => {
                        (self.as_i128_unchecked()).cmp(&(other.as_i64_unchecked() as i128))
                    }
                    IntSize::X128 => (self.as_i128_unchecked()).cmp(&other.as_i128_unchecked()),
                },
            }
        }
    }
}
