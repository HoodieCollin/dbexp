use std::{
    io::{Cursor, Read, Write},
    mem::size_of,
};

use anyhow::Result;

pub trait AccessBytes {
    fn access_bytes<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>;

    fn access_bytes_mut<F, R>(&mut self, f: F) -> Result<Option<R>>
    where
        Self: Sized,
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static;
}

impl AccessBytes for [u8] {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        f(self)
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        Ok(Some(f(self)?))
    }
}

impl AccessBytes for Option<&mut [u8]> {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        if let Some(bytes) = self {
            f(bytes)
        } else {
            Ok(())
        }
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        if let Some(bytes) = self {
            Ok(Some(f(bytes)?))
        } else {
            Ok(None)
        }
    }
}

impl AccessBytes for Vec<u8> {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        f(self)
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        Ok(Some(f(self)?))
    }
}

impl AccessBytes for Option<Vec<u8>> {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        if let Some(bytes) = self {
            f(bytes)
        } else {
            Ok(())
        }
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        if let Some(bytes) = self {
            Ok(Some(f(bytes)?))
        } else {
            Ok(None)
        }
    }
}

macro_rules! impl_access_bytes_for_integers {
    ($($n:ty),+) => {
        $(
            impl AccessBytes for $n {
                fn access_bytes<F>(&self, mut f: F) -> Result<()>
                where
                    F: FnMut(&[u8]) -> Result<()>,
                {
                    f(&self.to_ne_bytes())
                }

                fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
                where
                    F: FnMut(&mut [u8]) -> Result<R>,
                    R: 'static,
                {
                    let mut bytes = self.to_ne_bytes();
                    Ok(Some(f(&mut bytes)?))
                }
            }

            impl AccessBytes for Option<$n> {
                fn access_bytes<F>(&self, mut f: F) -> Result<()>
                where
                    F: FnMut(&[u8]) -> Result<()>,
                {
                    if let Some(val) = self {
                        f(&val.to_ne_bytes())
                    } else {
                        Ok(())
                    }
                }

                fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
                where
                    F: FnMut(&mut [u8]) -> Result<R>,
                    R: 'static,
                {
                    if let Some(val) = self {
                        let mut bytes = val.to_ne_bytes();
                        Ok(Some(f(&mut bytes)?))
                    } else {
                        Ok(None)
                    }
                }
            }
        )+
    };

}

impl_access_bytes_for_integers! {
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
}

macro_rules! impl_access_bytes_for_non_zero_integers {
    ($($n:ty),+) => {
        $(
            impl AccessBytes for $n {
                fn access_bytes<F>(&self, mut f: F) -> Result<()>
                where
                    F: FnMut(&[u8]) -> Result<()>,
                {
                    f(&self.get().to_ne_bytes())
                }

                fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
                where
                    F: FnMut(&mut [u8]) -> Result<R>,
                    R: 'static,
                {
                    let mut bytes = self.get().to_ne_bytes();
                    Ok(Some(f(&mut bytes[..])?))
                }
            }

            impl AccessBytes for Option<$n> {
                fn access_bytes<F>(&self, mut f: F) -> Result<()>
                where
                    F: FnMut(&[u8]) -> Result<()>,
                {
                    if let Some(val) = self {
                        f(&val.get().to_ne_bytes())
                    } else {
                        Ok(())
                    }
                }

                fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
                where
                    F: FnMut(&mut [u8]) -> Result<R>,
                    R: 'static,
                {
                    if let Some(val) = self {
                        let mut bytes = val.get().to_ne_bytes();
                        Ok(Some(f(&mut bytes)?))
                    } else {
                        Ok(None)
                    }
                }
            }
        )+
    };

}

impl_access_bytes_for_non_zero_integers! {
    std::num::NonZeroU8, std::num::NonZeroU16, std::num::NonZeroU32, std::num::NonZeroU64, std::num::NonZeroU128, std::num::NonZeroUsize,
    std::num::NonZeroI8, std::num::NonZeroI16, std::num::NonZeroI32, std::num::NonZeroI64, std::num::NonZeroI128, std::num::NonZeroIsize
}

impl AccessBytes for bool {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        f(&[*self as u8])
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        let mut bytes = [*self as u8];
        Ok(Some(f(&mut bytes[..])?))
    }
}

impl AccessBytes for f32 {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        f(&self.to_bits().to_ne_bytes())
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        let mut bytes = self.to_bits().to_ne_bytes();
        Ok(Some(f(&mut bytes[..])?))
    }
}

impl AccessBytes for f64 {
    fn access_bytes<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        f(&self.to_bits().to_ne_bytes())
    }

    fn access_bytes_mut<F, R>(&mut self, mut f: F) -> Result<Option<R>>
    where
        F: FnMut(&mut [u8]) -> Result<R>,
        R: 'static,
    {
        let mut bytes = self.to_bits().to_ne_bytes();
        Ok(Some(f(&mut bytes[..])?))
    }
}

pub trait IntoBytes: Sized {
    const BYTE_COUNT: usize = size_of::<Self>();

    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()>;

    fn into_bytes(&self) -> Result<[u8; Self::BYTE_COUNT]> {
        let mut bytes = [0u8; Self::BYTE_COUNT];
        let mut encoder = ByteEncoder {
            cursor: Cursor::new(&mut bytes),
        };
        self.encode_bytes(&mut encoder)?;
        Ok(bytes)
    }

    fn into_vec(&self) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; Self::BYTE_COUNT];
        let mut encoder = ByteEncoder {
            cursor: Cursor::new(&mut bytes),
        };
        self.encode_bytes(&mut encoder)?;
        Ok(bytes)
    }
}

pub struct ByteEncoder<'a> {
    cursor: Cursor<&'a mut [u8]>,
}

impl ByteEncoder<'_> {
    pub fn encode<T: 'static + AccessBytes>(&mut self, value: T) -> Result<()> {
        value.access_bytes(|bytes| Ok(self.cursor.write_all(bytes)?))
    }

    pub fn encode_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.cursor.write_all(bytes)?;
        Ok(())
    }
}

pub trait FromBytes: IntoBytes {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()>;

    fn from_bytes(bytes: &[u8]) -> Result<Self>
    where
        Self: Default,
    {
        let mut this = Self::default();
        let mut decoder = ByteDecoder::new(bytes);
        Self::decode_bytes(&mut this, &mut decoder)?;
        Ok(this)
    }

    fn init_from_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let mut decoder = ByteDecoder::new(bytes);
        Self::decode_bytes(self, &mut decoder)?;
        Ok(())
    }
}

pub trait ScalarFromBytes: Sized {
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
}

impl ScalarFromBytes for u8 {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bytes[0])
    }
}

impl ScalarFromBytes for bool {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bytes[0] != 0)
    }
}

impl ScalarFromBytes for f32 {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self::from_bits(u32::from_ne_bytes(bytes.try_into()?)))
    }
}

impl ScalarFromBytes for f64 {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self::from_bits(u64::from_ne_bytes(bytes.try_into()?)))
    }
}

macro_rules! impl_scalar_from_bytes_for_integers {
    ($($n:ty),+) => {
        $(
            impl ScalarFromBytes for $n {
                fn from_bytes(bytes: &[u8]) -> Result<Self> {
                    Ok(Self::from_ne_bytes(bytes.try_into()?))
                }
            }
        )+
    };
}

impl_scalar_from_bytes_for_integers! {
    u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
}

macro_rules! impl_scalar_from_bytes_for_non_zero_integers {
    ($([$base:ty => $n:ty]),+) => {
        $(
            impl ScalarFromBytes for $n {
                fn from_bytes(bytes: &[u8]) -> Result<Self> {
                    let base = <$base>::from_ne_bytes(bytes.try_into()?);

                    if base == 0 {
                        anyhow::bail!("cannot be zero")
                    } else {
                        Ok(unsafe { Self::new_unchecked(base) })
                    }
                }
            }

            impl ScalarFromBytes for Option<$n> {
                fn from_bytes(bytes: &[u8]) -> Result<Self> {
                    let base = <$base>::from_ne_bytes(bytes.try_into()?);

                    if base == 0 {
                        Ok(None)
                    } else {
                        Ok(Some(unsafe { <$n>::new_unchecked(base) }))
                    }
                }
            }
        )+
    };
}

impl_scalar_from_bytes_for_non_zero_integers! {
    [u8 => std::num::NonZeroU8], [u16 => std::num::NonZeroU16], [u32 => std::num::NonZeroU32], [u64 => std::num::NonZeroU64], [u128 => std::num::NonZeroU128], [usize => std::num::NonZeroUsize],
    [i8 => std::num::NonZeroI8], [i16 => std::num::NonZeroI16], [i32 => std::num::NonZeroI32], [i64 => std::num::NonZeroI64], [i128 => std::num::NonZeroI128], [isize => std::num::NonZeroIsize]
}

pub struct ByteDecoder<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> ByteDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(bytes),
        }
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.cursor.read_exact(buf)?;
        Ok(())
    }

    pub fn decode<T: 'static + AccessBytes + ScalarFromBytes>(
        &mut self,
        dst: &mut T,
    ) -> Result<()> {
        if let Some(val) = dst.access_bytes_mut(|bytes| {
            self.cursor.read_exact(bytes)?;
            Ok(T::from_bytes(bytes)?)
        })? {
            *dst = val;
        }

        Ok(())
    }

    pub fn delegate<T: 'static + FromBytes>(&mut self, dst: &mut T) -> Result<()> {
        let mut buf = vec![0u8; T::BYTE_COUNT];
        self.cursor.read_exact(&mut buf)?;
        <T as FromBytes>::init_from_bytes(dst, &buf)?;
        Ok(())
    }
}

#[macro_export]
macro_rules! impl_access_bytes_for_into_bytes_type {
    ($ty:ty) => {
        impl $crate::byte_encoding::AccessBytes for $ty {
            fn access_bytes<F>(&self, mut f: F) -> anyhow::Result<()>
            where
                F: FnMut(&[u8]) -> Result<()>,
            {
                let bytes = self.into_bytes()?;
                f(&bytes)
            }

            fn access_bytes_mut<F, R>(&mut self, mut f: F) -> anyhow::Result<Option<R>>
            where
                F: FnMut(&mut [u8]) -> Result<R>,
                R: 'static,
            {
                let mut bytes = self.into_bytes()?;
                Ok(Some(f(&mut bytes)?))
            }
        }
    };
}
