use std::{
    any::TypeId,
    io::{Cursor, Read, Write},
    mem::{size_of, ManuallyDrop},
};

use crate::object_ids::ThinRecordId;
use anyhow::Result;
use data_types::oid::{O16, O32, O64};

use crate::object_ids::TableId;

pub trait IntoBytes: Sized {
    const BYTE_COUNT: usize = size_of::<Self>();

    fn encode_bytes(&self, encoder: &mut ByteEncoder<'_>) -> Result<()>;

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
    pub fn encode<T: 'static>(&mut self, value: T) -> Result<()> {
        union Transmuter<A, B> {
            from: ManuallyDrop<A>,
            to: ManuallyDrop<B>,
        }

        impl<A, B> Transmuter<A, B> {
            pub unsafe fn new(from: A) -> Self {
                Self {
                    from: ManuallyDrop::new(from),
                }
            }

            pub unsafe fn convert(self) -> B {
                ManuallyDrop::into_inner(unsafe { self.to })
            }
        }

        unsafe {
            match TypeId::of::<T>() {
                t if t == TypeId::of::<usize>() => {
                    let x = Transmuter::<_, usize>::new(value);
                    self.cursor.write_all(&x.convert().to_ne_bytes())?;
                }
                t if t == TypeId::of::<O16>() => {
                    let x = Transmuter::<_, O16>::new(value);
                    self.cursor.write_all(&x.convert().into_array())?;
                }
                t if t == TypeId::of::<O32>() => {
                    let x = Transmuter::<_, O32>::new(value);
                    self.cursor.write_all(&x.convert().into_array())?;
                }
                t if t == TypeId::of::<O64>() => {
                    let x = Transmuter::<_, O64>::new(value);
                    self.cursor.write_all(&x.convert().into_array())?;
                }
                t if t == TypeId::of::<TableId>() => {
                    let x = Transmuter::<_, TableId>::new(value);
                    self.cursor.write_all(&x.convert().into_array())?;
                }
                t if t == TypeId::of::<ThinRecordId>() => {
                    let x = Transmuter::<_, ThinRecordId>::new(value);
                    self.cursor.write_all(&x.convert().into_array())?;
                }
                _ => anyhow::bail!("unsupported type"),
            }
        }
        Ok(())
    }

    pub fn encode_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.cursor.write_all(bytes)?;
        Ok(())
    }
}

pub trait FromBytes: IntoBytes {
    fn decode_bytes(this: &mut Self, decoder: &mut ByteDecoder<'_>) -> Result<()>;

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

    pub fn decode<T: 'static>(&mut self, dst: &mut T) -> Result<()> {
        unsafe {
            match TypeId::of::<T>() {
                t if t == TypeId::of::<usize>() => {
                    let mut buf = [0u8; size_of::<usize>()];
                    self.cursor.read_exact(&mut buf)?;
                    *(dst as *mut _ as *mut usize) = usize::from_ne_bytes(buf);
                }
                t if t == TypeId::of::<O16>() => {
                    let mut buf = [0u8; size_of::<O16>()];
                    self.cursor.read_exact(&mut buf)?;
                    *(dst as *mut _ as *mut O16) = O16::from_array(buf);
                }
                t if t == TypeId::of::<O32>() => {
                    let mut buf = [0u8; size_of::<O32>()];
                    self.cursor.read_exact(&mut buf)?;
                    *(dst as *mut _ as *mut O32) = O32::from_array(buf);
                }
                t if t == TypeId::of::<O64>() => {
                    let mut buf = [0u8; size_of::<O64>()];
                    self.cursor.read_exact(&mut buf)?;
                    *(dst as *mut _ as *mut O64) = O64::from_array(buf);
                }
                t if t == TypeId::of::<TableId>() => {
                    let mut buf = [0u8; size_of::<TableId>()];
                    self.cursor.read_exact(&mut buf)?;
                    *(dst as *mut _ as *mut TableId) = TableId::from_array(buf);
                }
                t if t == TypeId::of::<ThinRecordId>() => {
                    let mut buf = [0u8; size_of::<ThinRecordId>()];
                    self.cursor.read_exact(&mut buf)?;
                    *(dst as *mut _ as *mut ThinRecordId) = ThinRecordId::from_array(buf);
                }
                _ => anyhow::bail!("unsupported type"),
            }
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
