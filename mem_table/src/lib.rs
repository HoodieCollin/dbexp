#![feature(lazy_cell)]
#![feature(allocator_api)]
#![feature(os_str_display)]
#![feature(generic_const_exprs)]

use std::{
    any::TypeId,
    ffi::os_str,
    io::{Cursor, Read, Write},
    mem::{size_of, ManuallyDrop},
    os::unix::ffi::OsStrExt,
    path::Path,
};

use anyhow::Result;
use data_types::oid::{O16, O32, O64};
use object_ids::ThinRecordId;

use crate::object_ids::TableId;

pub mod object_ids;
pub mod store;

trait IntoBytes: Sized {
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

struct ByteEncoder<'a> {
    cursor: Cursor<&'a mut [u8]>,
}

impl ByteEncoder<'_> {
    fn encode<T: 'static>(&mut self, value: T) -> Result<()> {
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

    fn encode_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.cursor.write_all(bytes)?;
        Ok(())
    }
}

trait FromBytes: IntoBytes {
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

struct ByteDecoder<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> ByteDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(bytes),
        }
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

#[derive(Clone, Copy)]
struct InternalPath {
    len: usize,
    buf: [u8; 256],
}

impl Default for InternalPath {
    fn default() -> Self {
        Self {
            len: 0,
            buf: [0; 256],
        }
    }
}

impl std::ops::Deref for InternalPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        self.as_path()
    }
}

impl AsRef<Path> for InternalPath {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl AsRef<[u8]> for InternalPath {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for InternalPath {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl std::fmt::Debug for InternalPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_path().fmt(f)
    }
}

impl PartialEq for InternalPath {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for InternalPath {}

impl std::hash::Hash for InternalPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl IntoBytes for InternalPath {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.len)?;
        x.encode_bytes(self.as_slice())?;
        Ok(())
    }
}

impl FromBytes for InternalPath {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.len)?;
        x.cursor.read_exact(&mut this.buf[..this.len])?;
        Ok(())
    }
}

impl InternalPath {
    pub fn new(path: &Path) -> Result<Self> {
        let mut buf = [0; 256];
        let path_bytes = path.as_os_str().as_bytes();
        let path_len = path_bytes.len();

        if path_len > 256 {
            anyhow::bail!("path too long");
        }

        buf[..path_len].copy_from_slice(path_bytes);
        Ok(Self { buf, len: path_len })
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_path(&self) -> &Path {
        Path::new(os_str::OsStr::from_bytes(self.as_slice()))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        &mut self.buf[..self.len]
    }

    pub fn replace(&mut self, path: &Path) -> Result<()> {
        let path_bytes = path.as_os_str().as_bytes();
        let path_len = path_bytes.len();

        if path_len > 256 {
            anyhow::bail!("path too long");
        }

        self.len = path_len;
        self.buf[..path_len].copy_from_slice(path_bytes);
        Ok(())
    }
}
