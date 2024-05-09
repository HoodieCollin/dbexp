use std::num::NonZeroU64;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::byte_encoding::{ByteEncoder, IntoBytes, ScalarFromBytes};

mod ops;

pub mod gen;
pub mod thin;

pub use gen::Gen;
pub use thin::{MaybeThinIdx, ThinIdx};

const U48_MAX: u64 = (u64::MAX >> u16::BITS) - 1;
const OID_INIT: [u8; 2] = [0; 2];
const U64_BYTES_INIT: [u8; 8] = [0; 8];

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Idx(ThinIdx);

impl std::fmt::Debug for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !f.alternate() {
            write!(f, "{:?}", self.into_usize())
        } else {
            f.debug_struct("Idx")
                .field("gen", &self.into_gen().into_raw())
                .field("val", &self.into_usize())
                .finish()
        }
    }
}

impl std::fmt::Display for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.into_usize(), f)
    }
}

impl Into<u64> for Idx {
    fn into(self) -> u64 {
        self.into_u64()
    }
}

impl Into<usize> for Idx {
    fn into(self) -> usize {
        self.into_usize()
    }
}

impl From<ThinIdx> for Idx {
    fn from(thin: ThinIdx) -> Self {
        Self::from_thin(thin)
    }
}

impl From<Idx> for Gen {
    fn from(idx: Idx) -> Self {
        idx.into_gen()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct SerdeIdx {
    pub id: Gen,
    pub value: NonZeroU64,
}

impl Serialize for Idx {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (id, value) = self.into_parts();
        let serde_idx = SerdeIdx { id, value };

        serde_idx.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Idx {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let SerdeIdx { id, value } = SerdeIdx::deserialize(deserializer)?;

        if value.get() == 0 {
            return Err(serde::de::Error::custom("value cannot be zero"));
        } else if value.get() > U48_MAX {
            return Err(serde::de::Error::custom("value exceeds max value"));
        }

        Ok(unsafe { Idx::from_parts(id, value) })
    }
}

crate::impl_access_bytes_for_into_bytes_type!(Idx);

impl IntoBytes for Idx {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode_bytes(&self.into_array())
    }
}

impl ScalarFromBytes for Idx {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl ScalarFromBytes for Option<Idx> {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        match bytes.try_into() {
            Ok(arr) => Ok(Idx::from_array(arr)),
            Err(_) => return Ok(None),
        }
    }
}

impl Idx {
    pub const MAX: usize = U48_MAX as usize;
    pub const INVALID: Self = Self(ThinIdx::INVALID);
    pub const NIL: Option<Self> = None;

    pub fn new(n: usize) -> Self {
        let n = n as u64;

        if n >= U48_MAX + 1 {
            Self::INVALID
        } else {
            let n = n + 1;
            let id = Gen::new();
            let mut bytes = U64_BYTES_INIT;
            bytes[..2].copy_from_slice(&id.into_array());
            bytes[2..].copy_from_slice(&n.to_ne_bytes()[..6]);

            Self(unsafe { ThinIdx::exact_unchecked(u64::from_ne_bytes(bytes)) })
        }
    }

    #[must_use]
    pub fn new_validated(n: usize) -> Result<Self> {
        let new = Self::new(n);

        if new == Self::INVALID {
            anyhow::bail!("exceeds max value")
        } else {
            Ok(new)
        }
    }

    pub fn into_maybe_thin(self) -> MaybeThinIdx {
        MaybeThinIdx::from(self)
    }

    pub fn into_thin(self) -> ThinIdx {
        self.0
    }

    pub fn from_thin(thin: ThinIdx) -> Self {
        Self(thin)
    }

    pub fn into_array(&self) -> [u8; 8] {
        self.0.into_array()
    }

    pub fn from_array(arr: [u8; 8]) -> Option<Self> {
        match Self::try_from_array(arr) {
            Ok(idx) => Some(idx),
            Err(_) => None,
        }
    }

    pub fn try_from_array(arr: impl TryInto<[u8; 8]>) -> Result<Self> {
        match arr.try_into() {
            Ok(arr) => {
                Gen::try_from_array(&arr[..2])?;

                let mut idx_bytes = U64_BYTES_INIT;
                idx_bytes[..6].copy_from_slice(&arr[2..]);

                // value is assumed to be stored as n + 1 by `into_array`
                let n = {
                    let n = u64::from_ne_bytes(idx_bytes);

                    // this should never happen
                    if n == u64::MIN {
                        anyhow::bail!("cannot be zero")
                    }

                    n - 1
                };

                if n >= U48_MAX + 1 {
                    anyhow::bail!("exceeds max value")
                } else {
                    Ok(Self(unsafe {
                        ThinIdx::exact_unchecked(u64::from_ne_bytes(arr))
                    }))
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn into_gen(self) -> Gen {
        let mut bytes = OID_INIT;
        bytes.copy_from_slice(&self.0 .0.get().to_ne_bytes()[..2]);
        Gen::from_array(bytes).unwrap()
    }

    pub fn into_u64(self) -> u64 {
        self.0.into_u64()
    }

    pub fn into_usize(self) -> usize {
        self.into_u64() as usize
    }

    /// Extract the generation id and the value as a `NonZeroU64` representing `n + 1`.
    pub fn into_parts(self) -> (Gen, NonZeroU64) {
        (self.into_gen(), unsafe {
            NonZeroU64::new_unchecked(self.0.into_u64() + 1)
        })
    }

    /// Assumes the value is stored as `n + 1` and constructs an `Idx` from the generation id and value.
    pub unsafe fn from_parts(id: Gen, n: NonZeroU64) -> Self {
        let mut bytes = U64_BYTES_INIT;
        bytes[..2].copy_from_slice(&id.into_array());
        bytes[2..].copy_from_slice(&n.get().to_ne_bytes()[..6]);

        Self(ThinIdx::exact_unchecked(u64::from_ne_bytes(bytes)))
    }
}

#[cfg(test)]
mod tests {
    use crate::into_bytes;

    use super::*;

    #[test]
    fn test_into_primitive() {
        let idx = Idx::new(2);

        assert_eq!(idx.into_usize(), 2);
        assert_eq!(idx.into_u64(), 2);

        const N: u64 = 1_000_000;
        let idx = Idx::new(N as usize);

        assert_eq!(idx.into_usize(), N as usize);
        assert_eq!(idx.into_u64(), N);
    }

    #[test]
    fn test_max() {
        let max = Idx::MAX;

        assert_ne!(Idx::new(max), Idx::INVALID);
        assert_eq!(Idx::new(max + 1), Idx::INVALID);
    }

    #[test]
    fn test_into_from_bytes() -> Result<()> {
        let idx = Idx::new(2);

        let bytes = into_bytes!(idx, Idx)?;
        let idx2 = Idx::from_bytes(&bytes)?;

        assert_eq!(idx, idx2);

        Ok(())
    }
}
