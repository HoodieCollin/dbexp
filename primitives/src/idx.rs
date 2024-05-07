use std::num::NonZeroU64;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    byte_encoding::{ByteEncoder, IntoBytes, ScalarFromBytes},
    O16,
};

mod ops;
pub mod thin;

pub use thin::ThinIdx;

const U48_MAX: u64 = u64::MAX >> u16::BITS;
const OID_INIT: [u8; 2] = [0; 2];
const U48_INIT: [u8; 8] = [0; 8];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct SerdeIdx {
    pub id: O16,
    pub value: usize,
}

/// A 16-bit generation id followed by a 48-bit index. Internally implemented as a non-zero `u64`.
/// Despite the non-zero constraint internally, the value real value is stored as `n + 1` allowing zero to be stored safely.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Idx(NonZeroU64);

impl std::fmt::Debug for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (id, n) = self.into_parts();

        write!(f, "Idx({}|{:?})", n, id)
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

impl Serialize for Idx {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let (id, value) = self.into_parts();
        let serde_idx = SerdeIdx { id, value };

        serde_idx.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Idx {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let serde_idx = SerdeIdx::deserialize(deserializer)?;

        Ok(Idx::from_parts(serde_idx.id, serde_idx.value))
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
        let arr: [u8; 8] = match bytes.try_into() {
            Ok(arr) => arr,
            Err(_) => return Ok(None),
        };

        let mut id_bytes = OID_INIT;
        id_bytes.copy_from_slice(&arr[..2]);

        match O16::from_array(id_bytes) {
            Some(..) => {}
            None => return Ok(None),
        }

        let mut idx_bytes = U48_INIT;
        idx_bytes.copy_from_slice(&arr[2..]);

        // value is assumed to be stored as n + 1 by `into_array`
        let n = {
            let n = u64::from_ne_bytes(idx_bytes);

            // this should never happen
            if n == u64::MIN {
                return Ok(None);
            }

            n - 1
        };

        if n >= Idx::MAX + 1 || n == u64::MIN {
            Ok(None)
        } else {
            Ok(Some(Idx(unsafe {
                NonZeroU64::new_unchecked(u64::from_ne_bytes(arr))
            })))
        }
    }
}

impl Idx {
    pub const MAX: u64 = U48_MAX;
    pub const INVALID: Self = Self(NonZeroU64::MAX);
    pub const NIL: Option<Self> = None;

    pub fn new(n: usize) -> Self {
        let n = n as u64;

        if n >= Self::MAX + 1 {
            Self::INVALID
        } else {
            let n = n + 1;
            let id = O16::new();
            let mut bytes = U48_INIT;
            bytes[..2].copy_from_slice(&id.into_array());
            bytes[2..].copy_from_slice(&n.to_ne_bytes()[..6]);

            Self(unsafe { NonZeroU64::new_unchecked(u64::from_ne_bytes(bytes)) })
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

    pub fn into_thin(self) -> ThinIdx {
        let n = self.into_usize();
        ThinIdx::new(n)
    }

    pub fn from_thin(thin: ThinIdx) -> Self {
        let n = thin.into_usize();
        Self::new(n)
    }

    pub fn into_array(&self) -> [u8; 8] {
        self.0.get().to_ne_bytes()
    }

    pub fn from_array(arr: [u8; 8]) -> Option<Self> {
        let mut id_bytes = OID_INIT;
        id_bytes.copy_from_slice(&arr[..2]);
        O16::from_array(id_bytes)?;

        let mut idx_bytes = U48_INIT;
        idx_bytes.copy_from_slice(&arr[2..]);

        // value is assumed to be stored as n + 1 by `into_array`
        let n = {
            let n = u64::from_ne_bytes(idx_bytes);

            // this should never happen
            if n == u64::MIN {
                return None;
            }

            n - 1
        };

        if n >= Self::MAX + 1 || n == u64::MIN {
            None
        } else {
            Some(Self(unsafe {
                NonZeroU64::new_unchecked(u64::from_ne_bytes(arr))
            }))
        }
    }

    pub fn try_from_array(arr: impl TryInto<[u8; 8]>) -> Result<Self> {
        match arr.try_into() {
            Ok(arr) => {
                let mut id_bytes = OID_INIT;
                id_bytes.copy_from_slice(&arr[..2]);
                O16::from_array(id_bytes).ok_or_else(|| anyhow::anyhow!("invalid id bytes"))?;

                let mut idx_bytes = U48_INIT;
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

                if n == u64::MIN {
                    anyhow::bail!("cannot be zero")
                } else if n >= Self::MAX + 1 {
                    anyhow::bail!("exceeds max value")
                } else {
                    Ok(Self(unsafe {
                        NonZeroU64::new_unchecked(u64::from_ne_bytes(arr))
                    }))
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn into_gen_id(self) -> O16 {
        let mut bytes = OID_INIT;
        bytes.copy_from_slice(&self.0.get().to_ne_bytes()[..2]);
        O16::from_array(bytes).unwrap()
    }

    pub fn into_u64(self) -> u64 {
        let n = self.0.get() >> u16::BITS; // remove the generation id
        n - 1
    }

    pub fn into_usize(self) -> usize {
        self.into_u64() as usize
    }

    pub fn from_parts(id: O16, n: usize) -> Self {
        let n = n as u64;

        if n >= Self::MAX + 1 {
            return Self::INVALID;
        }

        let n = n + 1;
        let mut bytes = U48_INIT;
        bytes[..2].copy_from_slice(&id.into_array());
        bytes[2..].copy_from_slice(&n.to_ne_bytes()[2..]);

        Self(unsafe { NonZeroU64::new_unchecked(u64::from_ne_bytes(bytes)) })
    }

    pub fn into_parts(self) -> (O16, usize) {
        (self.into_gen_id(), self.into_usize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idx() -> Result<()> {
        let idx = Idx::new(0);
        assert_eq!(idx.into_usize(), 0);

        let idx = Idx::new(1);
        assert_eq!(idx.into_usize(), 1);

        let idx = Idx::new(2);
        assert_eq!(idx.into_usize(), 2);

        let bytes = idx.into_bytes()?;
        let idx2 = Idx::from_bytes(&bytes)?;
        assert_eq!(idx, idx2);

        Ok(())
    }
}
