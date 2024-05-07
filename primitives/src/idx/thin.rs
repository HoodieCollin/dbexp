use std::num::NonZeroU64;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::byte_encoding::{ByteEncoder, IntoBytes, ScalarFromBytes};

use super::{Idx, U48_MAX};

/// A 64-bit index with a max value of `u64::MAX >> 16`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ThinIdx(NonZeroU64);

impl std::fmt::Debug for ThinIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.into_u64(), f)
    }
}

impl std::fmt::Display for ThinIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.into_u64(), f)
    }
}

impl From<ThinIdx> for usize {
    fn from(idx: ThinIdx) -> Self {
        idx.into_usize()
    }
}

impl From<usize> for ThinIdx {
    fn from(n: usize) -> Self {
        Self::new(n)
    }
}

impl From<ThinIdx> for u64 {
    fn from(idx: ThinIdx) -> Self {
        idx.into_u64()
    }
}

impl From<u64> for ThinIdx {
    fn from(n: u64) -> Self {
        Self::new(n as usize)
    }
}

impl Serialize for ThinIdx {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.into_u64().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ThinIdx {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let n = u64::deserialize(deserializer)?;

        Ok(ThinIdx::new(n as usize))
    }
}

impl From<Idx> for ThinIdx {
    fn from(idx: Idx) -> Self {
        Self(unsafe { NonZeroU64::new_unchecked(idx.into_u64()) })
    }
}

crate::impl_access_bytes_for_into_bytes_type!(ThinIdx);

impl IntoBytes for ThinIdx {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode_bytes(&self.into_array())
    }
}

impl ScalarFromBytes for ThinIdx {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl ScalarFromBytes for Option<ThinIdx> {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = match bytes.try_into() {
            Ok(arr) => arr,
            Err(_) => return Ok(None),
        };

        let base = u64::from_ne_bytes(arr);

        if base == u64::MIN {
            Ok(None)
        } else {
            let new = ThinIdx::new(base as usize);

            if new == ThinIdx::INVALID {
                Ok(None)
            } else {
                Ok(Some(new))
            }
        }
    }
}

impl ThinIdx {
    pub const MAX: usize = U48_MAX as usize;
    pub const INVALID: Self = Self(NonZeroU64::MAX);
    pub const NIL: Option<Self> = None;

    pub fn new(n: usize) -> Self {
        let n = n as u64;

        if n >= U48_MAX + 1 {
            Self::INVALID
        } else {
            Self(unsafe { NonZeroU64::new_unchecked(n + 1) })
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

    pub unsafe fn new_unchecked(n: usize) -> Self {
        Self(NonZeroU64::new_unchecked(n as u64 + 1))
    }

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    pub fn into_idx(self) -> Idx {
        Idx::new(self.into_usize())
    }

    pub fn into_array(&self) -> [u8; 8] {
        self.0.get().to_ne_bytes()
    }

    pub fn from_array(arr: [u8; 8]) -> Option<Self> {
        // value is assumed to be stored as n + 1 by `into_array`
        let n = {
            let n = u64::from_ne_bytes(arr);

            // this should never happen
            if n == u64::MIN {
                return None;
            }

            n - 1
        };

        let new = Self::new(n as usize);

        if new == Self::INVALID {
            None
        } else {
            Some(new)
        }
    }

    pub fn try_from_array(arr: impl TryInto<[u8; 8]>) -> Result<Self> {
        match arr.try_into() {
            Ok(arr) => {
                // value is assumed to be stored as n + 1 by `into_array`
                let n = {
                    let n = u64::from_ne_bytes(arr);

                    if n == u64::MIN {
                        anyhow::bail!("cannot be zero")
                    }

                    n - 1
                };

                let new = Self::new(n as usize);

                if new == Self::INVALID {
                    anyhow::bail!("exceeds max value")
                } else {
                    Ok(new)
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn into_u64(self) -> u64 {
        self.0.get() - 1
    }

    pub fn into_usize(self) -> usize {
        self.into_u64() as usize
    }
}
