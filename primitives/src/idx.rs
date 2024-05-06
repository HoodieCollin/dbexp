use std::num::NonZeroU64;

use anyhow::Result;

use crate::O16;

/// A 16-bit generation id followed by a 48-bit index. Internally implemented as a non-zero `u64`.
/// Despite the non-zero constraint internally, the value real value is stored as `n + 1` allowing zero to be stored safely.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Idx(NonZeroU64);

impl std::fmt::Debug for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.into_u64(), f)
    }
}

impl std::fmt::Display for Idx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.into_u64(), f)
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

const U48_MAX: u64 = u64::MAX >> u16::BITS;
const OID_INIT: [u8; 2] = [0; 2];
const U48_INIT: [u8; 8] = [0; 8];

impl Idx {
    pub const MAX: u64 = U48_MAX;
    pub const INVALID: Self = Self(NonZeroU64::MAX);
    pub const SENTINEL: Option<Self> = None;

    pub fn new(n: u64) -> Self {
        if n >= Self::MAX + 1 {
            Self::INVALID
        } else {
            let n = n + 1;
            let id = O16::new();
            let mut bytes = U48_INIT;
            bytes[..2].copy_from_slice(&id.into_array());
            bytes[2..].copy_from_slice(&n.to_ne_bytes()[2..]);

            Self(unsafe { NonZeroU64::new_unchecked(u64::from_ne_bytes(bytes)) })
        }
    }

    pub fn from_array(arr: [u8; 8]) -> Option<Self> {
        let mut id_bytes = OID_INIT;
        id_bytes.copy_from_slice(&arr[..2]);
        O16::from_array(id_bytes)?;

        let mut idx_bytes = U48_INIT;
        idx_bytes.copy_from_slice(&arr[2..]);

        let n = u64::from_ne_bytes(idx_bytes);

        if n >= Self::MAX + 1 || n == 0 {
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
                idx_bytes.copy_from_slice(&arr[2..]);

                let n = u64::from_ne_bytes(idx_bytes);

                if n == 0 {
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

    pub fn into_array(&self) -> [u8; 8] {
        self.0.get().to_ne_bytes()
    }

    pub fn into_id(self) -> O16 {
        let mut bytes = OID_INIT;
        bytes.copy_from_slice(&self.0.get().to_ne_bytes()[..2]);
        O16::from_array(bytes).unwrap()
    }

    pub fn into_u64(self) -> u64 {
        let n = self.0.get() >> 2; // remove the generation id
        n - 1
    }

    pub fn into_usize(self) -> usize {
        self.into_u64() as usize
    }

    pub fn into_parts(self) -> (O16, u64) {
        (self.into_id(), self.into_u64())
    }
}
