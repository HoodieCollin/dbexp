use std::num::NonZeroU64;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::byte_encoding::{ByteEncoder, IntoBytes, ScalarFromBytes};

use super::{Gen, Idx, U48_MAX, U64_BYTES_INIT};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum MaybeThinIdx {
    Thin(ThinIdx),
    Full(Idx),
}

impl std::fmt::Debug for MaybeThinIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Thin(thin) => {
                write!(f, "ThinIdx({})", thin)
            }
            Self::Full(full) => {
                if !f.alternate() {
                    write!(f, "Idx({:?})", full.into_usize())
                } else {
                    std::fmt::Debug::fmt(full, f)
                }
            }
        }
    }
}

impl std::fmt::Display for MaybeThinIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Thin(thin) => std::fmt::Display::fmt(thin, f),
            Self::Full(full) => std::fmt::Display::fmt(full, f),
        }
    }
}

impl From<MaybeThinIdx> for ThinIdx {
    fn from(maybe: MaybeThinIdx) -> Self {
        match maybe {
            MaybeThinIdx::Thin(thin) => thin,
            MaybeThinIdx::Full(full) => full.into(),
        }
    }
}

impl From<ThinIdx> for MaybeThinIdx {
    fn from(thin: ThinIdx) -> Self {
        Self::Thin(thin)
    }
}

impl From<Idx> for MaybeThinIdx {
    fn from(full: Idx) -> Self {
        Self::Full(full)
    }
}

impl PartialEq for MaybeThinIdx {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Thin(a), Self::Thin(b)) => a == b,
            (Self::Full(a), Self::Full(b)) => a == b,
            (Self::Thin(a), Self::Full(b)) => *a == ThinIdx::INVALID && *b == Idx::INVALID,
            (Self::Full(a), Self::Thin(b)) => *a == Idx::INVALID && *b == ThinIdx::INVALID,
        }
    }
}

impl Eq for MaybeThinIdx {}

impl std::hash::Hash for MaybeThinIdx {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Thin(thin) => thin.hash(state),
            Self::Full(full) => full.hash(state),
        }
    }
}

impl PartialEq<ThinIdx> for MaybeThinIdx {
    fn eq(&self, other: &ThinIdx) -> bool {
        match self {
            Self::Thin(thin) => thin == other,
            Self::Full(full) => full.0 == *other,
        }
    }
}

impl PartialEq<MaybeThinIdx> for ThinIdx {
    fn eq(&self, other: &MaybeThinIdx) -> bool {
        other == self
    }
}

impl PartialEq<Idx> for MaybeThinIdx {
    fn eq(&self, other: &Idx) -> bool {
        match self {
            Self::Thin(..) => false,
            Self::Full(full) => full == other,
        }
    }
}

impl PartialEq<MaybeThinIdx> for Idx {
    fn eq(&self, other: &MaybeThinIdx) -> bool {
        other == self
    }
}

impl PartialOrd for MaybeThinIdx {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Thin(a), Self::Thin(b)) => a.partial_cmp(b),
            (Self::Full(a), Self::Full(b)) => a.partial_cmp(b),
            (Self::Thin(a), Self::Full(b)) => a.partial_cmp(&b.0),
            (Self::Full(a), Self::Thin(b)) => a.0.partial_cmp(b),
        }
    }
}

impl Ord for MaybeThinIdx {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd<ThinIdx> for MaybeThinIdx {
    fn partial_cmp(&self, other: &ThinIdx) -> Option<std::cmp::Ordering> {
        match self {
            Self::Thin(thin) => thin.partial_cmp(other),
            Self::Full(full) => full.0.partial_cmp(other),
        }
    }
}

impl PartialOrd<MaybeThinIdx> for ThinIdx {
    fn partial_cmp(&self, other: &MaybeThinIdx) -> Option<std::cmp::Ordering> {
        other.partial_cmp(self).map(std::cmp::Ordering::reverse)
    }
}

impl PartialOrd<Idx> for MaybeThinIdx {
    fn partial_cmp(&self, other: &Idx) -> Option<std::cmp::Ordering> {
        match self {
            Self::Thin(thin) => thin.partial_cmp(&other.0),
            Self::Full(full) => full.partial_cmp(other),
        }
    }
}

impl PartialOrd<MaybeThinIdx> for Idx {
    fn partial_cmp(&self, other: &MaybeThinIdx) -> Option<std::cmp::Ordering> {
        other.partial_cmp(self).map(std::cmp::Ordering::reverse)
    }
}

crate::impl_access_bytes_for_into_bytes_type!(MaybeThinIdx);

impl IntoBytes for MaybeThinIdx {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode_bytes(&self.into_array())
    }
}

impl MaybeThinIdx {
    pub const INVALID: Self = Self::Thin(ThinIdx::INVALID);
    pub const NIL: Option<Self> = None;

    pub fn new(n: usize) -> Self {
        Self::Thin(ThinIdx::new(n))
    }

    pub fn new_validated(n: usize) -> Result<Self> {
        Ok(Self::Thin(ThinIdx::new_validated(n)?))
    }

    pub fn upgrade(&mut self) {
        *self = self.into_upgraded();
    }

    pub fn downgrade(&mut self) {
        *self = self.into_downgraded();
    }

    pub fn into_upgraded(self) -> Self {
        match self {
            Self::Thin(thin) => Self::Full(thin.into_idx()),
            full => full,
        }
    }

    pub fn into_downgraded(self) -> Self {
        match self {
            Self::Full(full) => Self::Thin(full.into_thin()),
            thin => thin,
        }
    }

    pub fn into_gen(self) -> Option<Gen> {
        match self {
            Self::Thin(..) => None,
            Self::Full(full) => Some(full.into_gen()),
        }
    }

    pub fn into_array(&self) -> [u8; 8] {
        match self {
            Self::Thin(thin) => thin.into_array(),
            Self::Full(full) => full.into_array(),
        }
    }

    pub fn into_u64(self) -> u64 {
        match self {
            Self::Thin(thin) => thin.into_u64(),
            Self::Full(full) => full.into_u64(),
        }
    }

    pub fn into_usize(self) -> usize {
        match self {
            Self::Thin(thin) => thin.into_usize(),
            Self::Full(full) => full.into_usize(),
        }
    }

    pub fn into_thin(self) -> ThinIdx {
        match self {
            Self::Thin(thin) => thin,
            Self::Full(full) => full.into_thin(),
        }
    }

    pub fn into_gen_and_u64(self) -> (Option<Gen>, u64) {
        match self {
            Self::Thin(thin) => (None, thin.into_u64()),
            Self::Full(full) => (Some(full.into_gen()), full.into_u64()),
        }
    }

    pub fn into_gen_and_usize(self) -> (Option<Gen>, usize) {
        match self {
            Self::Thin(thin) => (None, thin.into_usize()),
            Self::Full(full) => (Some(full.into_gen()), full.into_usize()),
        }
    }
}

/// A 64-bit index with a max value of `u64::MAX >> 16`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ThinIdx(pub(super) NonZeroU64);

impl std::fmt::Debug for ThinIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ThinIdx({:?})", self.into_usize())
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
        let value = NonZeroU64::deserialize(deserializer)?;

        if value.get() == 0 {
            return Err(serde::de::Error::custom("value cannot be zero"));
        } else if value.get() > U48_MAX {
            return Err(serde::de::Error::custom("value exceeds max value"));
        }

        Ok(ThinIdx::new(value.get() as usize))
    }
}

impl From<Idx> for ThinIdx {
    fn from(idx: Idx) -> Self {
        idx.0
    }
}

impl PartialEq<Idx> for ThinIdx {
    fn eq(&self, other: &Idx) -> bool {
        *self == other.0
    }
}

impl PartialEq<ThinIdx> for Idx {
    fn eq(&self, other: &ThinIdx) -> bool {
        self.0 == *other
    }
}

impl PartialOrd for ThinIdx {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.is_valid() {
            if other.is_valid() {
                self.into_u64().partial_cmp(&other.into_u64())
            } else {
                Some(std::cmp::Ordering::Greater)
            }
        } else if other.is_valid() {
            Some(std::cmp::Ordering::Less)
        } else {
            Some(std::cmp::Ordering::Equal)
        }
    }
}

impl Ord for ThinIdx {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.into_u64().cmp(&other.into_u64())
    }
}

impl PartialOrd<Idx> for ThinIdx {
    fn partial_cmp(&self, other: &Idx) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.0)
    }
}

impl PartialOrd<ThinIdx> for Idx {
    fn partial_cmp(&self, other: &ThinIdx) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
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
        match bytes.try_into() {
            Ok(arr) => Ok(ThinIdx::from_array(arr)),
            Err(_) => return Ok(None),
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
            let n = n + 1;
            let n_bytes = n.to_ne_bytes();
            let mut bytes = U64_BYTES_INIT;
            bytes[2..].copy_from_slice(&n_bytes[..6]);

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

    pub unsafe fn exact_unchecked(n: u64) -> Self {
        Self(NonZeroU64::new_unchecked(n))
    }

    pub fn is_valid(self) -> bool {
        self != Self::INVALID
    }

    pub fn into_idx(self) -> Idx {
        Idx::new(self.into_usize())
    }

    pub fn into_maybe_thin(self) -> MaybeThinIdx {
        MaybeThinIdx::Thin(self)
    }

    pub fn into_array(&self) -> [u8; 8] {
        self.0.get().to_ne_bytes()
    }

    /// Assumes the input array is a 64-bit uint with the value stored in bytes `2..` as `n + 1` where `n <= 48::MAX`.
    /// Returns `None` if the value is zero or exceeds the max value.
    ///
    /// > ### Note:
    /// > The first 2 bytes are reserved for the generation id of the parent `Idx` type.
    pub fn from_array(arr: [u8; 8]) -> Option<Self> {
        match Self::try_from_array(arr) {
            Ok(idx) => Some(idx),
            Err(_) => None,
        }
    }

    /// Assumes the input array is a 64-bit uint with the value stored in bytes `2..` as `n + 1` where `n <= 48::MAX`.
    /// Returns an error if the value is zero or exceeds the max value.
    ///
    /// > ### Note:
    /// > The first 2 bytes are reserved for the generation id of the parent `Idx` type.
    pub fn try_from_array(arr: impl TryInto<[u8; 8]>) -> Result<Self> {
        match arr.try_into() {
            Ok(arr) => {
                let mut idx_bytes = U64_BYTES_INIT;
                idx_bytes[..6].copy_from_slice(&arr[2..]);

                // value is assumed to be stored as n + 1 by `into_array`
                let n = {
                    let n = u64::from_ne_bytes(idx_bytes);

                    if n == u64::MIN {
                        anyhow::bail!("cannot be zero")
                    }

                    n - 1
                };

                if n >= U48_MAX + 1 {
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

    pub fn into_u64(self) -> u64 {
        let n = self.0.get() >> u16::BITS; // remove the generation id
        n - 1
    }

    pub fn into_usize(self) -> usize {
        self.into_u64() as usize
    }

    pub fn into_raw(self) -> NonZeroU64 {
        self.0
    }

    pub unsafe fn from_raw(raw: NonZeroU64) -> Self {
        Self(raw)
    }
}

#[cfg(test)]
mod tests {
    use crate::into_bytes;

    use super::*;

    #[test]
    fn test_into_primitive() {
        let idx = ThinIdx::new(2);

        assert_eq!(idx.into_usize(), 2);
        assert_eq!(idx.into_u64(), 2);

        const N: u64 = 1_000_000;
        let idx = ThinIdx::new(N as usize);

        assert_eq!(idx.into_usize(), N as usize);
        assert_eq!(idx.into_u64(), N);
    }

    #[test]
    fn test_max() {
        let max = ThinIdx::MAX;

        assert_ne!(ThinIdx::new(max), ThinIdx::INVALID);
        assert_eq!(ThinIdx::new(max + 1), ThinIdx::INVALID);
    }

    #[test]
    fn test_into_from_bytes() -> Result<()> {
        let idx = ThinIdx::new(2);

        // let bytes = idx.into_bytes()?;
        let bytes = into_bytes!(idx, ThinIdx)?;
        let idx2 = ThinIdx::from_bytes(&bytes)?;

        assert_eq!(idx, idx2);

        Ok(())
    }
}
