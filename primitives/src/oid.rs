use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use anyhow::Result;
use base62::{decode, encode};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct O16(NonZeroU16);

impl O16 {
    pub const INVALID: Self = Self(NonZeroU16::MAX);
    pub const NIL: Option<Self> = None;

    pub fn new() -> Self {
        let mut id = rand::random::<u16>();

        while id == u16::MIN || id == u16::MAX {
            id = rand::random::<u16>();
        }

        Self(unsafe { NonZeroU16::new_unchecked(id) })
    }

    pub fn from_uint(id: impl Into<u16>) -> Option<Self> {
        Some(Self(NonZeroU16::new(id.into())?))
    }

    pub fn try_from_uint(id: impl TryInto<u16>) -> Result<Self> {
        match id.try_into() {
            Ok(id) => {
                if id == u16::MIN {
                    anyhow::bail!("cannot be zero")
                } else {
                    Ok(Self(unsafe { NonZeroU16::new_unchecked(id) }))
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn from_array(bytes: [u8; 2]) -> Option<Self> {
        Some(Self(NonZeroU16::new(u16::from_ne_bytes(bytes))?))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 2]>) -> Result<Self> {
        match bytes.try_into() {
            Ok(bytes) => {
                let id = u16::from_ne_bytes(bytes);

                if id == u16::MIN {
                    anyhow::bail!("cannot be zero")
                } else {
                    Ok(Self(unsafe { NonZeroU16::new_unchecked(id) }))
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn into_array(&self) -> [u8; 2] {
        self.0.get().to_ne_bytes()
    }

    pub fn into_usize(self) -> usize {
        self.0.get() as usize
    }
}

impl Default for O16 {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for O16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0.get()))
    }
}

impl std::fmt::Display for O16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0.get()))
    }
}

impl serde::Serialize for O16 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(self.0.get()))
    }
}

impl<'de> serde::Deserialize<'de> for O16 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;

        match decode(&s) {
            Ok(v) => {
                if v > u16::MIN as u128 {
                    Err(serde::de::Error::custom("value out of range"))
                } else {
                    Ok(O16(unsafe { NonZeroU16::new_unchecked(v as u16) }))
                }
            }
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct O32(NonZeroU32);

impl O32 {
    pub const INVALID: Self = Self(NonZeroU32::MAX);
    pub const NIL: Option<Self> = None;

    pub fn new() -> Self {
        let mut id = rand::random::<u32>();

        while id == u32::MIN || id == u32::MAX {
            id = rand::random::<u32>();
        }

        Self(unsafe { NonZeroU32::new_unchecked(id) })
    }

    pub fn from_uint(id: impl Into<u32>) -> Option<Self> {
        Some(Self(NonZeroU32::new(id.into())?))
    }

    pub fn try_from_uint(id: impl TryInto<u32>) -> Result<Self> {
        match id.try_into() {
            Ok(id) => {
                if id == u32::MIN {
                    anyhow::bail!("cannot be zero")
                } else {
                    Ok(Self(unsafe { NonZeroU32::new_unchecked(id) }))
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn from_array(bytes: [u8; 4]) -> Option<Self> {
        Some(Self(NonZeroU32::new(u32::from_ne_bytes(bytes))?))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 4]>) -> Result<Self> {
        match bytes.try_into() {
            Ok(bytes) => {
                let id = u32::from_ne_bytes(bytes);

                if id == u32::MIN {
                    anyhow::bail!("cannot be zero")
                } else {
                    Ok(Self(unsafe { NonZeroU32::new_unchecked(id) }))
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn into_array(&self) -> [u8; 4] {
        self.0.get().to_ne_bytes()
    }

    pub fn into_usize(self) -> usize {
        self.0.get() as usize
    }
}

impl Default for O32 {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for O32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0.get()))
    }
}

impl std::fmt::Display for O32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0.get()))
    }
}

impl serde::Serialize for O32 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(self.0.get()))
    }
}

impl<'de> serde::Deserialize<'de> for O32 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;

        match decode(&s) {
            Ok(v) => {
                if v > u32::MIN as u128 {
                    Err(serde::de::Error::custom("value out of range"))
                } else {
                    Ok(O32(unsafe { NonZeroU32::new_unchecked(v as u32) }))
                }
            }
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct O64(NonZeroU64);

impl O64 {
    pub const INVALID: Self = Self(NonZeroU64::MAX);
    pub const NIL: Option<Self> = None;

    pub fn new() -> Self {
        let mut id = rand::random::<u64>();

        while id == u64::MIN || id == u64::MAX {
            id = rand::random::<u64>();
        }

        Self(unsafe { NonZeroU64::new_unchecked(id) })
    }

    pub fn from_uint(id: impl Into<u64>) -> Option<Self> {
        Some(Self(NonZeroU64::new(id.into())?))
    }

    pub fn try_from_uint(id: impl TryInto<u64>) -> Result<Self> {
        match id.try_into() {
            Ok(id) => {
                if id == u64::MIN {
                    anyhow::bail!("cannot be zero")
                } else {
                    Ok(Self(unsafe { NonZeroU64::new_unchecked(id) }))
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn from_array(bytes: [u8; 8]) -> Option<Self> {
        Some(Self(NonZeroU64::new(u64::from_ne_bytes(bytes))?))
    }

    pub fn try_from_array(bytes: impl TryInto<[u8; 8]>) -> Result<Self> {
        match bytes.try_into() {
            Ok(bytes) => {
                let id = u64::from_ne_bytes(bytes);

                if id == u64::MIN {
                    anyhow::bail!("cannot be zero")
                } else {
                    Ok(Self(unsafe { NonZeroU64::new_unchecked(id) }))
                }
            }
            Err(_) => anyhow::bail!("invalid value"),
        }
    }

    pub fn into_array(&self) -> [u8; 8] {
        self.0.get().to_ne_bytes()
    }

    pub fn into_usize(self) -> usize {
        self.0.get() as usize
    }
}

impl Default for O64 {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for O64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0.get()))
    }
}

impl std::fmt::Display for O64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0.get()))
    }
}

impl serde::Serialize for O64 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(self.0.get()))
    }
}

impl<'de> serde::Deserialize<'de> for O64 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;

        match decode(&s) {
            Ok(v) => {
                if v > u64::MIN as u128 {
                    Err(serde::de::Error::custom("value out of range"))
                } else {
                    Ok(O64(unsafe { NonZeroU64::new_unchecked(v as u64) }))
                }
            }
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        }
    }
}
