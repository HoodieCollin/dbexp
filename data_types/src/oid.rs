use base62::{decode, encode};

pub trait ObjectId {
    fn as_usize(&self) -> usize;
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct O16(u16);

impl O16 {
    pub const SENTINEL: Self = Self(u16::MIN);

    pub fn new() -> Self {
        let mut id = rand::random::<u16>();

        while id == u16::MIN {
            id = rand::random::<u16>();
        }

        Self(id)
    }

    pub fn from_uint(id: impl Into<u16>) -> Self {
        Self(id.into())
    }

    pub fn from_array(bytes: [u8; 2]) -> Self {
        Self(u16::from_ne_bytes(bytes))
    }

    pub fn into_array(&self) -> [u8; 2] {
        self.0.to_ne_bytes()
    }
}

impl Default for O16 {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectId for O16 {
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Debug for O16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0))
    }
}

impl std::fmt::Display for O16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0))
    }
}

impl serde::Serialize for O16 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(self.0))
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
                    Ok(O16(v as u16))
                }
            }
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct O32(u32);

impl O32 {
    pub const SENTINEL: Self = Self(u32::MIN);

    pub fn new() -> Self {
        let mut id = rand::random::<u32>();

        while id == u32::MIN {
            id = rand::random::<u32>();
        }

        Self(id)
    }

    pub fn from_uint(id: impl Into<u32>) -> Self {
        Self(id.into())
    }

    pub fn from_array(bytes: [u8; 4]) -> Self {
        Self(u32::from_ne_bytes(bytes))
    }

    pub fn into_array(&self) -> [u8; 4] {
        self.0.to_ne_bytes()
    }
}

impl Default for O32 {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectId for O32 {
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Debug for O32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0))
    }
}

impl std::fmt::Display for O32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0))
    }
}

impl serde::Serialize for O32 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(self.0))
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
                    Ok(O32(v as u32))
                }
            }
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct O64(u64);

impl O64 {
    pub const SENTINEL: Self = Self(u64::MIN);

    pub fn new() -> Self {
        let mut id = rand::random::<u64>();

        while id == u64::MIN {
            id = rand::random::<u64>();
        }

        Self(id)
    }

    pub fn from_uint(id: impl Into<u64>) -> Self {
        Self(id.into())
    }

    pub fn from_array(bytes: [u8; 8]) -> Self {
        Self(u64::from_ne_bytes(bytes))
    }

    pub fn into_array(&self) -> [u8; 8] {
        self.0.to_ne_bytes()
    }
}

impl Default for O64 {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectId for O64 {
    fn as_usize(&self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Debug for O64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0))
    }
}

impl std::fmt::Display for O64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", encode(self.0))
    }
}

impl serde::Serialize for O64 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode(self.0))
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
                    Ok(O64(v as u64))
                }
            }
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        }
    }
}
