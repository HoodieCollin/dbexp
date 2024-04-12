use base62::{decode, encode};

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct O16(u16);

impl O16 {
    pub fn new() -> Self {
        Self(rand::random::<u16>())
    }

    pub fn from_le_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_le_bytes(bytes))
    }

    pub fn from_be_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_be_bytes(bytes))
    }

    pub fn to_le_bytes(&self) -> [u8; 2] {
        self.0.to_le_bytes()
    }

    pub fn to_be_bytes(&self) -> [u8; 2] {
        self.0.to_be_bytes()
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
                if v > u16::MAX as u128 {
                    Err(serde::de::Error::custom("value out of range"))
                } else {
                    Ok(O16(v as u16))
                }
            }
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        }
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct O32(u32);

impl O32 {
    pub fn new() -> Self {
        Self(rand::random::<u32>())
    }

    pub fn from_le_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }

    pub fn from_be_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_be_bytes(bytes))
    }

    pub fn to_le_bytes(&self) -> [u8; 4] {
        self.0.to_le_bytes()
    }

    pub fn to_be_bytes(&self) -> [u8; 4] {
        self.0.to_be_bytes()
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
                if v > u32::MAX as u128 {
                    Err(serde::de::Error::custom("value out of range"))
                } else {
                    Ok(O32(v as u32))
                }
            }
            Err(e) => Err(serde::de::Error::custom(e.to_string())),
        }
    }
}
