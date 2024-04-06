use fraction::Ratio;

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct R16(Ratio<i8>);

impl R16 {
    pub fn new() -> Self {
        Self(Ratio::new_raw(0, 1))
    }

    pub fn from_le_bytes(bytes: [u8; 2]) -> Self {
        Self(Ratio::new(
            i8::from_le_bytes([bytes[0]; 1]),
            i8::from_le_bytes([bytes[1]; 1]),
        ))
    }

    pub fn from_be_bytes(bytes: [u8; 2]) -> Self {
        Self(Ratio::new(
            i8::from_be_bytes([bytes[0]; 1]),
            i8::from_be_bytes([bytes[1]; 1]),
        ))
    }

    pub fn to_le_bytes(&self) -> [u8; 2] {
        [
            self.0.numer().to_le_bytes()[0],
            self.0.denom().to_le_bytes()[0],
        ]
    }

    pub fn to_be_bytes(&self) -> [u8; 2] {
        [
            self.0.numer().to_be_bytes()[0],
            self.0.denom().to_be_bytes()[0],
        ]
    }
}

impl std::fmt::Debug for R16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for R16 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for R16 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for R16 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map(R16)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct R32(Ratio<i16>);

impl R32 {
    pub fn new() -> Self {
        Self(Ratio::new_raw(0, 1))
    }

    pub fn from_le_bytes(bytes: [u8; 4]) -> Self {
        Self(Ratio::new(
            i16::from_le_bytes(bytes[0..2].try_into().unwrap()),
            i16::from_le_bytes(bytes[2..4].try_into().unwrap()),
        ))
    }

    pub fn from_be_bytes(bytes: [u8; 4]) -> Self {
        Self(Ratio::new(
            i16::from_be_bytes(bytes[0..2].try_into().unwrap()),
            i16::from_be_bytes(bytes[2..4].try_into().unwrap()),
        ))
    }

    pub fn to_le_bytes(&self) -> [u8; 4] {
        let mut bytes = [0; 4];
        bytes[0..2].copy_from_slice(&self.0.numer().to_le_bytes());
        bytes[2..4].copy_from_slice(&self.0.denom().to_le_bytes());
        bytes
    }

    pub fn to_be_bytes(&self) -> [u8; 4] {
        let mut bytes = [0; 4];
        bytes[0..2].copy_from_slice(&self.0.numer().to_be_bytes());
        bytes[2..4].copy_from_slice(&self.0.denom().to_be_bytes());
        bytes
    }
}

impl std::fmt::Debug for R32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for R32 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for R32 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for R32 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map(R32)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct R64(Ratio<i32>);

impl R64 {
    pub fn new() -> Self {
        Self(Ratio::new_raw(0, 1))
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(Ratio::new(
            i32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            i32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        ))
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(Ratio::new(
            i32::from_be_bytes(bytes[0..4].try_into().unwrap()),
            i32::from_be_bytes(bytes[4..8].try_into().unwrap()),
        ))
    }

    pub fn to_le_bytes(&self) -> [u8; 8] {
        let mut bytes = [0; 8];
        bytes[0..4].copy_from_slice(&self.0.numer().to_le_bytes());
        bytes[4..8].copy_from_slice(&self.0.denom().to_le_bytes());
        bytes
    }

    pub fn to_be_bytes(&self) -> [u8; 8] {
        let mut bytes = [0; 8];
        bytes[0..4].copy_from_slice(&self.0.numer().to_be_bytes());
        bytes[4..8].copy_from_slice(&self.0.denom().to_be_bytes());
        bytes
    }
}

impl std::fmt::Debug for R64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for R64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for R64 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for R64 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map(R64)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct R128(Ratio<i64>);

impl R128 {
    pub fn new() -> Self {
        Self(Ratio::new_raw(0, 1))
    }

    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        Self(Ratio::new(
            i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        ))
    }

    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(Ratio::new(
            i64::from_be_bytes(bytes[0..8].try_into().unwrap()),
            i64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        ))
    }

    pub fn to_le_bytes(&self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&self.0.numer().to_le_bytes());
        bytes[8..16].copy_from_slice(&self.0.denom().to_le_bytes());
        bytes
    }

    pub fn to_be_bytes(&self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&self.0.numer().to_be_bytes());
        bytes[8..16].copy_from_slice(&self.0.denom().to_be_bytes());
        bytes
    }
}

impl std::fmt::Debug for R128 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for R128 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for R128 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for R128 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map(R128)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}
