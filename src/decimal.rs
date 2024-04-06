use std::ops;

type Dec = rust_decimal::Decimal;

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Decimal(Dec);

impl Decimal {
    pub fn new() -> Self {
        Self(Dec::ZERO)
    }

    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        Self(Dec::deserialize(u128::from_le_bytes(bytes).to_ne_bytes()))
    }

    pub fn from_be_bytes(bytes: [u8; 16]) -> Self {
        Self(Dec::deserialize(u128::from_be_bytes(bytes).to_ne_bytes()))
    }

    pub fn to_le_bytes(&self) -> [u8; 16] {
        u128::from_ne_bytes(self.0.serialize()).to_le_bytes()
    }

    pub fn to_be_bytes(&self) -> [u8; 16] {
        u128::from_ne_bytes(self.0.serialize()).to_be_bytes()
    }
}

impl ops::Deref for Decimal {
    type Target = Dec;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for Decimal {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::fmt::Debug for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for Decimal {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Decimal {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse()
            .map(Decimal)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}
