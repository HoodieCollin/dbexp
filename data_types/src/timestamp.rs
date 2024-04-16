use std::ops;

use anyhow::Result;
use chrono::{DateTime, Utc};

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    pub fn new() -> Self {
        Self(Utc::now())
    }

    pub fn from_le_bytes(bytes: [u8; 8]) -> Result<Self> {
        if let Some(timestamp) = DateTime::from_timestamp_millis(i64::from_le_bytes(bytes)) {
            Ok(Self(timestamp))
        } else {
            anyhow::bail!("invalid timestamp")
        }
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Result<Self> {
        if let Some(timestamp) = DateTime::from_timestamp_millis(i64::from_be_bytes(bytes)) {
            Ok(Self(timestamp))
        } else {
            anyhow::bail!("invalid timestamp")
        }
    }

    pub fn to_le_bytes(&self) -> [u8; 8] {
        self.0.timestamp_millis().to_le_bytes()
    }

    pub fn to_be_bytes(&self) -> [u8; 8] {
        self.0.timestamp_millis().to_be_bytes()
    }

    pub fn to_integer(&self) -> i64 {
        self.0.timestamp_millis()
    }

    pub fn from_integer(timestamp: i64) -> Result<Self> {
        if let Some(timestamp) = DateTime::from_timestamp_millis(timestamp) {
            Ok(Self(timestamp))
        } else {
            anyhow::bail!("invalid timestamp")
        }
    }
}

impl ops::Deref for Timestamp {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ops::DerefMut for Timestamp {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%d %H:%M:%S"))
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.format("%Y-%m-%d %H:%M:%S"))
    }
}

impl serde::Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_rfc3339())
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|d| Self(d.to_utc()))
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}
