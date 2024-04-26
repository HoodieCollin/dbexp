use std::ops;

use anyhow::Result;
use chrono::{
    format::{DelayedFormat, StrftimeItems},
    DateTime, Utc,
};

use crate::number;

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    pub fn new() -> Self {
        Self(Utc::now())
    }

    // Not actually unsafe, but to conform with the other from_array methods
    pub unsafe fn from_array(bytes: [u8; 8]) -> Self {
        if let Some(timestamp) = DateTime::from_timestamp_millis(i64::from_ne_bytes(bytes)) {
            Self(timestamp)
        } else {
            panic!("invalid timestamp")
        }
    }

    pub fn into_array(&self) -> [u8; 8] {
        self.0.timestamp_millis().to_ne_bytes()
    }

    pub fn as_i128(&self) -> i128 {
        self.0.timestamp_millis() as i128
    }

    pub fn try_from_number<T: number::Builtin>(value: T) -> Result<Self> {
        if let Some(timestamp) = DateTime::from_timestamp_millis(value.as_i64()?) {
            Ok(Self(timestamp))
        } else {
            anyhow::bail!("invalid timestamp")
        }
    }

    pub fn try_from_str(value: &str) -> Result<Self> {
        DateTime::parse_from_rfc3339(value)
            .map(|d| Self(d.with_timezone(&Utc)))
            .map_err(|e| e.into())
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            anyhow::bail!("invalid timestamp length");
        }

        let mut buf = [0; 8];
        buf.copy_from_slice(bytes);
        Ok(unsafe { Self::from_array(buf) })
    }

    pub fn as_str(&self) -> DelayedFormat<StrftimeItems> {
        self.0.format("%d/%m/%Y %H:%M")
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
