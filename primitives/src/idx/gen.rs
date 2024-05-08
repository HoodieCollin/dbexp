use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    byte_encoding::{ByteEncoder, IntoBytes, ScalarFromBytes},
    O16,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Gen(O16);

impl std::fmt::Debug for Gen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Gen({:?})", &self.0)
    }
}

impl std::fmt::Display for Gen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

crate::impl_access_bytes_for_into_bytes_type!(Gen);

impl IntoBytes for Gen {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode_bytes(&self.into_array())
    }
}

impl ScalarFromBytes for Gen {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Self::try_from_array(bytes)
    }
}

impl ScalarFromBytes for Option<Gen> {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        match bytes.try_into() {
            Ok(arr) => Ok(Gen::from_array(arr)),
            Err(_) => Ok(None),
        }
    }
}

impl Gen {
    pub const INVALID: Self = Self(O16::INVALID);
    pub const NIL: Option<Self> = None;

    pub fn new() -> Self {
        Self(O16::new())
    }

    pub fn into_array(&self) -> [u8; 2] {
        self.0.into_array()
    }

    pub fn from_array(arr: [u8; 2]) -> Option<Self> {
        Some(Self(O16::from_array(arr)?))
    }

    pub fn try_from_array(arr: impl TryInto<[u8; 2]>) -> Result<Self> {
        Ok(Self(O16::try_from_array(arr)?))
    }

    pub fn into_raw(self) -> O16 {
        self.0
    }

    pub fn from_raw(raw: O16) -> Self {
        Self(raw)
    }
}
