use std::ptr;

use anyhow::Result;

use crate::number::{NumKind, Number};

#[derive(Default, Clone, Copy)]
#[repr(transparent)]
pub struct Float(f64);

impl Float {
    pub fn new(value: f64) -> Self {
        Self(value)
    }

    pub unsafe fn from_array(data: [u8; 8]) -> Self {
        Self(f64::from_ne_bytes(data))
    }

    pub fn into_array(self) -> [u8; 8] {
        self.0.to_ne_bytes()
    }

    pub fn try_from_number<T: Number>(value: T) -> Result<Self> {
        unsafe {
            match T::KIND {
                NumKind::I8 => Ok(Self(value.assume_i8() as f64)),
                NumKind::I16 => Ok(Self(value.assume_i16() as f64)),
                NumKind::I32 => Ok(Self(value.assume_i32() as f64)),
                NumKind::U8 => Ok(Self(value.assume_u8() as f64)),
                NumKind::U16 => Ok(Self(value.assume_u16() as f64)),
                NumKind::U32 => Ok(Self(value.assume_u32() as f64)),
                NumKind::F32 => Ok(Self(value.assume_f32() as f64)),
                NumKind::F64 => Ok(Self(value.assume_f64())),
                NumKind::I64 => {
                    let value = value.assume_i64();
                    if value > f64::MAX as i64 || value < f64::MIN as i64 {
                        Err(anyhow::anyhow!("value out of range"))
                    } else {
                        Ok(Self(value as f64))
                    }
                }
                NumKind::I128 => {
                    let value = value.assume_i128();
                    if value > f64::MAX as i128 || value < f64::MIN as i128 {
                        Err(anyhow::anyhow!("value out of range"))
                    } else {
                        Ok(Self(value as f64))
                    }
                }
                NumKind::ISize => {
                    let value = value.assume_isize();
                    if value > f64::MAX as isize || value < f64::MIN as isize {
                        Err(anyhow::anyhow!("value out of range"))
                    } else {
                        Ok(Self(value as f64))
                    }
                }
                NumKind::U64 => {
                    let value = value.assume_u64();
                    if value > f64::MAX as u64 {
                        Err(anyhow::anyhow!("value out of range"))
                    } else {
                        Ok(Self(value as f64))
                    }
                }
                NumKind::U128 => {
                    let value = value.assume_u128();
                    if value > f64::MAX as u128 {
                        Err(anyhow::anyhow!("value out of range"))
                    } else {
                        Ok(Self(value as f64))
                    }
                }
                NumKind::USize => {
                    let value = value.assume_usize();
                    if value > f64::MAX as usize {
                        Err(anyhow::anyhow!("value out of range"))
                    } else {
                        Ok(Self(value as f64))
                    }
                }
            }
        }
    }

    pub fn try_from_str(value: &str) -> Result<Self> {
        let value = value.parse::<f64>()?;
        Ok(Self(value))
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            Err(anyhow::anyhow!("Slice is not 8 bytes long"))
        } else {
            Ok(unsafe { Self::from_slice_unchecked(bytes) })
        }
    }

    pub unsafe fn from_slice_unchecked(bytes: &[u8]) -> Self {
        let mut data = [0; 8];
        ptr::copy_nonoverlapping(bytes.as_ptr(), (&mut data[..]).as_mut_ptr(), 8);
        Self::from_array(data)
    }

    pub fn as_i128(self) -> Result<i128> {
        if self.0.is_infinite() || self.0.is_nan() {
            Err(anyhow::anyhow!("Value is not a finite number"))
        } else {
            Ok(self.0 as i128)
        }
    }

    pub fn as_f64(self) -> f64 {
        self.0
    }
}

impl std::ops::Deref for Float {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Float {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl AsRef<f64> for Float {
    fn as_ref(&self) -> &f64 {
        &self.0
    }
}

impl AsMut<f64> for Float {
    fn as_mut(&mut self) -> &mut f64 {
        &mut self.0
    }
}

impl std::fmt::Debug for Float {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for Float {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for Float {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_f64(self.0)
    }
}

impl<'de> serde::Deserialize<'de> for Float {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let f = f64::deserialize(deserializer)?;
        Ok(Self(f))
    }
}

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() || other.0.is_nan() {
            false
        } else if self.0.is_infinite() || other.0.is_infinite() {
            false
        } else {
            self.0.to_bits() == other.0.to_bits()
        }
    }
}

impl Eq for Float {}

impl PartialOrd for Float {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let a = self.0;
        let b = other.0;

        if a.is_nan() && b.is_nan() {
            Some(std::cmp::Ordering::Equal)
        } else if a.is_nan() {
            Some(std::cmp::Ordering::Less)
        } else if b.is_nan() {
            Some(std::cmp::Ordering::Greater)
        } else if a.is_infinite() && b.is_infinite() {
            Some(a.partial_cmp(&b).unwrap())
        } else if a.is_infinite() {
            if a.is_sign_positive() {
                Some(std::cmp::Ordering::Greater)
            } else {
                Some(std::cmp::Ordering::Less)
            }
        } else if b.is_infinite() {
            if b.is_sign_positive() {
                Some(std::cmp::Ordering::Less)
            } else {
                Some(std::cmp::Ordering::Greater)
            }
        } else {
            a.partial_cmp(&b)
        }
    }
}

impl Ord for Float {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl std::hash::Hash for Float {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state)
    }
}
