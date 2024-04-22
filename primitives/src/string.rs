use std::cell::LazyCell;

use anyhow::Result;
use serde::Serialize;

use crate::{sealed::GlobalRecycler, Recycler};

pub mod fixed;

crate::new_global_recycler!(StringRecycler);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct String(Vec<u8, StringRecycler>);

impl GlobalRecycler for String {
    fn recycler() -> Recycler {
        StringRecycler::recycler()
    }
}

impl std::ops::Deref for String {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl std::ops::DerefMut for String {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_str_mut()
    }
}

impl String {
    pub fn new() -> Self {
        Self(Vec::new_in(StringRecycler))
    }

    pub fn with_capacity(capacity: u32) -> Self {
        Self(Vec::with_capacity_in(capacity as usize, StringRecycler))
    }

    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }

    pub fn capacity(&self) -> u32 {
        self.0.capacity() as u32
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    pub fn as_str_mut(&mut self) -> &mut str {
        unsafe { std::str::from_utf8_unchecked_mut(&mut self.0) }
    }

    pub fn push(&mut self, c: char) {
        let mut buf = [0; 4];
        self.0.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
    }

    pub fn pop(&mut self) -> Option<char> {
        let ch = self.chars().rev().next()?;
        let new_len = self.len() as usize - ch.len_utf8();

        unsafe {
            self.0.set_len(new_len);
        }

        Some(ch)
    }

    pub fn push_str(&mut self, s: &str) {
        self.0.extend_from_slice(s.as_bytes());
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub fn reserve(&mut self, additional: u32) {
        self.0.reserve(additional as usize);
    }

    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }
}

impl std::fmt::Debug for String {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl std::fmt::Display for String {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl std::fmt::Write for String {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        self.push_str(s);
        Ok(())
    }
}

impl Serialize for String {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
    }
}
impl<'de> serde::Deserialize<'de> for String {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StringVisitor;

        impl<'de> serde::de::Visitor<'de> for StringVisitor {
            type Value = String;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                let mut new = String::with_capacity(v.len() as u32);
                new.push_str(v);
                Ok(new)
            }

            fn visit_string<E: serde::de::Error>(
                self,
                v: std::string::String,
            ) -> Result<Self::Value, E> {
                let mut new = String::with_capacity(v.len() as u32);
                new.push_str(&v);
                Ok(new)
            }
        }

        deserializer.deserialize_str(StringVisitor)
    }
}
