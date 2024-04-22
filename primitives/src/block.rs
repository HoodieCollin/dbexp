use std::cell::LazyCell;

use anyhow::Result;
use serde::Serialize;

use crate::{sealed::GlobalRecycler, Recycler};

crate::new_global_recycler!(BlockRecycler);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Block<T>(Box<T, BlockRecycler>);

impl<T> GlobalRecycler for Block<T> {
    fn recycler() -> Recycler {
        BlockRecycler::recycler()
    }
}

impl<T> std::ops::Deref for Block<T> {
    type Target = Box<T, BlockRecycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for Block<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> Block<T> {
    pub fn new(x: T) -> Self {
        Self(Box::new_in(x, BlockRecycler))
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for Block<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Block<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<T: Serialize> Serialize for Block<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Block<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let x = T::deserialize(deserializer)?;
        Ok(Self::new(x))
    }
}
