use std::{cell::LazyCell, collections::BTreeSet};

use anyhow::Result;
use serde::Serialize;

use crate::{sealed::GlobalRecycler, Recycler};

crate::new_global_recycler!(OrdSetRecycler);

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct OrdSet<T>(BTreeSet<T, OrdSetRecycler>);

impl<T> GlobalRecycler for OrdSet<T> {
    fn recycler() -> Recycler {
        OrdSetRecycler::recycler()
    }
}

impl<T> std::ops::Deref for OrdSet<T> {
    type Target = BTreeSet<T, OrdSetRecycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for OrdSet<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> OrdSet<T> {
    pub fn new() -> Self {
        Self(BTreeSet::new_in(OrdSetRecycler))
    }
}

impl<T: Serialize> Serialize for OrdSet<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;

        let mut seq = serializer.serialize_seq(Some(self.len()))?;

        for item in self.iter() {
            seq.serialize_element(item)?;
        }

        seq.end()
    }
}

impl<T> Default for OrdSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
