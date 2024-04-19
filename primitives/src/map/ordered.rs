use std::{cell::LazyCell, collections::BTreeMap};

use anyhow::Result;
use serde::Serialize;

use crate::{sealed::GlobalRecycler, shared_object::SharedObject, Recycler};

pub mod shared;

crate::new_global_recycler!(OrdMapRecycler);

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct OrdMap<K, V>(BTreeMap<K, V, OrdMapRecycler>);

impl<K, V> GlobalRecycler for OrdMap<K, V> {
    fn recycler() -> Recycler {
        OrdMapRecycler::recycler()
    }
}

impl<K, V> std::ops::Deref for OrdMap<K, V> {
    type Target = BTreeMap<K, V, OrdMapRecycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> std::ops::DerefMut for OrdMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> OrdMap<K, V> {
    pub fn new() -> Self {
        Self(BTreeMap::new_in(OrdMapRecycler))
    }

    pub fn into_shared(self) -> shared::SharedOrdMap<K, V>
    where
        K: Send + Sync,
        V: Send + Sync,
    {
        shared::SharedOrdMap(SharedObject::new(self))
    }
}

impl<K: Serialize, V: Serialize> Serialize for OrdMap<K, V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(self.len()))?;

        for (k, v) in self.iter() {
            map.serialize_entry(k, v)?;
        }

        map.end()
    }
}
