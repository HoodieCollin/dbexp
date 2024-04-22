use std::{cell::LazyCell, hash::RandomState};

use anyhow::Result;
use indexmap::IndexMap;
use serde::Serialize;

use crate::{sealed::GlobalRecycler, Recycler};

pub mod ordered;

crate::new_global_recycler!(MapRecycler);

#[derive(Debug, Clone, Serialize)]
#[repr(transparent)]
pub struct Map<K: Eq + std::hash::Hash, V>(IndexMap<K, V, RandomState, MapRecycler>);

impl<K: Eq + std::hash::Hash, V> GlobalRecycler for Map<K, V> {
    fn recycler() -> Recycler {
        MapRecycler::recycler()
    }
}

impl<K: Eq + std::hash::Hash, V> std::ops::Deref for Map<K, V> {
    type Target = IndexMap<K, V, RandomState, MapRecycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Eq + std::hash::Hash, V> std::ops::DerefMut for Map<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K: Eq + std::hash::Hash, V> FromIterator<(K, V)> for Map<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut map = Self::with_capacity(iter.size_hint().0);

        map.extend(iter);
        map
    }
}

impl<K: Eq + std::hash::Hash, V> IntoIterator for Map<K, V> {
    type Item = (K, V);
    type IntoIter = indexmap::map::IntoIter<K, V, MapRecycler>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K: Eq + std::hash::Hash, V> IntoIterator for &'a Map<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = indexmap::map::Iter<'a, K, V, MapRecycler>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, K: Eq + std::hash::Hash, V> IntoIterator for &'a mut Map<K, V> {
    type Item = (&'a K, &'a mut V);
    type IntoIter = indexmap::map::IterMut<'a, K, V, MapRecycler>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<K: Eq + std::hash::Hash, V> Map<K, V> {
    pub fn new() -> Self {
        Self(IndexMap::new_in(MapRecycler))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(IndexMap::with_capacity_and_hasher_in(
            capacity,
            RandomState::default(),
            MapRecycler,
        ))
    }
}

impl<K: Eq + std::hash::Hash, V> Default for Map<K, V> {
    fn default() -> Self {
        Self::new()
    }
}
