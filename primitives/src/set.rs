use std::{cell::LazyCell, hash::RandomState};

use anyhow::Result;
use indexmap::IndexSet;
use serde::Serialize;

use crate::{sealed::GlobalRecycler, shared_object::SharedObject, Recycler};

pub mod ordered;
pub mod shared;

crate::new_global_recycler!(SetRecycler);

#[derive(Debug, Clone, Serialize)]
pub struct Set<T: Eq + std::hash::Hash>(IndexSet<T, RandomState, SetRecycler>);

impl<T: Eq + std::hash::Hash> GlobalRecycler for Set<T> {
    fn recycler() -> Recycler {
        SetRecycler::recycler()
    }
}

impl<T: Eq + std::hash::Hash> std::ops::Deref for Set<T> {
    type Target = IndexSet<T, RandomState, SetRecycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Eq + std::hash::Hash> std::ops::DerefMut for Set<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Eq + std::hash::Hash> FromIterator<T> for Set<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut set = Self::with_capacity(iter.size_hint().0);

        set.extend(iter);
        set
    }
}

impl<T: Eq + std::hash::Hash> IntoIterator for Set<T> {
    type Item = T;
    type IntoIter = indexmap::set::IntoIter<T, SetRecycler>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, T: Eq + std::hash::Hash> IntoIterator for &'a Set<T> {
    type Item = &'a T;
    type IntoIter = indexmap::set::Iter<'a, T, SetRecycler>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: Eq + std::hash::Hash> Set<T> {
    pub fn new() -> Self {
        Self(IndexSet::new_in(SetRecycler))
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(IndexSet::with_capacity_and_hasher_in(
            capacity,
            RandomState::default(),
            SetRecycler,
        ))
    }

    pub fn into_shared(self) -> shared::SharedSet<T>
    where
        T: Send + Sync,
    {
        shared::SharedSet(SharedObject::new(self))
    }
}
