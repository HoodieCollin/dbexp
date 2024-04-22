use std::{cell::LazyCell, hash::RandomState};

use anyhow::Result;
use indexmap::IndexSet;
use serde::Serialize;

use crate::{sealed::GlobalRecycler, Recycler};

pub mod ordered;

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
}

impl<T: Eq + std::hash::Hash> Default for Set<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'de, T: Eq + std::hash::Hash + serde::Deserialize<'de>> serde::Deserialize<'de> for Set<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SetVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T: Eq + std::hash::Hash + serde::Deserialize<'de>> serde::de::Visitor<'de>
            for SetVisitor<T>
        {
            type Value = Set<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a set")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut set = Set::new();
                seq.size_hint().map(|size| set.reserve(size));

                while let Some(value) = seq.next_element()? {
                    set.insert(value);
                }

                Ok(set)
            }
        }

        deserializer.deserialize_seq(SetVisitor(std::marker::PhantomData))
    }
}
