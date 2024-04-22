use std::{cell::LazyCell, hash::RandomState};

use anyhow::Result;
use petgraph::{
    graphmap::{GraphMap, NodeTrait},
    Directed, EdgeType, Undirected,
};
use serde::{Deserialize, Serialize};

use crate::{sealed::GlobalRecycler, Recycler};

crate::new_global_recycler!(GraphRecycler);

pub type DiGraph<N, E> = Graph<N, E, Directed>;

pub type UnGraph<N, E> = Graph<N, E, Undirected>;

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Graph<N: NodeTrait, E: Clone, Ty: EdgeType + Clone>(
    GraphMap<N, E, Ty, RandomState, GraphRecycler>,
);

impl<N: NodeTrait, E: Clone, Ty: EdgeType + Clone> GlobalRecycler for Graph<N, E, Ty> {
    fn recycler() -> Recycler {
        GraphRecycler::recycler()
    }
}

impl<N: NodeTrait, E: Clone, Ty: EdgeType + Clone> std::ops::Deref for Graph<N, E, Ty> {
    type Target = GraphMap<N, E, Ty, RandomState, GraphRecycler>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<N: NodeTrait, E: Clone, Ty: EdgeType + Clone> std::ops::DerefMut for Graph<N, E, Ty> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<N: NodeTrait, E: Clone, Ty: EdgeType + Clone> Graph<N, E, Ty> {
    pub fn new() -> Self {
        Self(GraphMap::new_in(GraphRecycler))
    }

    pub fn with_capacity(nodes: usize, edges: usize) -> Self {
        Self(GraphMap::with_capacity_and_hasher_in(
            nodes,
            edges,
            RandomState::default(),
            GraphRecycler,
        ))
    }
}

impl<N: NodeTrait, E: Clone, Ty: EdgeType + Clone> Default for Graph<N, E, Ty> {
    fn default() -> Self {
        Self::new()
    }
}

impl<N: NodeTrait + Serialize, E: Clone + Serialize> Serialize for DiGraph<N, E> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, N: NodeTrait + Deserialize<'de>, E: Clone + Deserialize<'de>> Deserialize<'de>
    for DiGraph<N, E>
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let graph =
            GraphMap::<N, E, Directed, RandomState, GraphRecycler>::deserialize(deserializer)?;
        Ok(Self(graph))
    }
}

impl<N: NodeTrait + Serialize, E: Clone + Serialize> Serialize for UnGraph<N, E> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de, N: NodeTrait + Deserialize<'de>, E: Clone + Deserialize<'de>> Deserialize<'de>
    for UnGraph<N, E>
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let graph =
            GraphMap::<N, E, Undirected, RandomState, GraphRecycler>::deserialize(deserializer)?;
        Ok(Self(graph))
    }
}
