use std::{cell::LazyCell, hash::RandomState};

use anyhow::Result;
use petgraph::{
    graphmap::{GraphMap, NodeTrait},
    Directed, EdgeType, Undirected,
};
use serde::{Deserialize, Serialize};

use crate::{sealed::GlobalRecycler, shared_object::SharedObject, Recycler};

pub mod shared;

crate::new_global_recycler!(GraphRecycler);

pub type DiGraph<N, E> = Graph<N, E, Directed>;

pub type UnGraph<N, E> = Graph<N, E, Undirected>;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn into_shared(self) -> shared::SharedGraph<N, E, Ty>
    where
        N: shared::SafeNodeTrait,
        E: shared::SafeEdgeTrait,
        Ty: shared::SafeTypeTrait,
    {
        shared::SharedGraph(SharedObject::new(self))
    }
}
