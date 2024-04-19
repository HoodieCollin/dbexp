use anyhow::Result;
use petgraph::{graphmap::NodeTrait, Directed, EdgeType, Undirected};
use serde::{Deserialize, Serialize};

use crate::shared_object::{SharedObject, SharedObjectMut, SharedObjectRef};

use super::Graph;

pub trait SafeNodeTrait: NodeTrait + Send + Sync {}

impl<T: NodeTrait + Send + Sync> SafeNodeTrait for T {}

pub trait SafeEdgeTrait: Clone + Send + Sync {}

impl<T: Clone + Send + Sync> SafeEdgeTrait for T {}

pub trait SafeTypeTrait: EdgeType + Clone + Send + Sync {}

impl<T: EdgeType + Clone + Send + Sync> SafeTypeTrait for T {}

pub type SharedDiGraph<N, E> = SharedGraph<N, E, Directed>;

pub type SharedUnGraph<N, E> = SharedGraph<N, E, Undirected>;

#[derive(Deserialize)]
#[repr(transparent)]
pub struct SharedGraph<
    N: SafeNodeTrait + 'static,
    E: SafeEdgeTrait + 'static,
    Ty: SafeTypeTrait + 'static,
>(pub(super) SharedObject<Graph<N, E, Ty>>);

impl<N: SafeNodeTrait, E: SafeEdgeTrait, Ty: SafeTypeTrait> SharedGraph<N, E, Ty> {
    pub fn new() -> Self {
        Self(SharedObject::new(Graph::new()))
    }

    pub fn with_capacity(nodes: usize, edges: usize) -> Self {
        Self(SharedObject::new(Graph::with_capacity(nodes, edges)))
    }

    pub fn new_copy(&self) -> Self {
        self.0
            .read_with(|inner| Self(SharedObject::new(inner.clone())))
    }

    pub fn unwrap_or_clone(self) -> Graph<N, E, Ty> {
        self.0.unwrap_or_clone()
    }

    pub fn try_unwrap(self) -> Result<Graph<N, E, Ty>, Self> {
        match SharedObject::try_unwrap(self.0) {
            Ok(inner) => Ok(inner),
            Err(inner) => Err(Self(inner)),
        }
    }

    pub fn node_count(&self) -> usize {
        self.0.read_with(|inner| inner.node_count())
    }

    pub fn edge_count(&self) -> usize {
        self.0.read_with(|inner| inner.edge_count())
    }

    pub fn capacity(&self) -> (usize, usize) {
        self.0.read_with(|inner| inner.capacity())
    }

    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Graph<N, E, Ty>) -> R,
    {
        self.0.read_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Graph<N, E, Ty>) -> R,
    {
        self.0.read_recursive_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn read_guard(&self) -> SharedObjectRef<Graph<N, E, Ty>> {
        self.0.read_guard()
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Graph<N, E, Ty>) -> R,
    {
        self.0.write_with(f)
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn write_guard(&self) -> SharedObjectMut<Graph<N, E, Ty>> {
        self.0.write_guard()
    }
}

impl<N: Clone + SafeNodeTrait, E: SafeEdgeTrait, Ty: SafeTypeTrait> Clone
    for SharedGraph<N, E, Ty>
{
    fn clone(&self) -> Self {
        Self(SharedObject::new(self.0.read_with(|inner| inner.clone())))
    }
}

impl<
        N: std::fmt::Debug + SafeNodeTrait,
        E: std::fmt::Debug + SafeEdgeTrait,
        Ty: std::fmt::Debug + SafeTypeTrait,
    > std::fmt::Debug for SharedGraph<N, E, Ty>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.read_recursive_with(|inner| write!(f, "{:?}", inner))
    }
}

impl<N: Serialize + SafeNodeTrait, E: Serialize + SafeEdgeTrait, Ty: SafeTypeTrait> Serialize
    for SharedGraph<N, E, Ty>
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.read_recursive_with(|inner| inner.serialize(serializer))
    }
}
