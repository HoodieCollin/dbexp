use primitives::graph::DiGraph;
use serde::{Deserialize, Serialize};

use crate::object_ids::RecordId;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RecordGraph(DiGraph<RecordId, RecordId>);

impl RecordGraph {
    pub fn new() -> Self {
        Self(DiGraph::new())
    }
}
