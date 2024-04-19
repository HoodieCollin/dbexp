use primitives::graph::shared::SharedDiGraph;
use serde::Serialize;

use crate::object_ids::RecordId;

#[derive(Debug, Clone, Serialize)]
pub struct RecordGraph(SharedDiGraph<RecordId, RecordId>);
