#[derive(Debug, Clone)]
pub struct MemGraph {
    id: oid::O32,
    graph: DiGraphMap<CellId, ()>,
    tables: HashSet<oid::O32>,
    columns: HashSet<ColumnId>,
}

impl MemGraph {
    pub fn new() -> Self {
        Self {
            id: oid::O32::default(),
            graph: DiGraphMap::new(),
            tables: HashSet::new(),
            columns: HashSet::new(),
        }
    }

    pub fn id(&self) -> oid::O32 {
        self.id
    }

    pub fn add_vertex(&mut self, value: CellId) -> Result<()> {
        if self.graph.contains_node(value) {
            anyhow::bail!("node already exists");
        }

        self.graph.add_node(value);

        Ok(())
    }

    pub fn add_edge(&mut self, from: CellId, to: CellId) -> Result<()> {
        if !self.graph.contains_node(from) || !self.graph.contains_node(to) {
            anyhow::bail!("invalid node");
        }

        self.graph.add_edge(from, to, ());

        Ok(())
    }
}

impl std::ops::Deref for MemGraph {
    type Target = DiGraphMap<CellId, ()>;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl std::ops::DerefMut for MemGraph {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.graph
    }
}
