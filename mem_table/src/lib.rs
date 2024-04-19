#![feature(lazy_cell)]
#![feature(allocator_api)]

use anyhow::Result;
use data_types::{ExpectedType, Loader};
use im::HashSet;
use parking_lot::RwLock;
use primitives::buffer::Buffer;
use primitives::map::Map;
use primitives::set::Set;
use primitives::typed_arc::TypedArc;

use crate::cell::{Cell, CellKey, CellPool};
use crate::object_ids::{ColumnId, RecordId, TableId};

pub mod cell;
pub mod graph;
pub mod object_ids;

struct TableInner {
    cell_pool: CellPool,
    columns: HashSet<ColumnId>,
    records: HashSet<RecordId>,
}

pub struct Table(TypedArc<(TableId, RwLock<TableInner>)>);

impl Table {
    pub fn new(
        cell_pool: &CellPool,
        columns: impl IntoIterator<Item = impl Into<ExpectedType>>,
    ) -> Self {
        let id = TableId::new();

        Self(TypedArc::new((
            id,
            RwLock::new(TableInner {
                cell_pool: cell_pool.clone(),
                columns: columns
                    .into_iter()
                    .map(|kind| ColumnId::new(id, kind))
                    .collect(),
                records: HashSet::new(),
            }),
        )))
    }

    pub fn id(&self) -> TableId {
        self.0 .0
    }

    pub fn columns(&self) -> HashSet<ColumnId> {
        self.0 .1.read().columns.clone()
    }

    pub fn records(&self) -> Set<Record> {
        let guard = self.0 .1.read();

        guard
            .records
            .iter()
            .map(|record_id| Record {
                id: *record_id,
                columns: guard.columns.clone(),
                cell_pool: guard.cell_pool.clone(),
            })
            .collect()
    }

    pub fn load_data<T, U>(&self, columns: T) -> Result<()>
    where
        T: IntoIterator<Item = (ColumnId, U)>,
        U: AsRef<[u8]>,
    {
        let table_id = self.0 .0;
        let mut inner = self.0 .1.write();

        let mut columns = columns
            .into_iter()
            .map(|(column_id, v)| {
                if column_id.table() != table_id {
                    anyhow::bail!("Column does not belong to this table");
                }

                Ok((column_id, Loader::new(column_id.kind(), v)?))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut pending_cells = Buffer::with_capacity(columns.len() as u32);

        loop {
            let record_id = RecordId::new(table_id);
            let mut columns_depleted = 0;

            for (column_id, loader) in &mut columns {
                let column_id = *column_id;
                let value = loader.try_next()?;

                if value.is_none() {
                    columns_depleted += 1;
                }

                pending_cells.push((column_id, record_id, value));
            }

            if columns_depleted == columns.len() {
                break;
            }

            for (column_id, record_id, value) in pending_cells.drain(..) {
                inner.cell_pool.new_cell(column_id, record_id, value)?;
            }

            inner.records.insert(record_id);
        }

        Ok(())
    }

    pub fn get_cell(&self, key: impl CellKey) -> Option<Cell> {
        let inner = self.0 .1.read();
        inner.cell_pool.get(key)
    }
}

#[derive(Clone)]
pub struct Record {
    id: RecordId,
    columns: HashSet<ColumnId>,
    cell_pool: CellPool,
}

impl Record {
    pub fn id(&self) -> RecordId {
        self.id
    }

    pub fn get_cell(&self, column_id: ColumnId) -> Result<Cell> {
        if self.columns.contains(&column_id) {
            if let Some(cell) = self.cell_pool.get((column_id, self.id)) {
                Ok(cell)
            } else {
                panic!("Cell not found");
            }
        } else {
            anyhow::bail!("Column does not belong to this record");
        }
    }

    pub fn get_cell_map(&self) -> Map<ColumnId, Cell> {
        self.columns
            .iter()
            .filter_map(|column_id| {
                self.cell_pool
                    .get((*column_id, self.id))
                    .map(|cell| (column_id.clone(), cell))
            })
            .collect()
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Record {}

impl std::hash::Hash for Record {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !f.alternate() {
            std::fmt::Debug::fmt(&self.id, f)
        } else {
            f.debug_struct("Record")
                .field("id", &self.id)
                .field("fields", &self.get_cell_map())
                .finish()
        }
    }
}

#[cfg(test)]
mod test {
    use data_types::{number::IntSize, DataType};

    use super::*;

    #[test]
    fn test_table() -> Result<()> {
        let cell_pool = CellPool::new();

        let table = Table::new(
            &cell_pool,
            vec![
                DataType::Integer(IntSize::X8),
                DataType::Integer(IntSize::X8),
            ],
        );

        let (col_1, col_2) = {
            let columns = table.columns();
            let mut columns = columns.iter().copied();
            (columns.next().unwrap(), columns.next().unwrap())
        };

        table.load_data(vec![(col_1, [1u8, 3, 5]), (col_2, [2u8, 4, 6])])?;

        let records = table.records();

        for record in records {
            println!("Record {:#?}", record);
        }

        Ok(())
    }
}
