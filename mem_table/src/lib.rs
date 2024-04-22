#![feature(lazy_cell)]
#![feature(allocator_api)]

use anyhow::Result;
use data_types::{DataValue, ExpectedType};
use im::HashSet;
use parking_lot::RwLock;
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

#[derive(Clone)]
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

    pub fn create_column(&self, kind: impl Into<ExpectedType>) -> Result<ColumnId> {
        let table_id = self.0 .0;
        let mut inner = self.0 .1.write();

        let new_id = ColumnId::new(table_id, kind);

        if inner.columns.contains(&new_id) {
            anyhow::bail!("Column already exists");
        }

        inner.columns.insert(new_id);

        Ok(new_id)
    }

    pub fn load_data(&self, column: ColumnId, data: impl AsRef<[u8]>) -> Result<()> {
        let table_id = self.0 .0;
        let mut inner = self.0 .1.write();

        if column.table() != table_id {
            anyhow::bail!("Column does not belong to this table");
        }

        let mut loader = Loader::new(column.kind(), data.as_ref())?;

        while let Some((head, value)) = loader.try_next()? {
            let record_id = RecordId::from_array(head, table_id);
            inner.cell_pool.new_cell(column, record_id, Some(value))?;
            inner.records.insert(record_id);
        }

        Ok(())
    }

    pub fn get_cell(&self, key: impl CellKey) -> Option<Cell> {
        let inner = self.0 .1.read();
        inner.cell_pool.get(key)
    }
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !f.alternate() {
            f.debug_struct("Table")
                .field("columns", &self.columns())
                .finish()
        } else {
            f.debug_struct("Table")
                .field("id", &self.0 .0)
                .field("columns", &self.columns())
                .finish()
        }
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
            f.debug_struct("Record")
                .field("fields", &self.get_cell_map())
                .finish()
        } else {
            f.debug_struct("Record")
                .field("id", &self.id)
                .field("fields", &self.get_cell_map())
                .finish()
        }
    }
}

pub struct Loader<T: AsRef<[u8]>> {
    src: T,
    kind: ExpectedType,
    index: usize,
}

impl<T: AsRef<[u8]>> Loader<T> {
    pub fn new(kind: impl Into<ExpectedType>, src: T) -> Result<Self> {
        let kind: ExpectedType = kind.into();
        let src_len = src.as_ref().len();

        if src_len % kind.byte_count() != 0 {
            anyhow::bail!("buffer is not divisible by the size of intended type")
        }

        Ok(Self {
            src,
            kind: kind.into(),
            index: 0,
        })
    }

    pub fn set_index(&mut self, index: usize) {
        self.index = index;
    }

    pub fn skip(&mut self, count: usize) {
        self.index += count;
    }

    pub fn rewind(&mut self, count: usize) {
        self.index -= count;
    }

    pub fn try_next(&mut self) -> Result<Option<([u8; 4], DataValue)>> {
        let head_len = 4;
        let body_len = self.kind.byte_count();
        let data_len = head_len + body_len;

        let nil_loc = self.index * data_len;
        let head_start = nil_loc + 1;
        let body_start = head_start + head_len;
        let byte_after = nil_loc + data_len;

        let src_len = self.src.as_ref().len();

        if nil_loc >= src_len {
            return Ok(None);
        }

        self.index += 1;

        let data = self.src.as_ref() as *const [u8];
        let mut head = [0; 4];

        unsafe {
            std::ptr::copy_nonoverlapping(
                (&*data)[head_start..body_start].as_ptr(),
                head.as_mut_ptr(),
                4,
            );

            if (&*data)[nil_loc] == 0 {
                Ok(Some((head, DataValue::Nil(self.kind))))
            } else {
                Ok(Some((
                    head,
                    DataValue::try_from_any(self.kind, &(&*data)[body_start..byte_after])?,
                )))
            }
        }
    }
}

pub struct Unloader<'a, T: AsMut<[u8]> + 'a, U: IntoIterator<Item = &'a DataValue>> {
    dest: T,
    src: U::IntoIter,
    index: usize,
}

impl<'a, T: AsMut<[u8]> + 'a, U: IntoIterator<Item = &'a DataValue>> Unloader<'a, T, U> {
    pub fn new(dest: T, src: U) -> Self {
        Self {
            dest,
            src: src.into_iter(),
            index: 0,
        }
    }

    pub fn set_index(&mut self, index: usize) {
        self.index = index;
    }

    pub fn skip(&mut self, count: usize) {
        self.index += count;
    }

    pub fn rewind(&mut self, count: usize) {
        self.index -= count;
    }

    pub fn try_next(&mut self) -> Result<Option<()>> {
        let value = self.src.next();

        if let Some(value) = value {
            let count = value.get_type().byte_count();
            let nil_byte = self.index * count;
            let start = nil_byte + 1;
            let end = nil_byte + count;

            let dest = self.dest.as_mut();

            if count > dest.len() {
                anyhow::bail!("buffer is too small")
            }

            if nil_byte == dest.len() {
                return Ok(None);
            }

            let dest = &mut dest[nil_byte..end];

            self.index += 1;

            value.write_to(dest)?;

            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    pub fn all(&mut self) -> Result<()> {
        while self.try_next()?.is_some() {}

        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use data_types::{number::IntSize, DataType};

//     use super::*;

//     #[test]
//     fn test_table() -> Result<()> {
//         let cell_pool = CellPool::new();

//         let table = Table::new(
//             &cell_pool,
//             vec![
//                 DataType::Integer(IntSize::X8),
//                 DataType::Integer(IntSize::X8),
//             ],
//         );

//         let (col_1, col_2) = {
//             let columns = table.columns();
//             let mut columns = columns.iter().copied();
//             (columns.next().unwrap(), columns.next().unwrap())
//         };

//         table.load_data(vec![(col_1, [1u8, 3, 5]), (col_2, [2u8, 4, 6])])?;

//         let records = table.records();

//         for record in records {
//             println!("Record {:#?}", record);
//         }

//         Ok(())
//     }
// }
