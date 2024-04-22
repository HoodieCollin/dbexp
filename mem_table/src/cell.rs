use std::{any::type_name, mem::ManuallyDrop};

use anyhow::Result;
use data_types::{DataValue, ExpectedType};
use im::HashMap;

use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use primitives::typed_arc::TypedArc;
use sharded_slab::Slab;

use crate::object_ids::{CellId, ColumnId, RecordId, TableId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellMeta {
    id: CellId,
    column: ColumnId,
    record: RecordId,
}

impl CellMeta {
    pub fn new(id: CellId, column: ColumnId, record: RecordId) -> Result<Self> {
        if column.table() != record.table() {
            anyhow::bail!("column and record must be in the same table");
        }

        Ok(Self { id, column, record })
    }

    pub fn id(&self) -> CellId {
        self.id
    }

    pub fn column(&self) -> ColumnId {
        self.column
    }

    pub fn record(&self) -> RecordId {
        self.record
    }

    pub fn table(&self) -> TableId {
        self.column.table()
    }

    pub fn kind(&self) -> ExpectedType {
        self.column.kind()
    }
}

pub struct CellInner {
    meta: CellMeta,
    pool: CellPool,
    value: RwLock<DataValue>,
}

impl Clone for CellInner {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            pool: self.pool.clone(),
            value: RwLock::new(self.value.read().clone()),
        }
    }
}

impl CellInner {
    pub fn meta(&self) -> &CellMeta {
        &self.meta
    }

    pub fn read(&self) -> RwLockReadGuard<'_, DataValue> {
        self.value.read()
    }

    pub fn read_recursive(&self) -> RwLockReadGuard<'_, DataValue> {
        self.value.read_recursive()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, DataValue> {
        self.value.write()
    }
}

impl Drop for CellInner {
    fn drop(&mut self) {
        {
            let mut lookup = self.pool.0.lookup.write();
            lookup.remove(&(self.meta.column(), self.meta.record()));
        }

        {
            let cells = &self.pool.0.cells;
            cells.remove(self.meta.id().as_usize());
        }
    }
}

pub struct Cell(TypedArc<CellInner>);

impl Cell {
    pub fn id(&self) -> CellId {
        self.0.meta.id()
    }

    pub fn column(&self) -> ColumnId {
        self.0.meta.column()
    }

    pub fn record(&self) -> RecordId {
        self.0.meta.record()
    }

    pub fn table(&self) -> TableId {
        self.0.meta.table()
    }

    pub fn kind(&self) -> ExpectedType {
        self.0.meta.kind()
    }

    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&DataValue) -> R,
    {
        f(&*self.0.read())
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&DataValue) -> R,
    {
        f(&*self.0.read_recursive())
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn read_guard(&self) -> CellRef {
        union Transmute<'a> {
            from: ManuallyDrop<RwLockReadGuard<'a, DataValue>>,
            to: ManuallyDrop<RwLockReadGuard<'static, DataValue>>,
        }

        let guard = ManuallyDrop::new(self.0.read());
        let transmute = Transmute { from: guard };

        CellRef(
            TypedArc::clone(&self.0),
            ManuallyDrop::into_inner(unsafe { transmute.to }),
        )
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut DataValue) -> R,
    {
        f(&mut *self.0.write())
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn write_guard(&self) -> CellMut {
        union Transmute<'a> {
            from: ManuallyDrop<RwLockWriteGuard<'a, DataValue>>,
            to: ManuallyDrop<RwLockWriteGuard<'static, DataValue>>,
        }

        let guard = ManuallyDrop::new(self.0.write());
        let transmute = Transmute { from: guard };

        CellMut(
            TypedArc::clone(&self.0),
            ManuallyDrop::into_inner(unsafe { transmute.to }),
        )
    }
}

impl Clone for Cell {
    fn clone(&self) -> Self {
        Self(TypedArc::clone(&self.0))
    }
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        self.0.meta.column() == other.0.meta.column()
            && self.0.meta.record() == other.0.meta.record()
    }
}

impl Eq for Cell {}

impl std::fmt::Debug for Cell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.0.value.read();

        if !f.alternate() {
            f.debug_tuple("Cell").field(&guard).finish()
        } else {
            f.debug_struct("Cell")
                .field("meta", &self.0.meta)
                .field("value", &guard)
                .finish()
        }
    }
}

/// A `RwLockReadGuard` that dereferences to a `DataValue`. The `Cell` is kept alive by an owned `TypedArc` so there is no lifetime component.
pub struct CellRef(TypedArc<CellInner>, RwLockReadGuard<'static, DataValue>);

impl std::ops::Deref for CellRef {
    type Target = DataValue;

    fn deref(&self) -> &Self::Target {
        &*self.1
    }
}

impl AsRef<DataValue> for CellRef {
    fn as_ref(&self) -> &DataValue {
        &*self.1
    }
}

impl AsRef<CellMeta> for CellRef {
    fn as_ref(&self) -> &CellMeta {
        &self.0.meta
    }
}

/// A `RwLockWriteGuard` that dereferences to a `DataValue`. The `Cell` is kept alive by an owned `TypedArc` so there is no lifetime component.
pub struct CellMut(TypedArc<CellInner>, RwLockWriteGuard<'static, DataValue>);

impl std::ops::Deref for CellMut {
    type Target = DataValue;

    fn deref(&self) -> &Self::Target {
        &*self.1
    }
}

impl std::ops::DerefMut for CellMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.1
    }
}

impl AsRef<DataValue> for CellMut {
    fn as_ref(&self) -> &DataValue {
        &*self.1
    }
}

impl AsMut<DataValue> for CellMut {
    fn as_mut(&mut self) -> &mut DataValue {
        &mut *self.1
    }
}

impl AsRef<CellMeta> for CellMut {
    fn as_ref(&self) -> &CellMeta {
        &self.0.meta
    }
}

pub enum CellKeyKind {
    Id,
    Tuple,
}

pub trait CellKey: std::fmt::Debug + Sized {
    const KIND: CellKeyKind;

    fn kind(&self) -> CellKeyKind {
        Self::KIND
    }

    unsafe fn assume_id_key(self) -> CellId {
        unimplemented!("{:?} doesn't implement assume_id_key", type_name::<Self>())
    }

    unsafe fn assume_tuple_key(self) -> (ColumnId, RecordId) {
        unimplemented!(
            "{:?} doesn't implement assume_tuple_key",
            type_name::<Self>()
        )
    }
}

impl CellKey for CellId {
    const KIND: CellKeyKind = CellKeyKind::Id;

    unsafe fn assume_id_key(self) -> CellId {
        self
    }
}

impl CellKey for (ColumnId, RecordId) {
    const KIND: CellKeyKind = CellKeyKind::Tuple;

    unsafe fn assume_tuple_key(self) -> (ColumnId, RecordId) {
        self
    }
}

struct CellPoolInner {
    cells: Slab<TypedArc<CellInner>>,
    lookup: RwLock<HashMap<(ColumnId, RecordId), CellId>>,
}

impl CellPoolInner {
    fn get(&self, key: impl CellKey) -> Option<Cell> {
        let inner = match key.kind() {
            CellKeyKind::Id => {
                let id = unsafe { key.assume_id_key() };
                self.cells.get(id.as_usize())?
            }
            CellKeyKind::Tuple => {
                let (column, record) = unsafe { key.assume_tuple_key() };
                let id = self.lookup.read().get(&(column, record)).copied()?;
                self.cells.get(id.as_usize())?
            }
        };

        Some(Cell(TypedArc::clone(&*inner)))
    }
}

pub struct CellPool(TypedArc<CellPoolInner>);

impl CellPool {
    pub fn new() -> Self {
        Self(TypedArc::new(CellPoolInner {
            cells: Slab::new(),
            lookup: RwLock::new(HashMap::new()),
        }))
    }

    pub fn new_cell(
        &self,
        column: ColumnId,
        record: RecordId,
        value: Option<DataValue>,
    ) -> Result<Cell> {
        let val = value
            .map(|val| {
                if column.kind().check(&val) {
                    Ok(val)
                } else {
                    anyhow::bail!("value does not match column type")
                }
            })
            .unwrap_or_else(|| Ok(DataValue::Nil(column.kind())))?;

        let slot = self
            .0
            .cells
            .vacant_entry()
            .ok_or_else(|| anyhow::anyhow!("out of memory"))?;

        let id = CellId::new(slot.key());
        let meta = CellMeta::new(id, column, record)?;

        let mut lookup = self.0.lookup.write();

        lookup.insert((column, record), id);

        let inner = TypedArc::new(CellInner {
            meta,
            pool: CellPool(TypedArc::clone(&self.0)),
            value: RwLock::new(val),
        });

        slot.insert(inner.clone());

        Ok(Cell(inner))
    }

    pub fn get(&self, key: impl CellKey) -> Option<Cell> {
        let item = self.0.get(key);

        item
    }
}

impl Clone for CellPool {
    fn clone(&self) -> Self {
        Self(TypedArc::clone(&self.0))
    }
}

impl std::fmt::Debug for CellPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CellPool")
            .field(&self.0.lookup.read())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use data_types::{number, DataType};

    use super::*;

    #[test]
    fn cell() -> Result<()> {
        let pool = CellPool::new();

        const INT: DataType = DataType::Integer(number::IntSize::X8);

        let table = TableId::new();
        let column = ColumnId::new(table, INT);
        let record = RecordId::new(table);
        let cell = pool.new_cell(column, record, None)?;

        assert_eq!(cell.column(), column);
        assert_eq!(cell.record().table(), table);
        assert_eq!(cell.kind(), ExpectedType::new(INT));

        let forty_two = DataValue::try_integer_from_number(42, None)?;

        cell.read_with(|value| {
            assert_eq!(*value, DataValue::Nil(ExpectedType::new(INT)));
        });

        cell.write_with(|value| {
            *value = forty_two.clone();
        });

        cell.read_with(|value| {
            assert_eq!(*value, forty_two);
        });

        Ok(())
    }
}
