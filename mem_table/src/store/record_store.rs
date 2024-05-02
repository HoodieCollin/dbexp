use std::{iter, num::NonZeroUsize, ops::RangeBounds};

use anyhow::Result;

use crate::object_ids::{RecordId, TableId};

use super::{InsertState, Store, StoreConfig, StoreError};

pub const MAX_COLUMNS: usize = 32;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ColumnIndexes(NonZeroUsize, [Option<NonZeroUsize>; MAX_COLUMNS]);

impl std::fmt::Debug for ColumnIndexes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_list();

        for i in 0..self.0.get() {
            if let Some(column) = self.1[i] {
                d.entry(&column.get());
            } else {
                d.entry(&"None");
            }
        }

        d.finish()
    }
}

impl ColumnIndexes {
    pub const INVALID: Self = Self(NonZeroUsize::MAX, [None; MAX_COLUMNS]);

    pub fn new(count: NonZeroUsize) -> Self {
        Self(count, [None; MAX_COLUMNS])
    }
}

pub type RecordInsertState = InsertState<ColumnIndexes>;
pub type RecordStoreError = StoreError<ColumnIndexes>;

#[derive(Debug, Clone)]
pub struct RecordStore {
    store: Store<ColumnIndexes>,
    table: TableId,
    columns: NonZeroUsize,
}

impl RecordStore {
    pub fn new(
        table: Option<TableId>,
        config: Option<StoreConfig>,
        columns: usize,
    ) -> Result<Self> {
        if columns > MAX_COLUMNS {
            anyhow::bail!("column count exceeds maximum");
        } else if columns == 0 {
            anyhow::bail!("column count must be greater than zero");
        }

        let table = table.unwrap_or_default();

        Ok(Self {
            store: Store::new(Some(table), config)?,
            table,
            columns: unsafe { NonZeroUsize::new_unchecked(columns) },
        })
    }

    pub fn load(&self, range: impl RangeBounds<usize>) -> Result<()> {
        self.store.load(range)
    }

    pub fn insert(&self, count: usize) -> Result<RecordInsertState, RecordStoreError> {
        if count == 0 {
            return Ok(RecordInsertState::NoOp);
        }

        let table = self.table;
        let columns = self.columns;

        self.store.insert(
            iter::repeat_with(move || (RecordId::new(table), ColumnIndexes::new(columns)))
                .take(count),
        )
    }
}
