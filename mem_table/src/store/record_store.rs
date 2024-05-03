use std::{iter, num::NonZeroUsize, ops::RangeBounds};

use anyhow::Result;
use data_types::DataValue;

use crate::object_ids::{RecordId, TableId};

use super::{slot::SlotHandle, InsertError, InsertState, Store, StoreConfig, StoreError};

pub const MAX_COLUMNS: usize = 32;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct CellIndex {
    pub block: NonZeroUsize,
    pub row: NonZeroUsize,
}

impl std::fmt::Debug for CellIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellIndex")
            .field("block", &self.block())
            .field("row", &self.row())
            .finish()
    }
}

impl From<SlotHandle<DataValue>> for CellIndex {
    fn from(handle: SlotHandle<DataValue>) -> Self {
        let block = handle.block.idx();
        let row = handle.idx;

        Self::new_base_zero(block, row)
    }
}

impl CellIndex {
    pub fn new_base_zero(block: usize, row: usize) -> Self {
        let block = if block == usize::MAX {
            usize::MAX
        } else {
            block + 1
        };

        let row = if row == usize::MAX {
            usize::MAX
        } else {
            row + 1
        };

        unsafe {
            Self {
                block: NonZeroUsize::new_unchecked(block),
                row: NonZeroUsize::new_unchecked(row),
            }
        }
    }

    pub fn new(block: NonZeroUsize, row: NonZeroUsize) -> Self {
        Self { block, row }
    }

    pub fn block(&self) -> usize {
        self.block.get() - 1
    }

    pub fn row(&self) -> usize {
        self.row.get() - 1
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ColumnIndexes(NonZeroUsize, [Option<CellIndex>; MAX_COLUMNS]);

impl std::fmt::Debug for ColumnIndexes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_list();

        for i in 0..self.0.get() {
            if let Some(cell) = self.1[i] {
                d.entry(&cell);
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

    #[must_use]
    pub fn replace(&mut self, column: usize, value: CellIndex) -> Result<()> {
        if column >= self.0.get() {
            anyhow::bail!("column index out of bounds");
        }

        unsafe {
            self.1.get_unchecked_mut(column).replace(value);
        }

        Ok(())
    }
}

pub type RecordStoreError = StoreError<ColumnIndexes>;
pub type RecordSlotHandle = SlotHandle<ColumnIndexes>;

#[derive(Debug, Clone)]
pub struct RecordStore {
    store: Store<ColumnIndexes>,
    table: TableId,
    columns: NonZeroUsize,
}

impl RecordStore {
    #[must_use]
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

    #[must_use]
    pub fn load(&self, range: impl RangeBounds<usize>) -> Result<()> {
        self.store.load(range)
    }

    #[must_use]
    pub fn insert_one(&self) -> Result<(RecordId, RecordSlotHandle), RecordStoreError> {
        let table = self.table;
        let columns = self.columns;

        let record = RecordId::new(table);
        let handle = self.store.insert_one(record, ColumnIndexes::new(columns))?;

        Ok((record, handle))
    }

    #[must_use]
    pub fn insert(
        &self,
        count: usize,
    ) -> Result<Vec<(RecordId, RecordSlotHandle)>, RecordStoreError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let table = self.table;
        let columns = self.columns;

        let records = iter::repeat_with(|| (RecordId::new(table), ColumnIndexes::new(columns)))
            .take(count)
            .collect::<Vec<_>>();

        match self.store.insert(records.clone())? {
            InsertState::Done(handles) => Ok(iter::zip(
                records.iter().copied().map(|(record, ..)| record),
                handles.into_iter(),
            )
            .collect::<Vec<_>>()),
            InsertState::Partial {
                errors, handles, ..
            } => {
                let mut tuples = handles
                    .into_iter()
                    .map(|(idx, h)| (records.get(idx).unwrap().0, h))
                    .collect::<Vec<_>>();

                for (_, error) in errors {
                    match error {
                        InsertError::AlreadyExists { .. } => {
                            let mut retries_remaining = 3i8;

                            loop {
                                match self.insert_one() {
                                    Ok(tuple) => {
                                        tuples.push(tuple);
                                        break;
                                    }
                                    Err(err) => {
                                        retries_remaining -= 1;

                                        if retries_remaining.is_negative() {
                                            return Err(err);
                                        }

                                        continue;
                                    }
                                }
                            }
                        }
                        _ => unreachable!("unexpected error"),
                    }
                }

                Ok(tuples)
            }
        }
    }

    /// Consumes the iterator inserting a record for each value. Returns a vector of record IDs and
    /// slot handles for each value along with the value itself in the order they were inserted.
    #[must_use]
    pub fn insert_map<I, T>(
        &self,
        iter: I,
    ) -> Result<Vec<(usize, RecordId, RecordSlotHandle, Vec<T>)>, RecordStoreError>
    where
        I: IntoIterator<Item = Vec<T>>,
    {
        let table = self.table;
        let columns = self.columns;

        let mut records_and_values = iter
            .into_iter()
            .enumerate()
            .map(|(idx, values)| {
                let record = RecordId::new(table);
                let columns = ColumnIndexes::new(columns);

                (idx, record, columns, values)
            })
            .collect::<Vec<_>>();

        let record_insert_state = self.store.insert(
            records_and_values
                .iter()
                .map(|(_, record, columns, _)| (*record, *columns))
                .collect::<Vec<_>>(),
        )?;

        match record_insert_state {
            InsertState::Done(handles) => Ok(iter::zip(
                records_and_values
                    .into_iter()
                    .map(|(idx, record, _, values)| (idx, record, values)),
                handles.into_iter(),
            )
            .map(|((idx, record, values), handle)| (idx, record, handle, values))
            .collect::<Vec<_>>()),
            InsertState::Partial { errors, handles } => {
                fn new_invalid_entry<T>() -> (usize, RecordId, ColumnIndexes, Vec<T>) {
                    (
                        usize::MAX,
                        RecordId::INVALID,
                        ColumnIndexes::INVALID,
                        vec![],
                    )
                }

                let mut tuples = {
                    handles
                        .into_iter()
                        .map(|(i, handle)| {
                            let entry = records_and_values.get_mut(i).unwrap();
                            let (idx, record, _, values) =
                                std::mem::replace(entry, new_invalid_entry());

                            (idx, record, handle, values)
                        })
                        .collect::<Vec<_>>()
                };

                for (idx, error) in errors {
                    match error {
                        InsertError::AlreadyExists { .. } => {
                            let mut retries_remaining = 3i8;

                            loop {
                                match self.insert_one() {
                                    Ok((record, handle)) => {
                                        let entry = records_and_values.get_mut(idx).unwrap();

                                        let (idx, _, _, values) =
                                            std::mem::replace(entry, new_invalid_entry());

                                        tuples.push((idx, record, handle, values));
                                        break;
                                    }
                                    Err(err) => {
                                        retries_remaining -= 1;

                                        if retries_remaining.is_negative() {
                                            return Err(err);
                                        }

                                        continue;
                                    }
                                }
                            }
                        }
                        _ => unreachable!("unexpected error"),
                    }
                }

                Ok(tuples)
            }
        }
    }
}
