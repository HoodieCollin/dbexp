use std::{iter, num::NonZeroUsize, ops::RangeBounds};

use anyhow::Result;
use primitives::{DataValue, Idx, ThinIdx};

use crate::{
    object_ids::{RecordId, TableId},
    slot::SlotHandle,
    store::{InsertError, InsertState, Store, StoreConfig, StoreError},
};

pub const MAX_COLUMNS: usize = 32;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct CellIndex {
    pub block: ThinIdx,
    pub row: Idx,
}

impl std::fmt::Debug for CellIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellIndex")
            .field("block", &self.block)
            .field("row", &self.row)
            .finish()
    }
}

impl From<SlotHandle<DataValue>> for CellIndex {
    fn from(handle: SlotHandle<DataValue>) -> Self {
        Self {
            block: handle.block.index(),
            row: handle.idx,
        }
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

        let store = self.store.write();
        let record = RecordId::new(store.next_available_index(), table);
        let handle = self.store.insert_one(None, ColumnIndexes::new(columns))?;

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

        match self
            .store
            .insert(vec![(None, ColumnIndexes::new(columns)); count])?
        {
            InsertState::Done(handles) => Ok(handles
                .into_iter()
                .map(|h| (RecordId::new(h.idx.into_thin(), table), h))
                .collect::<Vec<_>>()),
            InsertState::Partial {
                errors, handles, ..
            } => {
                let mut tuples = handles
                    .into_iter()
                    .map(|(_, h)| (RecordId::new(h.idx.into_thin(), table), h))
                    .collect::<Vec<_>>();

                for (_, error) in errors {
                    match error {
                        // handle Idx collision
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

        let mut values = iter
            .into_iter()
            .enumerate()
            .map(|(index, values)| {
                let columns = ColumnIndexes::new(columns);

                (index, columns, values)
            })
            .collect::<Vec<_>>();

        let record_insert_state = self.store.insert(
            values
                .iter()
                .map(|(_, columns, _)| (None, *columns))
                .collect::<Vec<_>>(),
        )?;

        match record_insert_state {
            InsertState::Done(handles) => Ok(iter::zip(
                values.into_iter().map(|(idx, _, values)| (idx, values)),
                handles.into_iter(),
            )
            .map(|((index, values), h)| (index, RecordId::new(h.idx.into_thin(), table), h, values))
            .collect::<Vec<_>>()),
            InsertState::Partial { errors, handles } => {
                fn new_invalid_entry<T>() -> (usize, ColumnIndexes, Vec<T>) {
                    (usize::MAX, ColumnIndexes::INVALID, vec![])
                }

                let mut tuples = {
                    handles
                        .into_iter()
                        .map(|(i, h)| {
                            let entry = values.get_mut(i).unwrap();
                            let (index, _, values) = std::mem::replace(entry, new_invalid_entry());

                            (index, RecordId::new(h.idx.into_thin(), table), h, values)
                        })
                        .collect::<Vec<_>>()
                };

                for (index, error) in errors {
                    match error {
                        InsertError::AlreadyExists { .. } => {
                            let mut retries_remaining = 3i8;

                            loop {
                                match self.insert_one() {
                                    Ok((record, h)) => {
                                        let entry = values.get_mut(index).unwrap();

                                        let (idx, _, values) =
                                            std::mem::replace(entry, new_invalid_entry());

                                        tuples.push((idx, record, h, values));
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
