use std::{iter, num::NonZeroUsize, ops::RangeBounds};

use anyhow::Result;

use crate::{
    indices::{ColumnIndices, MAX_COLUMNS},
    object_ids::{RecordId, TableId},
    slot::SlotHandle,
    store::{InsertError, InsertState, Store, StoreConfig, StoreError},
};

pub type RecordsError = StoreError<ColumnIndices>;
pub type RecordHandle = SlotHandle<ColumnIndices>;

#[derive(Debug, Clone)]
pub struct Records {
    store: Store<ColumnIndices>,
    table: TableId,
    columns: NonZeroUsize,
}

impl Records {
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
    pub fn insert_one(&self) -> Result<(RecordId, RecordHandle), RecordsError> {
        let table = self.table;
        let columns = self.columns;

        let store = self.store.write();
        let record = RecordId::new(store.next_available_index(), table);
        let handle = self.store.insert_one(None, ColumnIndices::new(columns))?;

        Ok((record, handle.ensure_idx_has_gen()))
    }

    #[must_use]
    pub fn insert(&self, count: usize) -> Result<Vec<(RecordId, RecordHandle)>, RecordsError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let table = self.table;
        let columns = self.columns;

        match self
            .store
            .insert(vec![(None, ColumnIndices::new(columns)); count])?
        {
            InsertState::Done(handles) => Ok(handles
                .into_iter()
                .map(|h| (RecordId::new(h.idx, table), h.ensure_idx_has_gen()))
                .collect::<Vec<_>>()),
            InsertState::Partial {
                errors, handles, ..
            } => {
                let mut tuples = handles
                    .into_iter()
                    .map(|(_, h)| (RecordId::new(h.idx, table), h.ensure_idx_has_gen()))
                    .collect::<Vec<_>>();

                for (_, error) in errors {
                    match error {
                        // handle Idx collision
                        InsertError::AlreadyExists { .. } => {
                            let mut retries_remaining = 3i8;

                            loop {
                                match self.insert_one() {
                                    Ok((record, handle)) => {
                                        tuples.push((record, handle.ensure_idx_has_gen()));
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
    pub fn insert_map<I, U, T>(
        &self,
        iter: I,
    ) -> Result<Vec<(usize, RecordId, RecordHandle, Vec<T>)>, RecordsError>
    where
        I: IntoIterator<Item = U>,
        U: IntoIterator<Item = T>,
    {
        let table = self.table;
        let columns = self.columns;

        let mut values = iter
            .into_iter()
            .enumerate()
            .map(|(index, values)| {
                let columns = ColumnIndices::new(columns);

                (index, columns, values.into_iter().collect::<Vec<_>>())
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
            .map(|((index, values), h)| {
                (
                    index,
                    RecordId::new(h.idx, table),
                    h.ensure_idx_has_gen(),
                    values,
                )
            })
            .collect::<Vec<_>>()),
            InsertState::Partial { errors, handles } => {
                fn new_invalid_entry<T>() -> (usize, ColumnIndices, Vec<T>) {
                    (usize::MAX, ColumnIndices::INVALID, vec![])
                }

                let mut tuples = {
                    handles
                        .into_iter()
                        .map(|(i, h)| {
                            let entry = values.get_mut(i).unwrap();
                            let (index, _, values) = std::mem::replace(entry, new_invalid_entry());

                            (
                                index,
                                RecordId::new(h.idx, table),
                                h.ensure_idx_has_gen(),
                                values,
                            )
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

                                        tuples.push((idx, record, h.ensure_idx_has_gen(), values));
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
