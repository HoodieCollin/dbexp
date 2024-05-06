// #![allow(incomplete_features)]
#![feature(os_str_display)]
#![feature(generic_const_exprs)]

// use std::{any::Any, mem::MaybeUninit, num::NonZeroUsize, ops::RangeBounds, path::Path};

// use anyhow::Result;
// use data_types::{DataValue, ExpectedType};
// use indexmap::IndexMap;
// use object_ids::TableId;
// use primitives::{
//     byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
//     impl_access_bytes_for_into_bytes_type,
//     shared_object::SharedObject,
//     InternalPath, InternalString,
// };
// use store::{
//     record_store::{ColumnIndexes, RecordSlotHandle, MAX_COLUMNS},
//     slot::SlotHandle,
//     RecordStore, Store, StoreConfig, StoreError,
// };

// pub mod object_ids;
// pub mod store;

// #[derive(thiserror::Error, Debug)]
// pub enum InsertError {
//     #[error("record has too many values")]
//     ColumnLengthMismatch {
//         record_handle: RecordSlotHandle,
//         expected: usize,
//         values: Vec<Option<DataValue>>,
//     },
//     #[error("record value is invalid")]
//     InvalidValue {
//         record_handle: RecordSlotHandle,
//         column_handles: Vec<SlotHandle<DataValue>>,
//         column: usize,
//         values: Vec<Option<DataValue>>,
//         #[source]
//         error: anyhow::Error,
//     },
//     #[error("no values to insert")]
//     NoValues { record_handle: RecordSlotHandle },
//     #[error(transparent)]
//     Unexpected(#[from] anyhow::Error),
// }

// #[derive(Debug)]
// pub enum InsertState {
//     Done(Vec<RecordSlotHandle>),
//     Partial {
//         handles: Vec<(usize, RecordSlotHandle, Vec<SlotHandle<DataValue>>)>,
//         errors: Vec<(usize, InsertError)>,
//     },
// }

// #[derive(Clone, Copy, PartialEq, Eq, Hash)]
// pub struct DataConfig {
//     pub initial_block_count: Option<NonZeroUsize>,
//     pub block_capacity: Option<NonZeroUsize>,
//     pub data_type: ExpectedType,
// }

// impl_access_bytes_for_into_bytes_type!(DataConfig);

// impl IntoBytes for DataConfig {
//     fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
//         x.encode(self.initial_block_count)?;
//         x.encode(self.block_capacity)?;
//         x.encode(self.data_type)
//     }
// }

// impl FromBytes for DataConfig {
//     fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
//         x.decode(&mut this.initial_block_count)?;
//         x.decode(&mut this.block_capacity)?;
//         x.decode(&mut this.data_type)
//     }
// }

// impl std::fmt::Debug for DataConfig {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let mut d = f.debug_struct("DataConfig");
//         let mut full = true;

//         d.field("data_type", &self.data_type);

//         if let Some(initial_block_count) = self.initial_block_count {
//             d.field("initial_block_count", &initial_block_count);
//         } else {
//             full = false;
//         }

//         if let Some(block_capacity) = self.block_capacity {
//             d.field("block_capacity", &block_capacity);
//         } else {
//             full = false;
//         }

//         if full {
//             d.finish()
//         } else {
//             d.finish_non_exhaustive()
//         }
//     }
// }

// impl DataConfig {
//     pub fn new(data_type: impl Into<ExpectedType>) -> Self {
//         Self {
//             initial_block_count: None,
//             block_capacity: None,
//             data_type: data_type.into(),
//         }
//     }

//     pub fn into_store_config(self, table_config: &TableConfig) -> StoreConfig {
//         let initial_block_count = self
//             .initial_block_count
//             .unwrap_or(table_config.initial_block_count);

//         let block_capacity = self.block_capacity.unwrap_or(table_config.block_capacity);

//         StoreConfig {
//             initial_block_count,
//             block_capacity,
//             persistance: table_config.persistance,
//         }
//     }

//     pub fn try_new_value<V: Any>(&self, value: V) -> Result<DataValue> {
//         DataValue::try_from_any(self.data_type, value)
//     }

//     // TODO: support custom config
// }

// #[derive(Clone, Copy)]
// #[repr(C)]
// pub struct ColumnConfigs(NonZeroUsize, [MaybeUninit<DataConfig>; MAX_COLUMNS]);

// impl std::fmt::Debug for ColumnConfigs {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let mut d = f.debug_list();

//         for i in 0..self.0.get() {
//             d.entry(&unsafe { self.1.get_unchecked(i).assume_init() });
//         }

//         d.finish()
//     }
// }

// impl_access_bytes_for_into_bytes_type!(ColumnConfigs);

// impl IntoBytes for ColumnConfigs {
//     fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
//         let column_count = self.0.get();

//         x.encode(column_count)?;

//         for i in 0..column_count {
//             x.encode(unsafe { self.1.get_unchecked(i).assume_init() })?;
//         }

//         Ok(())
//     }
// }

// impl FromBytes for ColumnConfigs {
//     fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
//         let mut column_count = 0;

//         x.decode(&mut column_count)?;

//         this.0 = unsafe { NonZeroUsize::new_unchecked(column_count) };

//         for i in 0..column_count {
//             x.delegate(unsafe { this.1.get_unchecked_mut(i).assume_init_mut() })?;
//         }

//         Ok(())
//     }
// }

// impl PartialEq for ColumnConfigs {
//     fn eq(&self, other: &Self) -> bool {
//         if self.0 != other.0 {
//             return false;
//         }

//         let column_count = self.0.get();

//         for i in 0..column_count {
//             let a = unsafe { self.1.get_unchecked(i).assume_init() };
//             let b = unsafe { other.1.get_unchecked(i).assume_init() };

//             if a != b {
//                 return false;
//             }
//         }

//         true
//     }
// }

// impl Eq for ColumnConfigs {}

// impl std::hash::Hash for ColumnConfigs {
//     fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//         self.0.hash(state);

//         let column_count = self.0.get();

//         for i in 0..column_count {
//             let data_config = unsafe { self.1.get_unchecked(i).assume_init() };
//             data_config.hash(state);
//         }
//     }
// }

// impl ColumnConfigs {
//     pub fn new(configs: impl AsRef<[DataConfig]>) -> Result<Self> {
//         let configs = configs.as_ref();
//         let column_count = configs.len();

//         if column_count > MAX_COLUMNS {
//             anyhow::bail!("column count exceeds maximum");
//         } else if configs.is_empty() {
//             anyhow::bail!("column count must be greater than zero");
//         }

//         let mut inner = [MaybeUninit::uninit(); MAX_COLUMNS];

//         for (i, config) in configs.iter().copied().enumerate() {
//             unsafe {
//                 inner.get_unchecked_mut(i).write(config);
//             }
//         }

//         Ok(Self(
//             unsafe { NonZeroUsize::new_unchecked(column_count) },
//             inner,
//         ))
//     }

//     pub fn len(&self) -> usize {
//         self.0.get()
//     }

//     pub fn get(&self, index: usize) -> Option<&DataConfig> {
//         if index < self.0.get() {
//             Some(unsafe { self.get_unchecked(index) })
//         } else {
//             None
//         }
//     }

//     pub unsafe fn get_unchecked(&self, index: usize) -> &DataConfig {
//         self.1.get_unchecked(index).assume_init_ref()
//     }
// }

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
// pub struct TableConfig {
//     pub initial_block_count: NonZeroUsize,
//     pub block_capacity: NonZeroUsize,
//     pub persistance: InternalPath,
//     pub columns: ColumnConfigs,
// }

// impl_access_bytes_for_into_bytes_type!(TableConfig);

// impl IntoBytes for TableConfig {
//     fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
//         x.encode(self.initial_block_count)?;
//         x.encode(self.block_capacity)?;
//         x.encode(self.persistance)?;
//         x.encode(self.columns)
//     }
// }

// impl FromBytes for TableConfig {
//     fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
//         x.decode(&mut this.initial_block_count)?;
//         x.decode(&mut this.block_capacity)?;
//         x.delegate(&mut this.persistance)?;
//         x.delegate(&mut this.columns)
//     }
// }

// impl From<TableConfig> for StoreConfig {
//     fn from(config: TableConfig) -> Self {
//         Self {
//             initial_block_count: config.initial_block_count,
//             block_capacity: config.block_capacity,
//             persistance: config.persistance,
//         }
//     }
// }

// impl TableConfig {
//     pub fn new(columns: impl AsRef<[DataConfig]>) -> Result<Self> {
//         let StoreConfig {
//             initial_block_count,
//             block_capacity,
//             persistance,
//         } = StoreConfig::default();

//         let columns = ColumnConfigs::new(columns)?;

//         Ok(Self {
//             initial_block_count,
//             block_capacity,
//             persistance,
//             columns,
//         })
//     }

//     pub fn new_persisted(
//         columns: impl AsRef<[DataConfig]>,
//         persistance: impl AsRef<Path>,
//     ) -> Result<Self> {
//         let StoreConfig {
//             initial_block_count,
//             block_capacity,
//             ..
//         } = StoreConfig::default();

//         let columns = ColumnConfigs::new(columns)?;

//         Ok(Self {
//             initial_block_count,
//             block_capacity,
//             persistance: InternalPath::new(persistance.as_ref())?,
//             columns,
//         })
//     }
// }

// #[derive(Debug, Clone)]
// pub struct Table {
//     id: TableId,
//     config: TableConfig,
//     records: RecordStore,
//     columns: SharedObject<IndexMap<usize, Store<DataValue>>>,
//     columns_by_name: IndexMap<InternalString, usize>,
// }

// impl Table {
//     pub fn new(
//         id: TableId,
//         config: TableConfig,
//         name_mapping: Option<IndexMap<InternalString, usize>>,
//     ) -> Result<Self> {
//         let column_count = config.columns.len();
//         let columns = IndexMap::with_capacity(column_count);
//         let records = RecordStore::new(Some(id), Some(config.into()), column_count)?;

//         Ok(Self {
//             id,
//             config,
//             records,
//             columns: SharedObject::new(columns),
//             columns_by_name: name_mapping.unwrap_or_default(),
//         })
//     }

//     pub fn config(&self) -> &TableConfig {
//         &self.config
//     }

//     pub fn get_column_store(&self, idx: usize) -> Result<Store<DataValue>> {
//         if idx >= self.config.columns.len() {
//             anyhow::bail!("column index out of bounds");
//         }

//         let columns = self.columns.upgradable();

//         if let Some(store) = columns.get(&idx) {
//             return Ok(store.clone());
//         }

//         let store = Store::new(
//             Some(self.id),
//             Some(unsafe {
//                 self.config
//                     .columns
//                     .get_unchecked(idx)
//                     .into_store_config(&self.config)
//             }),
//         )?;

//         let mut columns = columns.upgrade();

//         columns.insert(idx, store.clone());

//         Ok(store)
//     }

//     pub fn get_column_by_name(&self, name: impl AsRef<str>) -> Option<Store<DataValue>> {
//         let name = InternalString::new(name.as_ref()).ok()?;
//         let idx = *self.columns_by_name.get(&name)?;

//         self.get_column_store(idx).ok()
//     }

//     pub fn get_column_stores(
//         &self,
//         indices: impl Into<Vec<usize>>,
//     ) -> Result<Vec<Store<DataValue>>> {
//         let mut indices: Vec<usize> = indices.into();
//         indices.dedup();
//         indices.sort_unstable();

//         if let Some(&idx) = indices.last() {
//             if idx >= self.config.columns.len() {
//                 anyhow::bail!("column index out of bounds");
//             }
//         }

//         let count = indices.len();

//         let mut stores = Vec::with_capacity(count);
//         let mut missing = Vec::with_capacity(count);

//         let columns = self.columns.upgradable();

//         for idx in indices {
//             if let Some(store) = columns.get(&idx) {
//                 stores.push(store.clone());
//             } else {
//                 missing.push(idx);
//             }
//         }

//         if missing.is_empty() {
//             return Ok(stores);
//         }

//         let mut columns = columns.upgrade();

//         for idx in missing {
//             let store = Store::new(
//                 Some(self.id),
//                 Some(unsafe {
//                     self.config
//                         .columns
//                         .get_unchecked(idx)
//                         .into_store_config(&self.config)
//                 }),
//             )?;

//             columns.insert(idx, store.clone());
//             stores.push(store);
//         }

//         Ok(stores)
//     }

//     pub fn get_column_store_range(
//         &self,
//         indices: impl RangeBounds<usize>,
//     ) -> Result<Vec<Store<DataValue>>> {
//         let start = match indices.start_bound() {
//             std::ops::Bound::Included(&start) => start,
//             std::ops::Bound::Excluded(&start) => start + 1,
//             std::ops::Bound::Unbounded => 0,
//         };

//         let end = match indices.end_bound() {
//             std::ops::Bound::Included(&end) => end + 1,
//             std::ops::Bound::Excluded(&end) => end,
//             std::ops::Bound::Unbounded => self.config.columns.len(),
//         };

//         if end > self.config.columns.len() {
//             anyhow::bail!("column index out of bounds");
//         }

//         let count = end - start;

//         let mut stores = Vec::with_capacity(count);
//         let mut missing = Vec::with_capacity(count);

//         let columns = self.columns.upgradable();

//         for idx in start..end {
//             if let Some(store) = columns.get(&idx) {
//                 stores.push(store.clone());
//             } else {
//                 missing.push(idx);
//             }
//         }

//         if missing.is_empty() {
//             return Ok(stores);
//         }

//         let mut columns = columns.upgrade();

//         for idx in missing {
//             let store = Store::new(
//                 Some(self.id),
//                 Some(unsafe {
//                     self.config
//                         .columns
//                         .get_unchecked(idx)
//                         .into_store_config(&self.config)
//                 }),
//             )?;

//             columns.insert(idx, store.clone());
//             stores.push(store);
//         }

//         Ok(stores)
//     }

//     pub fn insert_one(&self, values: Vec<Option<DataValue>>) -> Result<RecordSlotHandle> {
//         let val_count = values.len();

//         // Empty check
//         if val_count == 0 {
//             let (_, record_handle) = self.records.insert_one().map_err(StoreError::thread_safe)?;
//             return Ok(record_handle);
//         // Out of bounds check
//         } else if val_count > self.config.columns.len() {
//             anyhow::bail!("value count exceeds column count");
//         }

//         let (record, record_handle) = self.records.insert_one().map_err(StoreError::thread_safe)?;

//         let stores = self.get_column_store_range(..values.len())?;

//         record_handle.write_with(|mut data| {
//             data.update(|columns: &mut ColumnIndexes| {
//                 for (i, value) in values.into_iter().enumerate() {
//                     if let Some(data) = value {
//                         let store = stores.get(i).expect("store exists");
//                         let data_handle = store
//                             .insert_one(record, data)
//                             .map_err(StoreError::thread_safe)?;

//                         columns.replace(i, data_handle.into())?;
//                     }
//                 }

//                 Ok(())
//             })
//         })?;

//         Ok(record_handle)
//     }

//     pub fn insert<I>(&self, values: I) -> Result<InsertState, anyhow::Error>
//     where
//         I: IntoIterator<Item = Vec<Option<DataValue>>>,
//     {
//         let records = self
//             .records
//             .insert_map(values)
//             .map_err(StoreError::thread_safe)?;

//         let mut all_handles = Vec::with_capacity(records.len());
//         let mut all_errors = Vec::new();
//         let expected = self.config.columns.len();

//         for (idx, record, record_handle, values) in records {
//             let val_count = values.len();

//             // Empty check
//             if val_count == 0 {
//                 all_handles.push((idx, record_handle, vec![]));
//                 continue;
//             // Out of bounds check
//             } else if val_count > expected {
//                 all_errors.push((
//                     idx,
//                     InsertError::ColumnLengthMismatch {
//                         record_handle,
//                         expected,
//                         values,
//                     },
//                 ));

//                 continue;
//             }

//             let stores = self.get_column_store_range(..values.len())?;
//             let handle = record_handle.clone();
//             let needs_rollback = handle.write_with(|mut data| {
//                 data.update(|columns: &mut ColumnIndexes| {
//                     let mut column_handles = Vec::with_capacity(val_count);

//                     for (column, value) in values.iter().enumerate() {
//                         if let Some(data) = value {
//                             let store = stores.get(column).expect("store exists");
//                             let data_insert_res = store.insert_one(record, data.clone());

//                             match data_insert_res {
//                                 Ok(data_handle) => {
//                                     column_handles.push(data_handle.clone());
//                                     columns.replace(column, data_handle.into())?;
//                                 }
//                                 Err(StoreError::InsertError(
//                                     store::result::InsertError::InvalidValue { error, .. },
//                                 )) => {
//                                     all_errors.push((
//                                         idx,
//                                         InsertError::InvalidValue {
//                                             record_handle,
//                                             column_handles,
//                                             column,
//                                             values: values.clone(),
//                                             error,
//                                         },
//                                     ));

//                                     return Ok(None);
//                                 }
//                                 Err(error) => {
//                                     return Ok(Some((
//                                         column,
//                                         error,
//                                         record_handle,
//                                         column_handles,
//                                     )));
//                                 }
//                             }
//                         }
//                     }

//                     all_handles.push((idx, record_handle, column_handles));
//                     Ok(None)
//                 })
//             })?;

//             if let Some((_, _, record_handle, column_handles)) = needs_rollback {
//                 for handle in column_handles {
//                     handle.remove_self()?;
//                 }

//                 record_handle.remove_self()?;

//                 while all_handles.len() > 0 || all_errors.len() > 0 {
//                     if let Some((_, error)) = all_errors.pop() {
//                         match error {
//                             InsertError::InvalidValue {
//                                 record_handle,
//                                 column_handles,
//                                 ..
//                             } => {
//                                 for handle in column_handles {
//                                     handle.remove_self()?;
//                                 }

//                                 record_handle.remove_self()?;
//                             }
//                             InsertError::NoValues { record_handle } => {
//                                 record_handle.remove_self()?;
//                             }
//                             _ => {}
//                         }
//                     }

//                     if let Some((_, record_handle, column_handles)) = all_handles.pop() {
//                         for handle in column_handles {
//                             handle.remove_self()?;
//                         }

//                         record_handle.remove_self()?;
//                     }
//                 }

//                 anyhow::bail!("unexpected error resulted in rollback")
//             }
//         }

//         if all_errors.is_empty() {
//             Ok(InsertState::Done(
//                 all_handles
//                     .into_iter()
//                     .map(|(_, handle, _)| handle)
//                     .collect(),
//             ))
//         } else {
//             Ok(InsertState::Partial {
//                 handles: all_handles,
//                 errors: all_errors,
//             })
//         }
//     }
// }

// #[allow(dead_code)]
// #[cfg(test)]
// mod tests {
//     use anyhow::Result;
//     use data_types::DataType;

//     use super::*;

//     // #[test]
//     // fn test_column_configs() {
//     //
//     // }

//     // #[test]
//     // fn test_table_config() {
//     //
//     // }

//     #[test]
//     fn test_insert_one() -> Result<()> {
//         let columns = vec![
//             DataConfig::new(DataType::Number),
//             DataConfig::new(DataType::Bool),
//             DataConfig::new(DataType::Text(50)),
//         ];

//         let table_config = TableConfig::new(&columns)?;
//         let table = Table::new(TableId::new(), table_config, None)?;

//         assert_eq!(table.config, table_config);

//         table.insert_one(vec![
//             Some(DataValue::try_from_any(columns[0].data_type, 42)?),
//             Some(DataValue::Bool(true)),
//             Some(DataValue::try_from_any(columns[2].data_type, "testing")?),
//         ])?;

//         println!("{:#?}", table);

//         Ok(())
//     }

//     #[test]
//     fn test_insert() -> Result<()> {
//         let columns = vec![
//             DataConfig::new(DataType::Number),
//             DataConfig::new(DataType::Bool),
//             DataConfig::new(DataType::Text(50)),
//         ];

//         let table_config = TableConfig::new(&columns)?;
//         let table = Table::new(TableId::new(), table_config, None)?;

//         assert_eq!(table.config, table_config);

//         const ROW_COUNT: usize = 10;
//         let alphabet = "abcdefghijklmnopqrstuvwxyz";

//         let mut n = 0;
//         let rows = std::iter::repeat_with(|| {
//             let idx = n / ROW_COUNT;
//             let row = vec![
//                 Some(DataValue::try_from_any(columns[0].data_type, n)?),
//                 Some(DataValue::Bool(idx % 2 == 0)),
//                 Some(DataValue::try_from_any(
//                     columns[2].data_type,
//                     &alphabet[idx..idx + 1],
//                 )?),
//             ];

//             n += 10;
//             Ok(row)
//         })
//         .take(ROW_COUNT)
//         .collect::<Result<Vec<_>>>()?;

//         let result = table.insert(rows)?;

//         println!("{:#?}", result);
//         println!("##############################################################");
//         println!("##############################################################");
//         println!("##############################################################");
//         println!("{:#?}", table);

//         Ok(())
//     }
// }
