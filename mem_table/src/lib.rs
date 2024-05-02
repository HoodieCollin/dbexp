// #![allow(incomplete_features)]
#![feature(lazy_cell)]
#![feature(allocator_api)]
#![feature(os_str_display)]
#![feature(generic_const_exprs)]

use std::{collections::HashMap, mem::MaybeUninit, num::NonZeroUsize, path::Path};

use anyhow::Result;
use data_types::{DataType, DataValue};
use internal_path::InternalPath;
use object_ids::TableId;
use primitives::{
    byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
    impl_access_bytes_for_into_bytes_type,
};
use store::{record_store::MAX_COLUMNS, RecordStore, Store, StoreConfig};

pub mod internal_path;
pub mod object_ids;
pub mod store;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DataConfig {
    pub initial_block_count: Option<NonZeroUsize>,
    pub block_capacity: Option<NonZeroUsize>,
    pub data_type: DataType,
}

impl_access_bytes_for_into_bytes_type!(DataConfig);

impl IntoBytes for DataConfig {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.initial_block_count)?;
        x.encode(self.block_capacity)?;
        x.encode(self.data_type)
    }
}

impl FromBytes for DataConfig {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.initial_block_count)?;
        x.decode(&mut this.block_capacity)?;
        x.decode(&mut this.data_type)
    }
}

impl std::fmt::Debug for DataConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("DataConfig");
        let mut full = true;

        d.field("data_type", &self.data_type);

        if let Some(initial_block_count) = self.initial_block_count {
            d.field("initial_block_count", &initial_block_count);
        } else {
            full = false;
        }

        if let Some(block_capacity) = self.block_capacity {
            d.field("block_capacity", &block_capacity);
        } else {
            full = false;
        }

        if full {
            d.finish()
        } else {
            d.finish_non_exhaustive()
        }
    }
}

impl DataConfig {
    pub fn new(data_type: DataType) -> Self {
        Self {
            initial_block_count: None,
            block_capacity: None,
            data_type,
        }
    }

    pub fn into_store_config(self, table_config: &TableConfig) -> StoreConfig {
        let initial_block_count = self
            .initial_block_count
            .unwrap_or(table_config.initial_block_count);

        let block_capacity = self.block_capacity.unwrap_or(table_config.block_capacity);

        StoreConfig {
            initial_block_count,
            block_capacity,
            persistance: table_config.persistance,
        }
    }

    // TODO: support custom config
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct ColumnConfigs(NonZeroUsize, [MaybeUninit<DataConfig>; MAX_COLUMNS]);

impl std::fmt::Debug for ColumnConfigs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_list();

        for i in 0..self.0.get() {
            d.entry(&unsafe { self.1.get_unchecked(i).assume_init() });
        }

        d.finish()
    }
}

impl_access_bytes_for_into_bytes_type!(ColumnConfigs);

impl IntoBytes for ColumnConfigs {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        let column_count = self.0.get();

        x.encode(column_count)?;

        for i in 0..column_count {
            x.encode(unsafe { self.1.get_unchecked(i).assume_init() })?;
        }

        Ok(())
    }
}

impl FromBytes for ColumnConfigs {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        let mut column_count = 0;

        x.decode(&mut column_count)?;

        this.0 = unsafe { NonZeroUsize::new_unchecked(column_count) };

        for i in 0..column_count {
            x.delegate(unsafe { this.1.get_unchecked_mut(i).assume_init_mut() })?;
        }

        Ok(())
    }
}

impl PartialEq for ColumnConfigs {
    fn eq(&self, other: &Self) -> bool {
        if self.0 != other.0 {
            return false;
        }

        let column_count = self.0.get();

        for i in 0..column_count {
            let a = unsafe { self.1.get_unchecked(i).assume_init() };
            let b = unsafe { other.1.get_unchecked(i).assume_init() };

            if a != b {
                return false;
            }
        }

        true
    }
}

impl Eq for ColumnConfigs {}

impl std::hash::Hash for ColumnConfigs {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);

        let column_count = self.0.get();

        for i in 0..column_count {
            let data_config = unsafe { self.1.get_unchecked(i).assume_init() };
            data_config.hash(state);
        }
    }
}

impl ColumnConfigs {
    pub fn new(configs: impl AsRef<[DataConfig]>) -> Result<Self> {
        let configs = configs.as_ref();
        let column_count = configs.len();

        if column_count > MAX_COLUMNS {
            anyhow::bail!("column count exceeds maximum");
        } else if configs.is_empty() {
            anyhow::bail!("column count must be greater than zero");
        }

        let mut inner = [MaybeUninit::uninit(); MAX_COLUMNS];

        for (i, config) in configs.iter().copied().enumerate() {
            unsafe {
                inner.get_unchecked_mut(i).write(config);
            }
        }

        Ok(Self(
            unsafe { NonZeroUsize::new_unchecked(column_count) },
            inner,
        ))
    }

    pub fn len(&self) -> usize {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableConfig {
    pub initial_block_count: NonZeroUsize,
    pub block_capacity: NonZeroUsize,
    pub persistance: InternalPath,
    pub columns: ColumnConfigs,
}

impl_access_bytes_for_into_bytes_type!(TableConfig);

impl IntoBytes for TableConfig {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.initial_block_count)?;
        x.encode(self.block_capacity)?;
        x.encode(self.persistance)?;
        x.encode(self.columns)
    }
}

impl FromBytes for TableConfig {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        x.decode(&mut this.initial_block_count)?;
        x.decode(&mut this.block_capacity)?;
        x.delegate(&mut this.persistance)?;
        x.delegate(&mut this.columns)
    }
}

impl From<TableConfig> for StoreConfig {
    fn from(config: TableConfig) -> Self {
        Self {
            initial_block_count: config.initial_block_count,
            block_capacity: config.block_capacity,
            persistance: config.persistance,
        }
    }
}

impl TableConfig {
    pub fn new(columns: impl AsRef<[DataConfig]>) -> Result<Self> {
        let StoreConfig {
            initial_block_count,
            block_capacity,
            persistance,
        } = StoreConfig::default();

        let columns = ColumnConfigs::new(columns)?;

        Ok(Self {
            initial_block_count,
            block_capacity,
            persistance,
            columns,
        })
    }

    pub fn new_persisted(
        columns: impl AsRef<[DataConfig]>,
        persistance: impl AsRef<Path>,
    ) -> Result<Self> {
        let StoreConfig {
            initial_block_count,
            block_capacity,
            ..
        } = StoreConfig::default();

        let columns = ColumnConfigs::new(columns)?;

        Ok(Self {
            initial_block_count,
            block_capacity,
            persistance: InternalPath::new(persistance.as_ref())?,
            columns,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    id: TableId,
    config: TableConfig,
    records: RecordStore,
    columns: HashMap<usize, Store<DataValue>>,
}

impl Table {
    pub fn new(id: TableId, config: TableConfig) -> Result<Self> {
        let column_count = config.columns.len();
        let columns = HashMap::with_capacity(column_count);
        let records = RecordStore::new(Some(id), Some(config.into()), column_count)?;

        Ok(Self {
            id,
            config,
            records,
            columns,
        })
    }
}

#[allow(dead_code)]
#[cfg(test)]
mod test {
    use anyhow::Result;

    use super::*;

    // #[test]
    // fn test_column_configs() {
    //
    // }

    // #[test]
    // fn test_table_config() {
    //
    // }

    #[test]
    fn test_table() -> Result<()> {
        let columns = vec![
            DataConfig::new(DataType::Number),
            DataConfig::new(DataType::Bool),
            DataConfig::new(DataType::Timestamp),
        ];

        let table_config = TableConfig::new(&columns)?;
        let table = Table::new(TableId::new(), table_config)?;

        assert_eq!(table.config, table_config);

        println!("{:#?}", table);

        Ok(())
    }
}
