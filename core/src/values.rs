use anyhow::Result;

use crate::{
    object_ids::TableId,
    slot::SlotHandle,
    store::{Store, StoreConfig, StoreError},
};

// mod math;
pub mod value;

pub use value::DataValue;

pub type ValueError = StoreError<DataValue>;
pub type ValueHandle = SlotHandle<DataValue>;

#[derive(Debug, Clone)]
pub struct Values(Store<DataValue>);

impl std::ops::Deref for Values {
    type Target = Store<DataValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for Values {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Values {
    #[must_use]
    pub fn new(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        Ok(Self(Store::new(table, config)?))
    }
}
