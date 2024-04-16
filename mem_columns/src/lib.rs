use std::{any::Any, sync::Arc};

use anyhow::Result;
use bumpalo::Bump;
use data_types::{bytes, integer, text, DataType, DataValue};
use im::Vector;

pub struct MemColumn {
    kind: DataType,
    cells: Vector<DataValue>,
    alloc: Arc<Bump>,
}

impl MemColumn {
    pub fn new(kind: DataType) -> Self {
        Self {
            kind,
            cells: Vector::new(),
            alloc: Arc::new(Bump::new()),
        }
    }

    pub fn push<T: Any>(&mut self, value: Option<T>) -> Result<()> {
        match &value {
            None => self.cells.push_back(DataValue::Nil(self.kind.into())),
            Some(value) => {
                let data = DataValue::try_from_any(self.kind, value, &self.alloc)?;
                self.cells.push_back(data);
            }
        }

        Ok(())
    }

    pub fn push_default(&mut self) {
        self.cells.push_back(match self.kind {
            DataType::Bool => DataValue::Bool(false),
            DataType::Integer(size) => DataValue::Integer(integer::Integer::new(size)),
            DataType::Ratio => DataValue::Ratio(Default::default()),
            DataType::Uid => DataValue::Uid(Default::default()),
            DataType::O16 => DataValue::O16(Default::default()),
            DataType::O32 => DataValue::O32(Default::default()),
            DataType::Decimal => DataValue::Decimal(Default::default()),
            DataType::Timestamp => DataValue::Timestamp(Default::default()),
            DataType::Bytes(cap) => DataValue::Bytes(bytes::Bytes::new(cap, &self.alloc)),
            DataType::Text(cap) => DataValue::Text(text::Text::new(cap, &self.alloc)),
        });
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn get(&self, index: usize) -> Option<&DataValue> {
        self.cells.get(index)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut DataValue> {
        self.cells.get_mut(index)
    }

    pub fn try_set<T: Any>(&mut self, index: usize, value: Option<T>) -> Result<DataValue> {
        let old = match &value {
            None => self.cells.set(index, DataValue::Nil(self.kind.into())),
            Some(value) => {
                let data = DataValue::try_from_any(self.kind, value, &self.alloc)?;
                self.cells.set(index, data)
            }
        };

        Ok(old)
    }
}
