use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;

use self::meta::FieldMeta;
use super::data_block::{DataBlock, BLOCK_CAPACITY};
use super::data_type::DataType;
use super::data_value::DataValue;
use super::FileAction;
use crate::uid::Uid;

mod meta;

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct Field {
    #[serde(default = "Uid::new")]
    pub id: Uid,
    pub unique: bool,
    pub required: bool,
    pub automatic: bool,
    pub data_type: DataType,
    #[serde(skip)]
    dir_path: Option<PathBuf>,
    #[serde(skip)]
    meta: FieldMeta,
    #[serde(skip)]
    blocks: Vec<DataBlock>,
}

#[derive(Debug, Copy, Clone)]
struct BlockIndex(pub usize);

#[derive(Debug, Copy, Clone)]
pub enum DataIndex {
    Global(usize),
    Local { block: usize, entry: usize },
}

impl Field {
    pub fn init<P: AsRef<Path>>(&mut self, parent_path: P) -> Result<()> {
        let partial_path = parent_path.as_ref().join(self.id.to_string());

        if !partial_path.exists() {
            fs::create_dir_all(&partial_path)?;
        }

        self.meta.open(&partial_path)?;

        let block_count = self.meta.block_count();

        self.blocks.clear();
        self.blocks.reserve(block_count);

        for block_index in 0..block_count {
            let block_path = partial_path.join(block_index.to_string());
            let mut block = DataBlock::new_with(self.data_type)?;

            block.open(block_path)?;

            if block.is_full() {
                self.meta.mark_occupied(block_index);
            } else {
                self.meta.mark_available(block_index);
            }

            self.blocks.push(block);
        }

        self.dir_path = Some(partial_path);

        Ok(())
    }

    fn next_available_block(&mut self) -> Result<(&mut DataBlock, BlockIndex)> {
        let index = if let Some(block_index) = self.meta.next_available() {
            block_index
        } else {
            let block_index = self.meta.add_block();
            let mut block = DataBlock::new_with(self.data_type)?;

            block.open(
                self.dir_path
                    .as_ref()
                    .unwrap()
                    .join(block_index.to_string()),
            )?;

            self.blocks.push(block);

            return Ok((self.blocks.last_mut().unwrap(), BlockIndex(block_index)));
        };

        Ok((self.blocks.get_mut(index).unwrap(), BlockIndex(index)))
    }

    fn calculate_indexes(&self, index: DataIndex) -> (usize, usize) {
        match index {
            DataIndex::Global(index) => {
                let block_index = index / BLOCK_CAPACITY;
                let entry_index = index % BLOCK_CAPACITY;

                (block_index, entry_index)
            }
            DataIndex::Local { block, entry } => (block, entry),
        }
    }

    pub fn next_available_index(&self) -> Option<DataIndex> {
        if let Some(block_index) = self.meta.next_available() {
            let block = self.blocks.get(block_index).unwrap();

            return Some(DataIndex::Local {
                block: block_index,
                entry: block.next_available_index().unwrap(),
            });
        }

        None
    }

    pub fn push(&mut self, value: DataValue) -> Result<DataIndex> {
        let (block, BlockIndex(block_index)) = self.next_available_block()?;
        let entry_index = block.push(value)?;

        if block.is_full() {
            self.meta.mark_occupied(block_index);
        }

        Ok(DataIndex::Local {
            block: block_index,
            entry: entry_index,
        })
    }

    pub fn insert(&mut self, index: DataIndex, value: DataValue) -> Result<()> {
        let (block_index, entry_index) = self.calculate_indexes(index);

        let block = if let Some(block) = self.blocks.get_mut(block_index) {
            block
        } else {
            anyhow::bail!("block not found")
        };

        block.insert(entry_index, value)?;

        if block.is_full() {
            self.meta.mark_occupied(block_index);
        }

        Ok(())
    }

    pub fn delete(&mut self, index: DataIndex) -> Result<()> {
        let (block_index, entry_index) = self.calculate_indexes(index);

        let block = if let Some(block) = self.blocks.get_mut(block_index) {
            block
        } else {
            anyhow::bail!("block not found")
        };

        block.delete(entry_index)?;

        if !block.is_full() {
            self.meta.mark_available(block_index);
        }

        Ok(())
    }

    pub fn save(&mut self) -> Result<()> {
        self.meta.save()?;

        for block in &mut self.blocks {
            block.save()?;
        }

        Ok(())
    }
}
