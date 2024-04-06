use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::{collections::BTreeSet, fs::File, path::Path};

use anyhow::Result;

use self::meta::DataBlockMeta;
use super::data_type::DataType;
use super::data_value::DataValue;
use super::FileAction;

mod meta;

pub(super) const BLOCK_CAPACITY: usize = 4096;

#[derive(Debug, Default)]
pub struct DataBlock {
    data_type: DataType,
    meta: DataBlockMeta,
    file: Option<File>,
    data: Vec<u8>,
    changed: BTreeSet<usize>,
}

impl DataBlock {
    pub fn new<T>() -> Result<Self> {
        let mut new = Self::default();
        new.data_type = DataType::from_type::<T>();
        new.data = vec![0; new.memory_capacity_in_bytes()];
        new.changed = BTreeSet::new();
        Ok(new)
    }

    pub fn new_with(data_type: DataType) -> Result<Self> {
        let mut new = Self::default();
        new.data_type = data_type;
        new.data = vec![0; new.memory_capacity_in_bytes()];
        new.changed = BTreeSet::new();
        Ok(new)
    }

    pub fn memory_capacity_in_bytes(&self) -> usize {
        self.data_type.size_as_bytes() * BLOCK_CAPACITY
    }

    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> Result<FileAction> {
        if !path.as_ref().exists() {
            fs::create_dir_all(&path.as_ref())?;
        }

        let block_path = path.as_ref().join(path.as_ref().file_name().unwrap());

        self.file = Some(
            File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(block_path.with_extension("dat"))?,
        );

        match self.meta.open(block_path)? {
            FileAction::Created => {
                self.file
                    .as_ref()
                    .unwrap()
                    .set_len(self.memory_capacity_in_bytes() as _)?;

                Ok(FileAction::Created)
            }
            FileAction::Opened => {
                self.file.as_ref().unwrap().read_to_end(&mut self.data)?;

                Ok(FileAction::Opened)
            }
        }
    }

    pub fn save(&mut self) -> Result<()> {
        self.meta.save()?;

        if self.file.is_none() {
            anyhow::bail!("file not open")
        }

        if self.changed.is_empty() {
            return Ok(());
        }

        if self.changed.len() == 1 {
            let index = *self.changed.iter().next().unwrap();
            let bytes = self.memory_slot(index)?;

            self.file
                .as_ref()
                .unwrap()
                .seek(SeekFrom::Start(index as u64))?;
            self.file.as_ref().unwrap().write_all(bytes)?;

            return Ok(());
        }

        const MAJORITY: usize = BLOCK_CAPACITY * (2 / 3);

        if self.changed.len() >= MAJORITY {
            self.file.as_ref().unwrap().seek(SeekFrom::Start(0))?;
            self.file.as_ref().unwrap().write_all(&self.data)?;
            return Ok(());
        }

        const GAP_TOLERANCE: usize = 4;

        let mut changes = Vec::with_capacity(self.data_type.size_as_bytes() * MAJORITY);
        let mut changed_indexes = self.changed.iter().copied();
        let mut previous_index = changed_indexes.next().unwrap();
        let mut start = previous_index;

        for index in changed_indexes {
            let should_flush = match index {
                1..=GAP_TOLERANCE => false,
                _ => index - GAP_TOLERANCE > previous_index,
            };

            if !should_flush {
                let one_after_current = index + 1;
                changes.extend_from_slice(self.memory_range(previous_index..one_after_current)?);
                previous_index = index;
                continue;
            }

            self.file
                .as_ref()
                .unwrap()
                .seek(SeekFrom::Start(start as u64))?;
            self.file.as_ref().unwrap().write_all(&changes)?;
            changes.clear();

            previous_index = index;
            start = index;
        }

        let one_after_current = previous_index + 1;
        changes.extend_from_slice(self.memory_range(start..one_after_current)?);

        self.file
            .as_ref()
            .unwrap()
            .seek(SeekFrom::Start(start as u64))?;
        self.file.as_ref().unwrap().write_all(&changes)?;

        Ok(())
    }

    fn memory_slot(&self, index: usize) -> Result<&[u8]> {
        if index >= BLOCK_CAPACITY {
            anyhow::bail!("index out of bounds")
        }

        let size = self.data_type.size_as_bytes();
        let head = index * size;
        let tail = head + size;

        Ok(&self.data[head..tail])
    }

    fn memory_range(&self, range: Range<usize>) -> Result<&[u8]> {
        let size = self.data_type.size_as_bytes();
        let head = range.start * size;
        let tail = head + (range.end - range.start) * size;

        Ok(&self.data[head..tail])
    }

    fn memory_slot_mut(&mut self, index: usize) -> Result<&mut [u8]> {
        if index >= BLOCK_CAPACITY {
            anyhow::bail!("index out of bounds")
        }

        let size = self.data_type.size_as_bytes();
        let head = index * size;
        let tail = head + size;

        Ok(&mut self.data[head..tail])
    }

    pub fn is_full(&self) -> bool {
        self.meta.is_full()
    }

    pub fn next_available_index(&self) -> Option<usize> {
        self.meta.next_available()
    }

    pub fn push(&mut self, value: DataValue) -> Result<usize> {
        if let Some(index) = self.next_available_index() {
            self.insert(index, value)?;

            Ok(index)
        } else {
            anyhow::bail!("no more space")
        }
    }

    pub fn insert(&mut self, index: usize, value: DataValue) -> Result<()> {
        value.check_type(self.data_type)?;

        let bytes = self.memory_slot_mut(index)?;
        value.copy_to(bytes)?;

        self.changed.insert(index);
        self.meta.mark_occupied(index);

        Ok(())
    }

    pub fn delete(&mut self, index: usize) -> Result<()> {
        self.changed.insert(index);
        self.meta.mark_available(index);

        Ok(())
    }

    pub fn get_with(&self, index: usize, dest: &mut DataValue) -> Result<bool> {
        let bytes = self.memory_slot(index)?;

        if self.meta.is_available(index) {
            return Ok(false);
        }

        dest.copy_from(bytes)?;

        Ok(true)
    }

    pub fn get(&self, index: usize) -> Result<Option<DataValue>> {
        let mut value = self.data_type.new_value();

        if self.get_with(index, &mut value)? {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    pub fn list_with(&self, dest: &mut Vec<DataValue>) -> Result<()> {
        let len = self.meta.occupied_len();
        dest.reserve(len);

        if len == 0 {
            return Ok(());
        }

        if len == 1 {
            self.meta.for_each_occupied(|index| {
                let mut value = self.data_type.new_value();
                if self.get_with(index, &mut value)? {
                    dest.push(value);
                }

                Ok(())
            })?;

            return Ok(());
        }

        self.meta.for_each_occupied(|index| {
            let mut value = self.data_type.new_value();
            let bytes = self.memory_slot(index)?;

            value.copy_from(bytes)?;
            dest.push(value);

            Ok(())
        })?;

        Ok(())
    }

    pub fn list(&self) -> Result<Vec<DataValue>> {
        let mut list = Vec::new();
        self.list_with(&mut list)?;
        Ok(list)
    }
}

impl Drop for DataBlock {
    fn drop(&mut self) {
        if let Err(e) = self.save() {
            eprintln!("failed to save: {:?}", e);
        }
    }
}
