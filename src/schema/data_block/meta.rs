use anyhow::Result;
use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use super::{FileAction, BLOCK_CAPACITY};

#[derive(Debug)]
pub struct DataBlockMeta {
    file: Option<File>,
    available: BTreeSet<usize>,
    occupied: BTreeSet<usize>,
    previous: Option<Vec<u8>>,
}

impl DataBlockMeta {
    pub fn new() -> Self {
        Self {
            file: None,
            available: (0..BLOCK_CAPACITY).collect(),
            occupied: BTreeSet::new(),
            previous: None,
        }
    }

    pub fn is_available(&self, index: usize) -> bool {
        self.available.contains(&index)
    }

    pub fn is_occupied(&self, index: usize) -> bool {
        self.occupied.contains(&index)
    }

    pub fn is_full(&self) -> bool {
        self.available.is_empty()
    }

    pub fn is_empty(&self) -> bool {
        self.occupied.is_empty()
    }

    pub fn mark_available(&mut self, index: usize) {
        self.available.insert(index);
        self.occupied.remove(&index);
    }

    pub fn mark_occupied(&mut self, index: usize) {
        self.occupied.insert(index);
        self.available.remove(&index);
    }

    pub fn next_available(&self) -> Option<usize> {
        self.available.iter().next().copied()
    }

    pub fn occupied_len(&self) -> usize {
        self.occupied.len()
    }

    pub fn for_each_occupied<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(usize) -> Result<()>,
    {
        for index in &self.occupied {
            f(*index)?;
        }

        Ok(())
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != BLOCK_CAPACITY {
            return Err(anyhow::anyhow!("invalid block capacity"));
        }

        let mut available = BTreeSet::new();
        let mut occupied = BTreeSet::new();

        for (i, byte) in bytes.iter().copied().enumerate() {
            if byte == 0 {
                available.insert(i);
            } else {
                occupied.insert(i);
            }
        }

        Ok(Self {
            file: None,
            available,
            occupied,
            previous: Some(bytes.to_owned()),
        })
    }

    /**
     * This assumes that the destination vector is already initialized with zeros.
     */
    pub fn to_bytes_with(&self, dest: &mut Vec<u8>) {
        for index in &self.occupied {
            dest[*index] = 1;
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![0; BLOCK_CAPACITY];

        self.to_bytes_with(&mut bytes);

        bytes
    }

    pub fn open<P: AsRef<Path>>(&mut self, path: P) -> Result<FileAction> {
        let file_path = path.as_ref().with_extension("meta");
        let write_zeros = !file_path.exists();

        self.file = Some(
            File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(file_path)?,
        );

        if write_zeros {
            self.file.as_ref().unwrap().set_len(BLOCK_CAPACITY as _)?;
            self.file.as_ref().unwrap().sync_all()?;
            Ok(FileAction::Created)
        } else {
            self.available.clear();
            self.occupied.clear();

            let mut bytes = Vec::with_capacity(BLOCK_CAPACITY);
            self.file.as_ref().unwrap().read_to_end(&mut bytes)?;

            for (i, byte) in bytes.iter().copied().enumerate() {
                if byte == 0 {
                    self.available.insert(i);
                } else {
                    self.occupied.insert(i);
                }
            }

            self.previous = Some(bytes);

            Ok(FileAction::Opened)
        }
    }

    pub fn save(&mut self) -> Result<()> {
        if self.file.is_none() {
            anyhow::bail!("file not open")
        }

        if self.previous.is_none() {
            let bytes = self.to_bytes();

            self.file.as_ref().unwrap().seek(SeekFrom::Start(0))?;
            self.file.as_ref().unwrap().write_all(&bytes)?;
            self.file.as_ref().unwrap().sync_all()?;
            self.previous = Some(bytes);

            return Ok(());
        }

        /* To avoid many small seeks and writes, we will allow for a small sections of unchanged bytes to be included in the write.
         */

        const GAP_TOLERANCE: usize = 4;
        let mut misses = 0;

        let mut start = 0;
        let mut changes = Vec::with_capacity(BLOCK_CAPACITY / 4);

        let current = self.to_bytes();
        let previous = self.previous.as_ref().unwrap();

        for (i, byte) in current.iter().copied().enumerate() {
            let old_byte = previous[i];
            let did_change = byte != old_byte;

            if did_change {
                changes.push(byte);
                misses = 0;
                continue;
            }

            if !changes.is_empty() {
                misses += 1;

                if misses <= GAP_TOLERANCE {
                    changes.push(old_byte);
                    continue;
                }

                self.file.as_ref().unwrap().seek(SeekFrom::Start(start))?;
                self.file.as_ref().unwrap().write_all(&changes)?;
                changes.clear();
                misses = 0;
            }

            start = i as u64;
        }

        self.file.as_ref().unwrap().sync_all()?;

        Ok(())
    }
}

impl Default for DataBlockMeta {
    fn default() -> Self {
        Self::new()
    }
}
