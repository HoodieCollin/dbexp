use std::{
    collections::HashMap,
    fs::{self, File},
    os::unix::fs::FileExt,
    sync::Arc,
};

use anyhow::Result;

use primitives::byte_encoding::{FromBytes, IntoBytes};

use crate::{
    object_ids::TableId,
    store::{Block, StoreConfig, StoreMeta},
};

pub struct StoreInner<T: 'static> {
    pub(super) meta: StoreMeta,
    pub(super) file: Option<Arc<File>>,
    pub(super) blocks: HashMap<usize, Block<T>>,
}

impl<T> StoreInner<T> {
    pub fn new(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();

        if config.persistance.is_empty() {
            Self::new_memory_only(table, Some(config))
        } else {
            Self::new_persisted(table, Some(config))
        }
    }

    pub fn new_memory_only(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let config = config.unwrap_or_default();

        if !config.persistance.is_empty() {
            eprintln!("WARNING: persistance path is ignored for memory-only store")
        }

        Ok(Self {
            meta: StoreMeta::new(table, Some(config)),
            file: None,
            blocks: HashMap::with_capacity(config.initial_block_count),
        })
    }

    pub fn new_persisted(table: Option<TableId>, config: Option<StoreConfig>) -> Result<Self> {
        let table = table.unwrap_or_else(|| TableId::new());
        let config = config.unwrap_or_default();

        if config.persistance.is_empty() {
            anyhow::bail!("persistance path is required for persisted store");
        }

        let path = config.persistance.as_path();
        let parent_dir = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("path has no parent"))?;

        let (meta, file) = if !path.exists() {
            fs::create_dir_all(parent_dir)?;

            let meta = StoreMeta::new(Some(table), Some(config));

            let file = File::create_new(path)?;
            file.set_len(meta.capacity_as_bytes::<T>() as u64)?;
            file.write_all_at(&meta.into_bytes()?, 0)?;

            (meta, file)
        } else {
            let file = fs::OpenOptions::new().read(true).write(true).open(&path)?;

            let fs_meta = file.metadata()?;

            if fs_meta.len() < StoreMeta::BYTE_COUNT as u64 {
                anyhow::bail!("file is too small");
            }

            let mut meta_bytes = [0u8; StoreMeta::BYTE_COUNT];
            file.read_exact_at(&mut meta_bytes, 0)?;

            let meta = StoreMeta::from_bytes(&meta_bytes)?;

            let expected_size = meta.capacity_as_bytes::<T>() as usize;
            let actual_len = (fs_meta.len() - StoreMeta::BYTE_COUNT as u64) as usize;

            if actual_len != expected_size {
                anyhow::bail!("file size does not match metadata");
            }

            (meta, file)
        };

        Ok(Self {
            meta,
            file: Some(Arc::new(file)),
            blocks: HashMap::with_capacity(meta.block_count),
        })
    }
}