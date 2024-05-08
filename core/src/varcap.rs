use std::ops::RangeBounds;

use anyhow::Result;

use primitives::{
    shared_object::{SharedObject, SharedObjectReadGuard, SharedObjectWriteGuard},
    ThinIdx,
};

use crate::{
    block::{self, Block},
    object_ids::{RecordId, TableId},
    slot::{SlotHandle, SlotTuple},
};

use self::{config::VarcapConfig, inner::VarcapInner};

pub mod config;
pub mod inner;
pub mod relay;

pub struct Varcap(SharedObject<VarcapInner>);

impl Clone for Varcap {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Varcap {
    pub fn new(table: Option<TableId>, config: VarcapConfig) -> Result<Self> {
        let varcap = Self(SharedObject::new(VarcapInner::new(table, config)?));

        if config.persistance.is_empty() {
            varcap.load(..)?;
        }

        Ok(varcap)
    }

    pub fn load(&self, r: impl RangeBounds<usize>) -> Result<()> {
        let inner = self.0.upgradable();

        // short-circuit if all blocks are already loaded
        if inner.relay.blocks.len() == inner.relay.meta.block_count.get() {
            return Ok(());
        }

        let (start, end_inclusive) = inner.relay._resolve_range(r)?;
        let mut needed = None;

        for (index, block) in inner.relay._get_block_range(start, end_inclusive) {
            if block.is_none() {
                if needed.is_none() {
                    needed = Some(Vec::with_capacity((end_inclusive - start + 1).into_usize()));
                }

                needed.as_mut().unwrap().push(index);
            }
        }

        let mut inner = inner.upgrade();

        for index in needed.unwrap_or_default() {
            inner.relay._create_block(index)?;
        }

        Ok(())
    }

    pub fn read(&self) -> SharedObjectReadGuard<VarcapInner> {
        self.0.upgradable()
    }

    pub fn write(&self) -> SharedObjectWriteGuard<VarcapInner> {
        self.0.upgradable().upgrade()
    }

    // TODO: implement insert methods
}
