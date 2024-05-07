use std::{mem::MaybeUninit, ptr::NonNull};

use anyhow::Result;
use primitives::{Idx, ThinIdx, O16};

use crate::object_ids::ThinRecordId;

use super::GAP_HEAD;

#[repr(C)]
pub struct SlotData<T> {
    gen_id: Option<O16>,
    record: Option<ThinRecordId>,
    data: MaybeUninit<T>,
}

impl<T: Clone> Clone for SlotData<T> {
    fn clone(&self) -> Self {
        if self.is_gap() {
            Self {
                gen_id: None,
                record: None,
                data: MaybeUninit::uninit(),
            }
        } else {
            Self {
                gen_id: self.gen_id,
                record: self.record,
                data: MaybeUninit::new(unsafe { self.data_unchecked().clone() }),
            }
        }
    }
}

impl<T: Copy> Copy for SlotData<T> {}

impl<T> Default for SlotData<T> {
    fn default() -> Self {
        Self {
            gen_id: None,
            record: None,
            data: MaybeUninit::uninit(),
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SlotData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_gap() {
            let mut d = f.debug_struct("Gap");
            let prev = unsafe { self.previous_gap_unchecked() };

            if prev != GAP_HEAD {
                d.field("next", &prev);
            } else {
                d.field("next", &Option::<usize>::None);
            }

            d.finish()
        } else {
            unsafe { std::fmt::Debug::fmt(self.data_unchecked(), f) }
        }
    }
}

impl<T> SlotData<T> {
    pub fn new(record: Option<impl Into<ThinRecordId>>, data: T) -> Self {
        Self {
            gen_id: Some(O16::new()),
            record: record.map(|x| x.into()),
            data: MaybeUninit::new(data),
        }
    }

    pub fn is_gap(&self) -> bool {
        self.gen_id.is_none()
    }

    /// Returns the previous gap idx in the chain.
    pub fn previous_gap(&self) -> Option<usize> {
        if self.is_gap() {
            Some(unsafe { self.previous_gap_unchecked() })
        } else {
            None
        }
    }

    pub unsafe fn previous_gap_unchecked(&self) -> usize {
        debug_assert!(self.is_gap());
        std::ptr::read_unaligned(self.data.as_ptr() as *const _)
    }

    pub fn thin_record_id(&self) -> Option<ThinRecordId> {
        if self.is_gap() {
            None
        } else {
            self.record
        }
    }

    pub fn data(&self) -> Option<&T> {
        if self.is_gap() {
            None
        } else {
            Some(unsafe { self.data_unchecked() })
        }
    }

    pub fn gen_id(&self) -> Option<O16> {
        if self.is_gap() {
            None
        } else {
            Some(unsafe { self.gen_id_unchecked() })
        }
    }

    pub unsafe fn gen_id_unchecked(&self) -> O16 {
        debug_assert!(!self.is_gap());
        self.gen_id.unwrap_unchecked()
    }

    pub unsafe fn data_unchecked(&self) -> &T {
        debug_assert!(!self.is_gap());
        self.data.assume_init_ref()
    }

    pub unsafe fn data_unchecked_mut(&mut self) -> &mut T {
        debug_assert!(!self.is_gap());
        self.data.assume_init_mut()
    }

    pub unsafe fn read_data_unchecked(&self) -> T {
        debug_assert!(!self.is_gap());
        std::ptr::read(self.data.as_ptr())
    }

    /// Blocks the previous gap idx in the chain.
    pub fn create_gap(&mut self, previous_gap: Option<impl Into<ThinIdx>>) {
        if self.is_gap() {
            return;
        }

        self.record = ThinRecordId::NIL;
        self.gen_id = O16::NIL;

        unsafe {
            std::ptr::write_unaligned(
                self.data.as_mut_ptr() as *mut _,
                previous_gap
                    .map_or(ThinIdx::INVALID, |x| x.into())
                    .into_usize(),
            );
        }
    }

    pub fn fill_gap(&mut self, record: Option<impl Into<ThinRecordId>>, idx: Idx, data: T) {
        #[cfg(debug_assertions)]
        {
            if !self.is_gap() {
                panic!("slot is not a gap");
            }
        }

        self.gen_id = Some(idx.into_gen_id());
        self.record = record.map(|x| x.into());
        self.data = MaybeUninit::new(data);
    }

    pub fn replace(&mut self, data: T) {
        #[cfg(debug_assertions)]
        {
            if self.is_gap() {
                panic!("slot is a gap");
            }
        }

        self.gen_id = Some(O16::new());
        self.data = MaybeUninit::new(data);
    }

    #[must_use]
    pub fn update<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut T) -> Result<R>,
    {
        #[cfg(debug_assertions)]
        {
            if self.is_gap() {
                panic!("slot is a gap");
            }
        }

        f(unsafe { self.data_unchecked_mut() })
    }

    pub fn from_parts(gen_id: O16, record: Option<impl Into<ThinRecordId>>, data: T) -> Self {
        Self {
            gen_id: Some(gen_id),
            record: record.map(|x| x.into()),
            data: MaybeUninit::new(data),
        }
    }

    pub fn into_parts(self) -> Option<(O16, Option<ThinRecordId>, T)> {
        if self.is_gap() {
            None
        } else {
            unsafe {
                Some((
                    self.gen_id.unwrap_unchecked(),
                    self.thin_record_id(),
                    self.read_data_unchecked(),
                ))
            }
        }
    }
}

#[derive(Clone, Copy)]
pub struct SlotDataRef<'a, T>(&'a NonNull<SlotData<T>>);

impl<'a, T> std::ops::Deref for SlotDataRef<'a, T> {
    type Target = SlotData<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl<'a, T> AsRef<SlotData<T>> for SlotDataRef<'a, T> {
    fn as_ref(&self) -> &SlotData<T> {
        unsafe { self.0.as_ref() }
    }
}

impl<'a, T> SlotDataRef<'a, T> {
    pub fn new(ptr: &'a NonNull<SlotData<T>>) -> Self {
        Self(ptr)
    }

    pub fn check_gen(self, expected_gen: O16) -> Result<()> {
        if let Some(gen) = unsafe { self.0.as_ref().gen_id() } {
            if gen != expected_gen {
                anyhow::bail!("slot has been invalidated");
            }
        } else {
            anyhow::bail!("slot is not initialized");
        }

        Ok(())
    }
}
