use std::{mem::MaybeUninit, ptr::NonNull};

use anyhow::Result;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use primitives::{idx::Gen, ThinIdx};

use crate::object_ids::ThinRecordId;

use super::GAP_HEAD;

#[repr(C)]
pub struct SlotData<T> {
    is_gap: bool,
    record: Option<ThinRecordId>,
    data: MaybeUninit<T>,
}

impl<T: Clone> Clone for SlotData<T> {
    fn clone(&self) -> Self {
        if self.is_gap {
            Self {
                is_gap: true,
                record: None,
                data: MaybeUninit::uninit(),
            }
        } else {
            Self {
                is_gap: false,
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
            is_gap: true,
            record: None,
            data: MaybeUninit::uninit(),
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SlotData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_gap {
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
            is_gap: false,
            record: record.map(|x| x.into()),
            data: MaybeUninit::new(data),
        }
    }

    pub fn is_gap(&self) -> bool {
        self.is_gap
    }

    /// Returns the previous gap idx in the chain.
    pub fn previous_gap(&self) -> Option<usize> {
        if self.is_gap {
            Some(unsafe { self.previous_gap_unchecked() })
        } else {
            None
        }
    }

    pub unsafe fn previous_gap_unchecked(&self) -> usize {
        debug_assert!(self.is_gap);
        std::ptr::read_unaligned(self.data.as_ptr() as *const _)
    }

    pub fn thin_record_id(&self) -> Option<ThinRecordId> {
        if self.is_gap {
            None
        } else {
            self.record
        }
    }

    pub fn data(&self) -> Option<&T> {
        if self.is_gap {
            None
        } else {
            Some(unsafe { self.data_unchecked() })
        }
    }

    pub unsafe fn data_unchecked(&self) -> &T {
        debug_assert!(!self.is_gap);
        self.data.assume_init_ref()
    }

    pub unsafe fn data_unchecked_mut(&mut self) -> &mut T {
        debug_assert!(!self.is_gap);
        self.data.assume_init_mut()
    }

    pub unsafe fn read_data_unchecked(&self) -> T {
        debug_assert!(!self.is_gap);
        std::ptr::read(self.data.as_ptr())
    }

    /// Blocks the previous gap idx in the chain.
    pub fn create_gap(&mut self, previous_gap: Option<impl Into<ThinIdx>>) {
        if self.is_gap {
            return;
        }

        self.is_gap = true;
        self.record = ThinRecordId::NIL;

        unsafe {
            std::ptr::write_unaligned(
                self.data.as_mut_ptr() as *mut _,
                previous_gap
                    .map_or(ThinIdx::INVALID, |x| x.into())
                    .into_usize(),
            );
        }
    }

    pub fn fill_gap(&mut self, record: Option<impl Into<ThinRecordId>>, data: T) {
        #[cfg(debug_assertions)]
        {
            if !self.is_gap {
                panic!("slot is not a gap");
            }
        }

        self.is_gap = false;
        self.record = record.map(|x| x.into());
        self.data = MaybeUninit::new(data);
    }

    pub fn replace(&mut self, data: T) {
        #[cfg(debug_assertions)]
        {
            if self.is_gap {
                panic!("slot is a gap");
            }
        }

        self.data = MaybeUninit::new(data);
    }

    #[must_use]
    pub fn update<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut T) -> Result<R>,
    {
        #[cfg(debug_assertions)]
        {
            if self.is_gap {
                panic!("slot is a gap");
            }
        }

        f(unsafe { self.data_unchecked_mut() })
    }

    /// Returns the record and data if the slot is not a gap.
    pub fn copy_parts(&self) -> Option<(Option<ThinRecordId>, T)>
    where
        T: Copy,
    {
        if self.is_gap {
            None
        } else {
            unsafe { Some((self.thin_record_id(), self.read_data_unchecked())) }
        }
    }

    /// Same as `copy_parts` but doesn't require `T: Copy`.
    ///
    /// > ## Safety
    /// > The caller must ensure that they have exclusive ownership of the slot and
    /// > immediately converts it to a gap after calling this method.
    ///
    /// > ### *Note:*
    /// > *This method is safe to call if the slot is a gap.*
    pub unsafe fn read_parts(&self) -> Option<(Option<ThinRecordId>, T)> {
        if self.is_gap {
            None
        } else {
            Some((self.thin_record_id(), self.read_data_unchecked()))
        }
    }

    pub fn check_gen(&self, expected_gen: Gen) -> Result<()> {
        if let Some(record) = self.thin_record_id() {
            if record.gen() != expected_gen {
                anyhow::bail!("record gen id mismatch");
            }
        }

        Ok(())
    }
}

pub struct SlotDataRef<'a, T>(RwLockReadGuard<'a, NonNull<SlotData<T>>>);

impl<T: std::fmt::Debug> std::fmt::Debug for SlotDataRef<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { std::fmt::Debug::fmt(self.0.as_ref(), f) }
    }
}

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
    pub fn new(rw: &'a RwLock<NonNull<SlotData<T>>>) -> Self {
        Self(rw.read())
    }

    pub fn from_guard(guard: RwLockReadGuard<'a, NonNull<SlotData<T>>>) -> Self {
        Self(guard)
    }

    pub fn unwrap_guard(self) -> RwLockReadGuard<'a, NonNull<SlotData<T>>> {
        self.0
    }
}

pub struct SlotDataMut<'a, T>(RwLockWriteGuard<'a, NonNull<SlotData<T>>>);

impl<T: std::fmt::Debug> std::fmt::Debug for SlotDataMut<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { std::fmt::Debug::fmt(self.0.as_ref(), f) }
    }
}

impl<'a, T> std::ops::Deref for SlotDataMut<'a, T> {
    type Target = SlotData<T>;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl<'a, T> std::ops::DerefMut for SlotDataMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl<'a, T> AsRef<SlotData<T>> for SlotDataMut<'a, T> {
    fn as_ref(&self) -> &SlotData<T> {
        unsafe { self.0.as_ref() }
    }
}

impl<'a, T> AsMut<SlotData<T>> for SlotDataMut<'a, T> {
    fn as_mut(&mut self) -> &mut SlotData<T> {
        unsafe { self.0.as_mut() }
    }
}

impl<'a, T> SlotDataMut<'a, T> {
    pub fn new(rw: &'a RwLock<NonNull<SlotData<T>>>) -> Self {
        Self(rw.write())
    }

    pub fn from_guard(guard: RwLockWriteGuard<'a, NonNull<SlotData<T>>>) -> Self {
        Self(guard)
    }

    pub fn unwrap_guard(self) -> RwLockWriteGuard<'a, NonNull<SlotData<T>>> {
        self.0
    }
}
