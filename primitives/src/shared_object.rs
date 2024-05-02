use std::sync::{Arc, Weak};

use anyhow::Result;
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};

#[derive(Default)]
#[repr(transparent)]
pub struct SharedObject<T: 'static>(Arc<RwLock<T>>);

pub type WeakObjectRef<T> = Weak<RwLock<T>>;

unsafe impl<T: Send> Send for SharedObject<T> {}
unsafe impl<T: Send + Sync> Sync for SharedObject<T> {}

impl<T> SharedObject<T> {
    pub fn new(value: T) -> Self {
        Self(Arc::new(RwLock::new(value)))
    }

    pub fn new_copy(&self) -> Self
    where
        T: Clone,
    {
        Self(self.0.clone())
    }

    pub fn weak_ref(&self) -> WeakObjectRef<T> {
        Arc::downgrade(&self.0)
    }

    pub fn unwrap_or_clone(self) -> T
    where
        T: Clone,
    {
        match Arc::try_unwrap(self.0) {
            Ok(inner) => inner.into_inner(),
            Err(arc) => Self(arc).read_with(|inner| inner.clone()),
        }
    }

    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        match Arc::try_unwrap(this.0) {
            Ok(inner) => Ok(inner.into_inner()),
            Err(arc) => Err(Self(arc)),
        }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read()
    }

    pub fn read_recursive(&self) -> RwLockReadGuard<'_, T> {
        self.0.read_recursive()
    }

    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        f(&*self.0.read())
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read` and `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        f(&*self.0.read_recursive())
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.write()
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        f(&mut *self.0.write())
    }

    pub fn upgradable(&self) -> SharedObjectReadGuard<'_, T> {
        SharedObjectReadGuard(self.0.upgradable_read())
    }

    pub fn downgradable(&self) -> SharedObjectWriteGuard<'_, T> {
        self.upgradable().upgrade()
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SharedObject<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.read_recursive_with(|inner| write!(f, "{:?}", inner))
    }
}

impl<T> Clone for SharedObject<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> PartialEq for SharedObject<T> {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0.data_ptr() as _, other.0.data_ptr() as _)
    }
}

impl<T> Eq for SharedObject<T> {}

impl<T: PartialOrd + PartialEq> PartialOrd for SharedObject<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.read_recursive_with(|inner| {
            other.read_recursive_with(|other_inner| inner.partial_cmp(other_inner))
        })
    }
}

impl<T: PartialOrd + Ord + PartialEq + Eq> Ord for SharedObject<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.read_recursive_with(|inner| {
            other.read_recursive_with(|other_inner| inner.cmp(other_inner))
        })
    }
}

impl<T: std::hash::Hash + Eq> std::hash::Hash for SharedObject<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.read_recursive_with(|inner| inner.hash(state))
    }
}

impl<T: Serialize> Serialize for SharedObject<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.read_recursive_with(|inner| inner.serialize(serializer))
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SharedObject<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::new(T::deserialize(deserializer)?))
    }
}

pub struct SharedObjectReadGuard<'a, T>(RwLockUpgradableReadGuard<'a, T>);

impl<'a, T> SharedObjectReadGuard<'a, T> {
    pub fn upgrade(self) -> SharedObjectWriteGuard<'a, T> {
        SharedObjectWriteGuard(RwLockUpgradableReadGuard::upgrade(self.0))
    }
}

impl<'a, T> std::ops::Deref for SharedObjectReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<'a, T> AsRef<T> for SharedObjectReadGuard<'a, T> {
    fn as_ref(&self) -> &T {
        &*self.0
    }
}

pub struct SharedObjectWriteGuard<'a, T>(RwLockWriteGuard<'a, T>);

impl<'a, T> SharedObjectWriteGuard<'a, T> {
    pub fn downgrade(self) -> SharedObjectReadGuard<'a, T> {
        SharedObjectReadGuard(RwLockWriteGuard::downgrade_to_upgradable(self.0))
    }
}

impl<'a, T> std::ops::Deref for SharedObjectWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<'a, T> std::ops::DerefMut for SharedObjectWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.0
    }
}

impl<'a, T> AsRef<T> for SharedObjectWriteGuard<'a, T> {
    fn as_ref(&self) -> &T {
        &*self.0
    }
}

impl<'a, T> AsMut<T> for SharedObjectWriteGuard<'a, T> {
    fn as_mut(&mut self) -> &mut T {
        &mut *self.0
    }
}
