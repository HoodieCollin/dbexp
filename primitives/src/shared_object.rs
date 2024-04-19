use anyhow::Result;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::Serialize;

use crate::{force_transmute, typed_arc::TypedArc};

#[repr(transparent)]
pub struct SharedObject<T: 'static>(TypedArc<RwLock<T>>);

unsafe impl<T: Send> Send for SharedObject<T> {}
unsafe impl<T: Send + Sync> Sync for SharedObject<T> {}

impl<T> SharedObject<T> {
    pub fn new(value: T) -> Self {
        Self(TypedArc::new(RwLock::new(value)))
    }

    pub fn new_copy(&self) -> Self
    where
        T: Clone,
    {
        Self(self.0.clone())
    }

    pub fn unwrap_or_clone(self) -> T
    where
        T: Clone,
    {
        match TypedArc::try_unwrap(self.0) {
            Ok(inner) => inner.into_inner(),
            Err(arc) => Self(arc).read_with(|inner| inner.clone()),
        }
    }

    pub fn try_unwrap(this: Self) -> Result<T, Self> {
        match TypedArc::try_unwrap(this.0) {
            Ok(inner) => Ok(inner.into_inner()),
            Err(arc) => Err(Self(arc)),
        }
    }

    pub fn read_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        f(&*self.0.read())
    }

    /// ## !!! WARNING !!!
    ///
    /// This variant of `read_with` allows the same thread to bypass any waiting readers,
    /// which can lead to starvation on those threads.
    pub fn read_recursive_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        f(&*self.0.read_recursive())
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn read_guard(&self) -> SharedObjectRef<T> {
        SharedObjectRef {
            _src: self.0.clone(),
            guard: unsafe { force_transmute::<_, RwLockReadGuard<'static, T>>(self.0.read()) },
        }
    }

    pub fn write_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        f(&mut *self.0.write())
    }

    /// ## !!! WARNING !!!
    ///
    /// While this function is safe to use, it can easily lead to deadlocks if not used properly.
    ///
    /// Prefer using `read_with` or `write_with` instead.
    pub fn write_guard(&self) -> SharedObjectMut<T> {
        SharedObjectMut {
            _src: self.0.clone(),
            guard: unsafe { force_transmute::<_, RwLockWriteGuard<'static, T>>(self.0.write()) },
        }
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

pub struct SharedObjectRef<T: 'static> {
    _src: TypedArc<RwLock<T>>,
    guard: RwLockReadGuard<'static, T>,
}

impl<T> std::ops::Deref for SharedObjectRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> AsRef<T> for SharedObjectRef<T> {
    fn as_ref(&self) -> &T {
        &self.guard
    }
}

pub struct SharedObjectMut<T: 'static> {
    _src: TypedArc<RwLock<T>>,
    guard: RwLockWriteGuard<'static, T>,
}

impl<T> std::ops::Deref for SharedObjectMut<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> std::ops::DerefMut for SharedObjectMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<T> AsRef<T> for SharedObjectMut<T> {
    fn as_ref(&self) -> &T {
        &self.guard
    }
}

impl<T> AsMut<T> for SharedObjectMut<T> {
    fn as_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}
