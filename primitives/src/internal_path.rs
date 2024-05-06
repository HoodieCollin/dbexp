use std::{
    collections::HashMap, ffi::OsStr, mem::MaybeUninit, os::unix::ffi::OsStrExt, path::Path,
};

use anyhow::Result;
use parking_lot::{Once, RwLock, RwLockUpgradableReadGuard};

use crate::{
    byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
    impl_access_bytes_for_into_bytes_type,
};

const MAX_LEN: usize = 4096;

#[derive(Clone, Copy)]
pub struct InternalPath(&'static Path);

impl Default for InternalPath {
    fn default() -> Self {
        Self(Path::new(OsStr::new("")))
    }
}

impl std::ops::Deref for InternalPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        self.as_path()
    }
}

impl AsRef<Path> for InternalPath {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl AsRef<[u8]> for InternalPath {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::fmt::Debug for InternalPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_path().fmt(f)
    }
}

impl PartialEq for InternalPath {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for InternalPath {}

impl std::hash::Hash for InternalPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}

impl_access_bytes_for_into_bytes_type!(InternalPath);

impl IntoBytes for InternalPath {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.len())?;
        x.encode_bytes(self.as_slice())?;
        Ok(())
    }
}

impl FromBytes for InternalPath {
    fn decode_bytes(this: &mut Self, x: &mut ByteDecoder<'_>) -> Result<()> {
        use std::cell::RefCell;

        let mut len = 0usize;
        x.decode(&mut len)?;

        thread_local! {
            static BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(MAX_LEN));
        }

        BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            buf.clear();
            buf.resize(len, 0);

            x.read_exact(&mut buf[..])?;

            let path = Path::new(OsStr::from_bytes(&buf[..len]));
            let interned = InternalPath::new(path)?;

            *this = interned;

            Ok(())
        })
    }
}

impl InternalPath {
    fn interned_store() -> &'static RwLock<HashMap<u64, &'static OsStr>> {
        static mut INTERNED: MaybeUninit<RwLock<HashMap<u64, &'static OsStr>>> =
            MaybeUninit::uninit();

        static INIT: Once = Once::new();

        INIT.call_once(|| unsafe {
            let interned = RwLock::new(HashMap::with_capacity(128));
            std::ptr::write(INTERNED.as_mut_ptr(), interned);
        });

        unsafe { &*INTERNED.as_ptr() }
    }

    pub fn new(p: impl AsRef<Path>) -> Result<Self> {
        use std::hash::{DefaultHasher, Hash, Hasher};

        let p = p.as_ref();
        let mut hasher = DefaultHasher::new();

        let store = Self::interned_store().upgradable_read();

        p.hash(&mut hasher);
        let id = hasher.finish();

        if let Some(interned) = store.get(&id) {
            Ok(Self(Path::new(*interned)))
        } else {
            let mut store = RwLockUpgradableReadGuard::upgrade(store);
            let leaked = &*Box::leak(p.as_os_str().to_owned().into_boxed_os_str());

            store.insert(id, leaked);
            drop(store);

            Ok(Self(Path::new(leaked)))
        }
    }

    pub fn len(&self) -> usize {
        self.0.as_os_str().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_path(&self) -> &Path {
        self.0
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_os_str().as_bytes()
    }
}
