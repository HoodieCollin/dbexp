use std::{collections::HashMap, mem::MaybeUninit};

use anyhow::Result;
use parking_lot::{Once, RwLock, RwLockUpgradableReadGuard};

use crate::{
    byte_encoding::{ByteDecoder, ByteEncoder, FromBytes, IntoBytes},
    impl_access_bytes_for_into_bytes_type,
};

const MAX_LEN: usize = 4096;

#[derive(Clone, Copy)]
pub struct InternalString(&'static str);

impl Default for InternalString {
    fn default() -> Self {
        Self("")
    }
}

impl std::ops::Deref for InternalString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl AsRef<str> for InternalString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Debug for InternalString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

impl PartialEq for InternalString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for InternalString {}

impl std::hash::Hash for InternalString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl_access_bytes_for_into_bytes_type!(InternalString);

impl IntoBytes for InternalString {
    fn encode_bytes(&self, x: &mut ByteEncoder<'_>) -> Result<()> {
        x.encode(self.len())?;
        x.encode_bytes(self.as_str().as_bytes())?;
        Ok(())
    }
}

impl FromBytes for InternalString {
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

            let s = std::str::from_utf8(&buf[..len])?;
            *this = InternalString::new(s)?;

            Ok(())
        })
    }
}

impl TryFrom<&str> for InternalString {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

impl TryFrom<String> for InternalString {
    type Error = anyhow::Error;

    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}

impl TryFrom<&String> for InternalString {
    type Error = anyhow::Error;

    fn try_from(s: &String) -> Result<Self> {
        Self::new(s)
    }
}

impl InternalString {
    fn interned_store() -> &'static RwLock<HashMap<u64, &'static str>> {
        static mut INTERNED: MaybeUninit<RwLock<HashMap<u64, &'static str>>> =
            MaybeUninit::uninit();

        static INIT: Once = Once::new();

        INIT.call_once(|| unsafe {
            let interned = RwLock::new(HashMap::with_capacity(128));
            std::ptr::write(INTERNED.as_mut_ptr(), interned);
        });

        unsafe { &*INTERNED.as_ptr() }
    }

    pub fn new(s: impl AsRef<str>) -> Result<Self> {
        use std::hash::{DefaultHasher, Hash, Hasher};

        let s = s.as_ref();
        let mut hasher = DefaultHasher::new();

        let store = Self::interned_store().upgradable_read();

        s.hash(&mut hasher);
        let id = hasher.finish();

        if let Some(interned) = store.get(&id) {
            Ok(Self(*interned))
        } else {
            let mut store = RwLockUpgradableReadGuard::upgrade(store);
            let leaked = &*s.to_owned().leak();

            store.insert(id, leaked);
            drop(store);

            Ok(Self(leaked))
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.len() == 0
    }

    pub fn as_str(&self) -> &str {
        self.0
    }
}
