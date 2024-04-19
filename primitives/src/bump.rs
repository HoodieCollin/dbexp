use bumpalo::Bump;
use bumpalo_herd::{Herd, Member};

use crate::{force_transmute, typed_arc::TypedArc};

#[derive(Clone)]
pub struct BumpPool(TypedArc<Herd>);

impl BumpPool {
    pub fn new() -> Self {
        Self(TypedArc::new(Herd::new()))
    }

    pub fn get_alloc(&self) -> BumpAlloc {
        BumpAlloc {
            _pool: self.clone(),
            alloc: TypedArc::new(unsafe { force_transmute::<_, Member<'static>>(self.0.get()) }),
        }
    }
}

impl Default for BumpPool {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct BumpAlloc {
    _pool: BumpPool,
    alloc: TypedArc<Member<'static>>,
}

impl std::ops::Deref for BumpAlloc {
    type Target = Bump;

    fn deref(&self) -> &Self::Target {
        self.alloc.as_bump()
    }
}

impl AsRef<Bump> for BumpAlloc {
    fn as_ref(&self) -> &Bump {
        self.alloc.as_bump()
    }
}
