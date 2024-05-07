use crate::{Idx, ThinIdx};

macro_rules! impl_ops {
    (
        $ty:ty => $other:ty;
        $($trait:ident $fn:ident $op:tt);+ $(;)?
    ) => {
        $(
            impl std::ops::$trait for $ty {
                type Output = Self;

                fn $fn(self, rhs: Self) -> Self {
                    Self::new(self.into_usize() $op rhs.into_usize())
                }
            }

            impl std::ops::$trait<usize> for $ty {
                type Output = Self;

                fn $fn(self, rhs: usize) -> Self {
                    Self::new(self.into_usize() $op rhs)
                }
            }

            impl std::ops::$trait<$ty> for usize {
                type Output = usize;

                fn $fn(self, rhs: $ty) -> usize {
                    self $op rhs.into_usize()
                }
            }

            impl std::ops::$trait<std::num::NonZeroUsize> for $ty {
                type Output = Self;

                fn $fn(self, rhs: std::num::NonZeroUsize) -> Self {
                    Self::new(self.into_usize() $op rhs.get())
                }
            }

            impl std::ops::$trait<$ty> for std::num::NonZeroUsize {
                type Output = usize;

                fn $fn(self, rhs: $ty) -> usize {
                    self.get() $op rhs.into_usize()
                }
            }

            impl std::ops::$trait<$other> for $ty {
                type Output = Self;

                fn $fn(self, rhs: $other) -> Self {
                    Self::new(self.into_usize() $op rhs.into_usize())
                }
            }
        )*

        impl std::ops::Not for $ty {
            type Output = Self;

            fn not(self) -> Self {
                Self::new(!self.into_usize())
            }
        }

        impl PartialEq<usize> for $ty {
            fn eq(&self, other: &usize) -> bool {
                self.into_usize() == *other
            }
        }

        impl PartialEq<$ty> for usize {
            fn eq(&self, other: &$ty) -> bool {
                *self == other.into_usize()
            }
        }

        impl PartialOrd<usize> for $ty {
            fn partial_cmp(&self, other: &usize) -> Option<std::cmp::Ordering> {
                self.into_usize().partial_cmp(other)
            }
        }

        impl PartialOrd<$ty> for usize {
            fn partial_cmp(&self, other: &$ty) -> Option<std::cmp::Ordering> {
                self.partial_cmp(&other.into_usize())
            }
        }

        impl PartialEq<std::num::NonZeroUsize> for $ty {
            fn eq(&self, other: &std::num::NonZeroUsize) -> bool {
                self.into_usize() == other.get()
            }
        }

        impl PartialEq<$ty> for std::num::NonZeroUsize {
            fn eq(&self, other: &$ty) -> bool {
                self.get() == other.into_usize()
            }
        }

        impl PartialOrd<std::num::NonZeroUsize> for $ty {
            fn partial_cmp(&self, other: &std::num::NonZeroUsize) -> Option<std::cmp::Ordering> {
                self.into_usize().partial_cmp(&other.get())
            }
        }

        impl PartialOrd<$ty> for std::num::NonZeroUsize {
            fn partial_cmp(&self, other: &$ty) -> Option<std::cmp::Ordering> {
                self.get().partial_cmp(&other.into_usize())
            }
        }

        impl std::iter::Step for $ty {
            fn steps_between(start: &Self, end: &Self) -> Option<usize> {
                usize::steps_between(&start.into_usize(), &end.into_usize())
            }

            fn forward_checked(start: Self, count: usize) -> Option<Self> {
                if let Some(n) = usize::forward_checked(start.into_usize(), count) {
                    if let Ok(n) = Self::new_validated(n) {
                        Some(n)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            fn backward_checked(start: Self, count: usize) -> Option<Self> {
                if let Some(n) = usize::backward_checked(start.into_usize(), count) {
                    if let Ok(n) = Self::new_validated(n) {
                        Some(n)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }

        impl<T> std::ops::Index<$ty> for [T] {
            type Output = T;

            fn index(&self, index: $ty) -> &Self::Output {
                &self[index.into_usize()]
            }
        }

        impl<T> std::ops::IndexMut<$ty> for [T] {
            fn index_mut(&mut self, index: $ty) -> &mut Self::Output {
                &mut self[index.into_usize()]
            }
        }

        impl<T> std::ops::Index<$ty> for Vec<T> {
            type Output = T;

            fn index(&self, index: $ty) -> &Self::Output {
                &self[index.into_usize()]
            }
        }

        impl<T> std::ops::IndexMut<$ty> for Vec<T> {
            fn index_mut(&mut self, index: $ty) -> &mut Self::Output {
                &mut self[index.into_usize()]
            }
        }
    };
}

macro_rules! impl_assign_ops {
    (
        $ty:ty => $other:ty;
        $($trait:ident $fn:ident $op:tt);+ $(;)?
    ) => {
        $(
            impl std::ops::$trait for $ty {
                fn $fn(&mut self, rhs: Self) {
                    *self = Self::new(self.into_usize() $op rhs.into_usize());
                }
            }

            impl std::ops::$trait<usize> for $ty {
                fn $fn(&mut self, rhs: usize) {
                    *self = Self::new(self.into_usize() $op rhs);
                }
            }

            impl std::ops::$trait<$ty> for usize {
                fn $fn(&mut self, rhs: $ty) {
                    *self = *self $op rhs.into_usize();
                }
            }

            impl std::ops::$trait<std::num::NonZeroUsize> for $ty {
                fn $fn(&mut self, rhs: std::num::NonZeroUsize) {
                    *self = Self::new(self.into_usize() $op rhs.get());
                }
            }

            impl std::ops::$trait<$other> for $ty {
                fn $fn(&mut self, rhs: $other) {
                    *self = Self::new(self.into_usize() $op rhs.into_usize());
                }
            }
        )*
    };
}

impl_ops! {
    Idx => ThinIdx;
    Add add +;
    Sub sub -;
    Mul mul *;
    Div div /;
    Rem rem %;
}

impl_ops! {
    ThinIdx => Idx;
    Add add +;
    Sub sub -;
    Mul mul *;
    Div div /;
    Rem rem %;
}

impl_assign_ops! {
    Idx => ThinIdx;
    AddAssign add_assign +;
    SubAssign sub_assign -;
    MulAssign mul_assign *;
    DivAssign div_assign /;
    RemAssign rem_assign %;
}

impl_assign_ops! {
    ThinIdx => Idx;
    AddAssign add_assign +;
    SubAssign sub_assign -;
    MulAssign mul_assign *;
    DivAssign div_assign /;
    RemAssign rem_assign %;
}
