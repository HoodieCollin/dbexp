use crate::{object_ids::RecordId, slot::SlotTuple};

#[derive(thiserror::Error)]
pub enum InsertError<T> {
    #[error("record table mismatch")]
    TableMismatch {
        item: SlotTuple<T>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
    #[error("record already exists")]
    AlreadyExists {
        item: SlotTuple<T>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
    #[error("block is full")]
    BlockFull {
        item: Option<SlotTuple<T>>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
    #[error("invalid value")]
    InvalidValue {
        item: SlotTuple<T>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
        #[source]
        error: anyhow::Error,
    },
    #[error("no values to insert")]
    NoValues,
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

impl<T> std::fmt::Debug for InsertError<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoValues => return std::fmt::Debug::fmt("InsertError: no values to insert", f),
            Self::Unexpected(e) => return std::fmt::Debug::fmt(e, f),
            _ => {
                // continue
            }
        }

        struct ItemDetail<U> {
            record: RecordId,
            data: U,
        }

        impl<U: std::fmt::Debug> std::fmt::Debug for ItemDetail<U> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_map()
                    .entry(&"record", &self.record)
                    .entry(&"data", &self.data)
                    .finish()
            }
        }

        let mut d = f.debug_struct("InsertError");

        d.field("error", &self.to_string());

        match self {
            Self::TableMismatch { item, .. } | Self::AlreadyExists { item, .. } => {
                d.field(
                    "item",
                    &ItemDetail {
                        record: item.0,
                        data: &item.1,
                    },
                );
            }
            Self::BlockFull { item, .. } => {
                if let Some((record, data)) = item {
                    d.field(
                        "item",
                        &ItemDetail {
                            record: *record,
                            data,
                        },
                    );
                } else {
                    d.field("item", &Option::<ItemDetail<T>>::None);
                }
            }
            Self::InvalidValue { error, item, .. } => {
                d.field("cause", error);

                d.field(
                    "item",
                    &ItemDetail {
                        record: item.0,
                        data: &item.1,
                    },
                );
            }
            Self::NoValues => unreachable!("handled above"),
            Self::Unexpected(..) => unreachable!("handled above"),
        }

        d.finish_non_exhaustive()
    }
}

#[derive(Debug, thiserror::Error)]
pub struct BlockCreationError {
    #[source]
    pub error: anyhow::Error,
}

impl std::fmt::Display for BlockCreationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.error)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError<T> {
    #[error(transparent)]
    BlockCreationError(#[from] BlockCreationError),
    #[error(transparent)]
    InsertError(#[from] InsertError<T>),
    #[error("block was not found??? (this should never happen)")]
    BlockNotFound,
    #[error(transparent)]
    Unexpected(#[from] anyhow::Error),
}

impl<T> StoreError<T> {
    pub fn thread_safe(self) -> anyhow::Error {
        anyhow::Error::msg(self.to_string())
    }
}
