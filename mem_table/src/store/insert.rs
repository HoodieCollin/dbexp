use crate::{
    object_ids::RecordId,
    store::slot::{SlotHandle, SlotTuple},
};

pub enum InsertState<T: 'static> {
    NoOp,
    Done(Vec<SlotHandle<T>>),
    Partial {
        errors: Vec<InsertError<T>>,
        handles: Vec<SlotHandle<T>>,
        iter: Option<Box<dyn Iterator<Item = SlotTuple<T>>>>,
    },
}

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
}

impl<T> std::fmt::Debug for InsertError<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        }

        d.finish_non_exhaustive()
    }
}
