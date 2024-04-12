pub mod decimal;
pub mod oid;
pub mod ratio;
pub mod timestamp;
pub mod uid;

pub enum DataType {
    Bool,
    U8,
    U16,
    U32,
    U64,
    U128,
    I8,
    I16,
    I32,
    I64,
    I128,
    R16,
    R32,
    R64,
    R128,
    Uid,
    O16,
    O32,
    Decimal,
    Timestamp,
    String { max_length: u32 },
    Bytes { max_length: u32 },
}
