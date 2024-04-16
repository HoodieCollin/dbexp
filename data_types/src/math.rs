use anyhow::Result;
use fraction::CheckedAdd;

use crate::{decimal, integer, ratio, timestamp};

use super::{DataType, DataValue, ExpectedType};
use DataType::{
    Bool as BoolTy, Decimal as DecimalTy, Integer as IntegerTy, Ratio as RatioTy,
    Timestamp as TimestampTy,
};
use DataValue::{Bool, Bytes, Decimal, Integer, Ratio, Text, Timestamp};

impl DataValue {
    pub fn is_integer(&self) -> bool {
        match self {
            Integer(_) => true,
            Ratio(r) => r.is_integer(),
            Decimal(d) => d.is_integer(),
            _ => false,
        }
    }

    pub fn is_float(&self) -> bool {
        match self {
            Ratio(r) => !r.is_integer(),
            Decimal(d) => !d.is_integer(),
            _ => false,
        }
    }

    pub fn try_cast(&self, ty: impl Into<ExpectedType>) -> Result<Self> {
        let ty: ExpectedType = ty.into();
        let ty = ty.into_inner();

        match ty {
            BoolTy => match self {
                Bool(_) => Ok(self.clone()),
                Integer(i) => Ok(Bool(i.into_inner() != 0)),
                Ratio(r) => Ok(Bool(r.is_integer() && r.numer() != &0)),
                Decimal(d) => Ok(Bool(d.is_integer() && d.to_integer() != 0)),
                Text(t) => Ok(Bool(!t.is_empty())),
                Bytes(b) => Ok(Bool(!b.is_empty())),
                _ => anyhow::bail!("cannot cast {:?} to bool", self),
            },
            IntegerTy(size) => match self {
                Integer(_) => Ok(self.clone()),
                Ratio(r) => Ok(Self::Integer(
                    integer::Integer::try_from_number(r.to_integer())?.try_to_fit(size)?,
                )),
                Decimal(d) => Ok(Self::Integer(
                    integer::Integer::try_from_number(d.to_integer())?.try_to_fit(size)?,
                )),
                Timestamp(t) => Ok(Self::Integer(
                    integer::Integer::try_from_number(t.to_integer())?.try_to_fit(size)?,
                )),
                _ => anyhow::bail!("cannot cast {:?} to integer", self),
            },
            RatioTy => match self {
                Integer(i) => Self::try_ratio_from_number(i.into_inner()),
                Ratio(_) => Ok(self.clone()),
                Decimal(d) => Self::try_ratio_from_str(&d.to_string()),
                _ => anyhow::bail!("cannot cast {:?} to ratio", self),
            },
            DecimalTy => match self {
                Integer(i) => Self::try_decimal_from_number(i.into_inner()),
                Ratio(r) => Self::try_decimal_from_str(&r.to_string()),
                Decimal(_) => Ok(self.clone()),
                _ => anyhow::bail!("cannot cast {:?} to decimal", self),
            },
            _ => anyhow::bail!("cannot cast {:?} to {:?}", self, ty),
        }
    }

    pub fn try_add(&self, other: &DataValue) -> Result<DataValue> {
        if self.is_nil() || other.is_nil() {
            anyhow::bail!("cannot add nil value");
        }

        match (self, other) {
            (Integer(a), b) => b
                .try_cast(IntegerTy(a.size()))
                .and_then(|b| {
                    a.into_inner()
                        .checked_add(match b {
                            Integer(b) => b.into_inner(),
                            _ => unreachable!(),
                        })
                        .ok_or_else(|| anyhow::anyhow!("overflow"))
                })
                .and_then(integer::Integer::try_from_number)
                .and_then(|i| i.try_to_fit(a.size()))
                .map(Integer),
            (Ratio(a), b) => b
                .try_cast(RatioTy)
                .and_then(|b| {
                    a.into_inner()
                        .checked_add(&match b {
                            Ratio(b) => b.into_inner(),
                            _ => unreachable!(),
                        })
                        .ok_or_else(|| anyhow::anyhow!("overflow"))
                })
                .map(|r| Ratio(unsafe { ratio::Ratio::from_raw_ratio(r) })),
            (Decimal(a), b) => b
                .try_cast(DecimalTy)
                .and_then(|b| {
                    a.into_inner()
                        .checked_add(match b {
                            Decimal(b) => b.into_inner(),
                            _ => unreachable!(),
                        })
                        .ok_or_else(|| anyhow::anyhow!("overflow"))
                })
                .map(|d| Decimal(decimal::Decimal::from_raw_decimal(d))),
            (Timestamp(a), b) => {
                let a = a.to_integer() as i128;
                let b = b.try_cast(TimestampTy).map(|b| match b {
                    Timestamp(t) => t.to_integer() as i128,
                    _ => unreachable!(),
                })?;

                let sum = a
                    .checked_add(b)
                    .ok_or_else(|| anyhow::anyhow!("overflow"))?;

                Ok(Timestamp(timestamp::Timestamp::from_integer(
                    sum.try_into()?,
                )?))
            }
            _ => anyhow::bail!("cannot add {:?} and {:?}", self, other),
        }
    }
}
