use anyhow::Result;
use fraction::CheckedAdd;

use crate::{decimal, integer, ratio, timestamp};

use super::{DataType, DataValue};
use DataType::{
    Decimal as DecimalTy, Integer as IntegerTy, Ratio as RatioTy, Timestamp as TimestampTy,
};
use DataValue::{Decimal, Integer, Ratio, Timestamp};

impl DataValue {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_addition() -> Result<()> {
        let a = DataValue::Integer(integer::Integer::try_from_number(1)?);
        let b = DataValue::Integer(integer::Integer::try_from_number(2)?);
        let c = a.try_add(&b)?;
        assert_eq!(c, DataValue::Integer(integer::Integer::try_from_number(3)?));

        let a = DataValue::Ratio(ratio::Ratio::try_from_parts(1, 2)?);
        let b = DataValue::Ratio(ratio::Ratio::try_from_parts(1, 3)?);
        let c = a.try_add(&b)?;
        assert_eq!(c, DataValue::Ratio(ratio::Ratio::try_from_parts(5, 6)?));

        let a = DataValue::Decimal(decimal::Decimal::try_from_number(1.0)?);
        let b = DataValue::Decimal(decimal::Decimal::try_from_number(2.0)?);
        let c = a.try_add(&b)?;
        assert_eq!(
            c,
            DataValue::Decimal(decimal::Decimal::try_from_number(3.0)?)
        );

        let a = DataValue::Timestamp(timestamp::Timestamp::from_integer(1)?);
        let b = DataValue::Timestamp(timestamp::Timestamp::from_integer(2)?);
        let c = a.try_add(&b)?;
        assert_eq!(
            c,
            DataValue::Timestamp(timestamp::Timestamp::from_integer(3)?)
        );

        Ok(())
    }
}
