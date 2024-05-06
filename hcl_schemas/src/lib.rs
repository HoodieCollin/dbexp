use anyhow::Result;
use data_types::{bytes::Bytes, text::Text, DataType};
use hcl::{
    eval::{Context, Evaluate},
    Block, Body, Expression,
};

use primitives::InternalString;

#[derive(Debug, Clone, Copy)]
pub struct ColumnDef {
    name: InternalString,
    data_type: DataType,
}

impl ColumnDef {
    pub fn name(&self) -> &InternalString {
        &self.name
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}

const EMAIL_TYPE: DataType = DataType::Text(120);
const PHONE_TYPE: DataType = DataType::Text(20);

fn parse_data_type(input: &Expression, ctx: &Context) -> Result<DataType> {
    use Expression::{FuncCall, Variable};

    match input {
        Variable(name) => match name.as_str() {
            "Number" => Ok(DataType::Number),
            "Email" => Ok(EMAIL_TYPE),
            "Phone" => Ok(PHONE_TYPE),
            "Timestamp" => Ok(DataType::Timestamp),
            "Text" => anyhow::bail!("Expected Text to have a length"),
            _ => anyhow::bail!("Unknown data type: {}", name.as_str()),
        },
        FuncCall(f) => {
            let name = InternalString::new(f.name.as_str())?;

            if f.args.len() != 1 {
                anyhow::bail!("Expected exactly one argument for type constructor");
            }

            match name.as_str() {
                "Text" => {
                    let max_len = f.args[0].evaluate(ctx)?.as_u64().ok_or_else(|| {
                        anyhow::anyhow!("Expected positive integer argument for Text")
                    })?;

                    if max_len > Text::MAX_LEN as u64 {
                        anyhow::bail!("Text length is too large");
                    }

                    Ok(DataType::Text(max_len as u32))
                }
                "Bytes" => {
                    let max_len = f.args[0].evaluate(ctx)?.as_u64().ok_or_else(|| {
                        anyhow::anyhow!("Expected positive integer argument for Bytes")
                    })?;

                    if max_len > Bytes::MAX_LEN as u64 {
                        anyhow::bail!("Bytes length is too large");
                    }

                    Ok(DataType::Bytes(max_len as u32))
                }
                _ => anyhow::bail!("Unknown data type: {}", name.as_str()),
            }
        }
        _ => anyhow::bail!("Expected variable or function call for data type"),
    }
}

#[derive(Debug, Clone)]
pub struct TableDef {
    name: InternalString,
    columns: Vec<ColumnDef>,
}

impl<'a> TryFrom<(&Block, &Context<'a>)> for TableDef {
    type Error = anyhow::Error;

    fn try_from(src: (&Block, &Context)) -> Result<Self> {
        let (block, ctx) = src;

        if block.identifier() != "table" {
            return Err(anyhow::anyhow!("Expected block identifier 'table'"));
        }

        let labels = block.labels();

        if labels.len() != 1 {
            return Err(anyhow::anyhow!("Expected exactly one label"));
        }

        let name = InternalString::new(labels[0].as_str())?;

        let columns = block
            .body
            .attributes()
            .map(|attr| {
                let name = InternalString::new(attr.key())?;

                Ok(ColumnDef {
                    name: InternalString::from(name),
                    data_type: parse_data_type(attr.expr(), ctx)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { name, columns })
    }
}

impl TableDef {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }
}

pub fn parse_hcl(input: &str) -> Result<Vec<TableDef>> {
    let body: Body = hcl::from_str(input)?;
    let ctx = Context::default();

    Ok(body
        .blocks()
        .filter_map(|block| match TableDef::try_from((block, &ctx)) {
            Ok(table) => Some(table),
            _ => None,
        })
        .collect::<Vec<_>>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hcl() {
        let input = r#"
            table "users" {
                email = Email
                first = Text(100)
                last  = Text(100)
                phone = Phone
            }
        "#;

        assert!(parse_hcl(input).is_ok());
    }
}
