// use anyhow::Result;
// use data_types::DataValue;
// use hcl_schemas::parse_hcl;
// use indexmap::IndexMap;
// use mem_table::{object_ids::TableId, DataConfig, Table, TableConfig};

// fn main() -> anyhow::Result<()> {
//     let hcl = r#"
//         table "users" {
//             email = Email
//             first = Text(100)
//             last  = Text(100)
//             phone = Phone
//         }
//     "#;

//     let tables = parse_hcl(hcl)?
//         .into_iter()
//         .map(|table_def| {
//             let id = TableId::new();
//             let mut name_mapping = IndexMap::new();

//             let columns = table_def
//                 .columns()
//                 .iter()
//                 .enumerate()
//                 .map(|(idx, column_def)| {
//                     name_mapping.insert(*column_def.name(), idx);
//                     DataConfig::new(column_def.data_type())
//                 })
//                 .collect::<Vec<_>>();

//             let config = TableConfig::new(&columns)?;

//             Table::new(id, config, Some(name_mapping))
//         })
//         .collect::<Result<Vec<_>>>()?;

//     println!("{:#?}", tables);

//     let users = tables.first().unwrap();
//     let (email_col, first_col, last_col, phone_col) = {
//         let config = users.config();

//         (
//             config.columns.get(0).unwrap(),
//             config.columns.get(1).unwrap(),
//             config.columns.get(2).unwrap(),
//             config.columns.get(3).unwrap(),
//         )
//     };

//     let res = users.insert(vec![
//         vec![
//             Some(email_col.try_new_value("foobar@example.com")?),
//             Some(first_col.try_new_value("Foo")?),
//             Some(last_col.try_new_value("Bar")?),
//             None,
//         ],
//         vec![
//             Some(email_col.try_new_value("other@example.com")?),
//             Some(first_col.try_new_value("Other")?),
//             None,
//             Some(phone_col.try_new_value("123-456-7890")?),
//         ],
//     ])?;

//     println!("{:#?}", res);
//     println!("##############################################################");
//     println!("##############################################################");
//     println!("##############################################################");
//     println!("{:#?}", users);

//     Ok(())
// }

fn main() {}
