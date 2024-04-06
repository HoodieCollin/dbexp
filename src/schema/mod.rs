use std::collections::HashMap;

pub use self::table::Table;

pub mod data_block;
pub mod data_type;
pub mod data_value;
pub mod field;
pub mod table;

#[derive(Debug, Clone, Copy)]
pub enum FileAction {
    Created,
    Opened,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct Schema {
    pub tables: HashMap<String, Table>,
}

// #[cfg(test)]
// mod tests {
//     use uuid::uuid;

//     use super::*;

//     #[test]
//     fn test_data() -> Result<()> {
//         let mut data = Data::new::<uuid::Uuid>(10)?;
//         let mut typed = data.as_typed_mut::<uuid::Uuid>()?;
//         typed.push(uuid!("11111111-1111-1111-1111-111111111111"))?;
//         typed.push(uuid!("22222222-2222-2222-2222-222222222222"))?;
//         typed.push(uuid!("33333333-3333-3333-3333-333333333333"))?;
//         typed.push(uuid!("44444444-4444-4444-4444-444444444444"))?;

//         println!("{:#?}", typed);

//         typed.delete(2)?;

//         println!("{:#?}", typed.list()?);

//         Ok(())
//     }

//     // #[test]
//     // fn test_field() -> Result<()> {
//     //     let mut field = Field {
//     //         id: Uuid::new_v4(),
//     //         unique: true,
//     //         required: true,
//     //         automatic: true,
//     //         data_type: DataType::Uuid,
//     //         data: Data::new(16, 10)?,
//     //     };

//     //     field.push(Uuid::new_v4())?;
//     //     field.push(Uuid::new_v4())?;
//     //     field.push(Uuid::new_v4())?;
//     //     field.push(Uuid::new_v4())?;
//     //     field.push(Uuid::new_v4())?;
//     //     field.push(Uuid::new_v4())?;
//     // }

//     // #[test]
//     // fn test_table() -> Result<()> {
//     //     let mut table = Table {
//     //         id: Uuid::new_v4(),
//     //         name: "users".to_string(),
//     //         fields: HashMap::new(),
//     //         hash_lookup: HashMap::new(),
//     //     };

//     //     table.fields.insert(
//     //         "id".to_string(),
//     //         Field {
//     //             id: Uuid::new_v4(),
//     //             unique: true,
//     //             required: true,
//     //             automatic: true,
//     //             data_type: DataType::Uuid,
//     //             data: Data::new(16, 10)?,
//     //         },
//     //     );

//     //     table.fields.insert(
//     //         "name".to_string(),
//     //         Field {
//     //             id: Uuid::new_v4(),
//     //             unique: true,
//     //             required: true,
//     //             automatic: true,
//     //             data_type: DataType::String,
//     //             data: Data::new(16, 10)?,
//     //         },
//     //     );

//     //     table.init(10)?;
//     //     table.fields.get_mut("id").unwrap().push(Uuid::new_v4())?;
//     //     table.fields.get_mut("id").unwrap().push(Uuid::new_v4())?;
//     //     table.fields.get_mut("id").unwrap().push(Uuid::new_v4())?;
//     //     table.fields.get_mut("id").unwrap().push(Uuid::new_v4())?;
//     //     table.fields.get_mut("id").unwrap().push(Uuid::new_v4())?;
//     //     table.fields.get_mut("id").unwrap().push(Uuid::new_v4())?;

//     //     table.fields.get_mut("name").unwrap().push("Alice".to_string())?;
//     //     table.fields.get_mut("name").unwrap().push("Bob".to_string())?;
//     //     table.fields.get_mut("name").unwrap().push("Charlie".to_string())?;
//     //     table.fields.get_mut("name").unwrap().push("David".to_string())?;
//     //     table.fields.get_mut("name").unwrap().push("Eve".to_string())?;
//     //     table.fields.get_mut("name").unwrap().push("Frank".to_string())?;

//     //     let ids: Vec<Uuid> = table.fields.get("id").unwrap().list()?;
//     //     let names: Vec<String> = table.fields.get("name").unwrap().list()?;

//     //     assert_eq!(ids.len(), 6);
//     //     assert_eq!(names.len(), 6);

//     //     Ok(())
//     // }
// }
