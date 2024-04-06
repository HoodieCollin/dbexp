use std::collections::{hash_map, HashMap};
use std::fs;
use std::path::PathBuf;

use anyhow::Result;

use super::data_type::DataType;
use super::data_value::DataValue;
use super::field::Field;
use crate::timestamp::Timestamp;
use crate::uid::Uid;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct Table {
    #[serde(default = "Uid::new")]
    id: Uid,
    name: String,
    #[serde(alias = "fields")]
    named_fields: HashMap<String, Field>,
    #[serde(skip_deserializing, default = "Table::new_row_id_field")]
    row_id_field: Field,
    #[serde(skip_deserializing, default = "Table::new_timestamp_field")]
    created_at_field: Field,
    #[serde(skip_deserializing, default = "Table::new_timestamp_field")]
    updated_at_field: Field,
    #[serde(skip)]
    dir_path: PathBuf,
}

impl Table {
    pub fn init(&mut self) -> Result<()> {
        let table_dir = crate::DATA_DIR.join(self.id.to_string());

        if !table_dir.exists() {
            fs::create_dir_all(&table_dir)?;
        }

        self.row_id_field.init(&table_dir)?;
        self.created_at_field.init(&table_dir)?;
        self.updated_at_field.init(&table_dir)?;

        for field in self.named_fields.values_mut() {
            field.init(&table_dir)?;
        }

        Ok(())
    }

    fn new_row_id_field() -> Field {
        let mut field = Field::default();
        field.id = Uid::new();
        field.data_type = DataType::Uid;
        field.unique = true;
        field.required = true;
        field.automatic = true;
        field
    }

    fn new_timestamp_field() -> Field {
        let mut field = Field::default();
        field.id = Uid::new();
        field.data_type = DataType::Timestamp;
        field.unique = false;
        field.required = true;
        field.automatic = true;
        field
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        match name {
            "row_id" => Some(&self.row_id_field),
            "created_at" => Some(&self.created_at_field),
            "updated_at" => Some(&self.updated_at_field),
            _ => self.named_fields.get(name),
        }
    }

    pub fn field_mut(&mut self, name: &str) -> Option<&mut Field> {
        match name {
            "row_id" => Some(&mut self.row_id_field),
            "created_at" => Some(&mut self.created_at_field),
            "updated_at" => Some(&mut self.updated_at_field),
            _ => self.named_fields.get_mut(name),
        }
    }

    pub fn fields(&self) -> TableFieldIter<'_> {
        TableFieldIter {
            row_id: Some(&self.row_id_field),
            created_at: Some(&self.created_at_field),
            updated_at: Some(&self.updated_at_field),
            fields: self.named_fields.iter(),
        }
    }

    pub fn fields_mut(&mut self) -> TableFieldIterMut<'_> {
        TableFieldIterMut {
            row_id: Some(&mut self.row_id_field),
            created_at: Some(&mut self.created_at_field),
            updated_at: Some(&mut self.updated_at_field),
            fields: self.named_fields.iter_mut(),
        }
    }

    pub fn push<R, K, V>(&mut self, row: R) -> Result<Uid>
    where
        K: AsRef<str>,
        V: ToOwned<Owned = DataValue>,
        R: IntoIterator<Item = (K, V)>,
    {
        let row_id = Uid::new();
        let index = if let Some(index) = self.row_id_field.next_available_index() {
            self.row_id_field.insert(index, DataValue::Uid(row_id))?;
            index
        } else {
            self.row_id_field.push(DataValue::Uid(row_id))?
        };

        self.created_at_field
            .insert(index, DataValue::Timestamp(Timestamp::new()))?;

        for (key, val) in row {
            if let Some(field) = self.named_fields.get_mut(key.as_ref()) {
                field.insert(index, val.to_owned())?;
            }
        }

        Ok(row_id)
    }

    pub fn save(&mut self) -> Result<()> {
        self.row_id_field.save()?;
        self.created_at_field.save()?;
        self.updated_at_field.save()?;

        for field in self.named_fields.values_mut() {
            field.save()?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct TableFieldIter<'a> {
    row_id: Option<&'a Field>,
    created_at: Option<&'a Field>,
    updated_at: Option<&'a Field>,
    fields: hash_map::Iter<'a, String, Field>,
}

impl<'a> Iterator for TableFieldIter<'a> {
    type Item = (&'a str, &'a Field);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row_id) = self.row_id.take() {
            return Some(("row_id", row_id));
        }

        if let Some(created_at) = self.created_at.take() {
            return Some(("created_at", created_at));
        }

        if let Some(updated_at) = self.updated_at.take() {
            return Some(("updated_at", updated_at));
        }

        self.fields
            .next()
            .map(|(name, field)| (name.as_str(), field))
    }
}

#[derive(Debug)]
pub struct TableFieldIterMut<'a> {
    row_id: Option<&'a mut Field>,
    created_at: Option<&'a mut Field>,
    updated_at: Option<&'a mut Field>,
    fields: hash_map::IterMut<'a, String, Field>,
}

impl<'a> Iterator for TableFieldIterMut<'a> {
    type Item = (&'a str, &'a mut Field);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row_id) = self.row_id.take() {
            return Some(("row_id", row_id));
        }

        if let Some(created_at) = self.created_at.take() {
            return Some(("created_at", created_at));
        }

        if let Some(updated_at) = self.updated_at.take() {
            return Some(("updated_at", updated_at));
        }

        self.fields
            .next()
            .map(|(name, field)| (name.as_str(), field))
    }
}
