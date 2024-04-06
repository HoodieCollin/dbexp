#![feature(lazy_cell)]
#![feature(iter_array_chunks)]
#![allow(dead_code)]

use std::{collections::HashMap, env, fs, path, sync::LazyLock};

use anyhow::Result;
use schema::data_value::DataValue;

mod decimal;
mod ratio;
mod schema;
mod timestamp;
mod uid;

pub static SCHEMA_DIR: LazyLock<path::PathBuf> = LazyLock::new(|| {
    let env = env::var("SCHEMA_DIR").unwrap_or_else(|_| ".sample/schema".to_string());
    let dir_path = path::PathBuf::from(env);

    if !dir_path.exists() {
        fs::create_dir_all(&dir_path).unwrap();
    }

    dir_path
});

pub static DATA_DIR: LazyLock<path::PathBuf> = LazyLock::new(|| {
    let env = env::var("DATA_DIR").unwrap_or_else(|_| ".sample/data".to_string());
    let dir_path = path::PathBuf::from(env);

    if !dir_path.exists() {
        fs::create_dir_all(&dir_path).unwrap();
    }

    dir_path
});

fn main() -> Result<()> {
    let schema_json_path = DATA_DIR.join("schema.json");
    let mut schema = schema::Schema::default();

    if schema_json_path.exists() {
        let schema_json = fs::read_to_string(schema_json_path)?;
        schema = serde_json::from_str(&schema_json)?;
    } else {
        // read the contents of `.sample/schema/*.toml`
        fs::read_dir(SCHEMA_DIR.as_path())?
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().unwrap() == "toml")
            .map(|path| fs::read_to_string(path).unwrap())
            .for_each(|content| {
                let table: schema::Table = toml::from_str(&content).unwrap();
                schema.tables.insert(table.name().to_string(), table);
            });
    }

    for table in schema.tables.values_mut() {
        table.init()?;
    }

    // write the schema to `.sample/data/schema.json`
    fs::write(
        DATA_DIR.join("schema.json"),
        serde_json::to_string_pretty(&schema).unwrap(),
    )?;

    // write some data
    let table = schema.tables.get_mut("test").unwrap();
    table.push(HashMap::from([("num", DataValue::I32(42))]))?;
    table.save()?;

    Ok(())
}
