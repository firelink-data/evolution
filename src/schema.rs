/*
* MIT License
*
* Copyright (c) 2023 Firelink Data
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*
* File created: 2023-11-25
* Last updated: 2023-11-26
*/

use arrow2::datatypes::{DataType, Field, Metadata, Schema};
use arrow2::error::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;

///
#[allow(dead_code)]
pub fn value_as_datatype(value: &Value) -> DataType {
    if value.is_string() {
        DataType::Utf8
    } else if value.is_u64() {
        DataType::UInt64
    } else if value.is_i64() {
        DataType::Int64
    } else if value.is_f64() {
        DataType::Float64
    } else if value.is_boolean() {
        DataType::Boolean
    } else {
        panic!(
            "Could not find matching arrow2::DataType from value: {:?}",
            value
        );
    }
}

/// Look into
/// https://docs.rs/arrow2/latest/src/arrow2/io/json_integration/read/schema.rs.html#442-480
/// this might solve what we are trying to do.
#[allow(dead_code)]
pub fn schema_from_str(json: &str, metadata: Metadata) -> Schema {
    let values: Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(e) => panic!(
            "Could not deserialize string to JSON values, error: {:?}",
            e
        ),
    };

    let mut fields: Vec<Field> = Vec::new();

    if let Value::Object(hash_map) = values {
        fields.extend(
            hash_map
                .iter()
                .map(|(key, value)| Field::new(key, value_as_datatype(value), false)),
        );
    }

    Schema { fields, metadata }
}

///
#[derive(Deserialize, Serialize)]
pub struct Column {
    name: String,
    length: usize,
    dtype: String,
    is_nullable: bool,
}

///
impl Column {
    ///
    pub fn arrow_dtype(&self) -> Result<DataType, Error> {
        match self.dtype.as_str() {
            "bool" => Ok(DataType::Boolean),
            "boolean" => Ok(DataType::Boolean),
            "i16" => Ok(DataType::Int16),
            "i32" => Ok(DataType::Int32),
            "i64" => Ok(DataType::Int64),
            "f16" => Ok(DataType::Float16),
            "f32" => Ok(DataType::Float32),
            "f64" => Ok(DataType::Float64),
            "utf8" => Ok(DataType::Utf8),
            "string" => Ok(DataType::Utf8),
            "lutf8" => Ok(DataType::LargeUtf8),
            "lstring" => Ok(DataType::LargeUtf8),
            _ => Err(Error::ExternalFormat(format!(
                "Could not parse json schema dtype to arrow datatype, dtype: {:?}",
                self.dtype,
            ))),
        }
    }
}

///
#[derive(Deserialize, Serialize)]
pub struct FixedSchema {
    name: String,
    version: i32,
    columns: Vec<Column>,
}

///
#[allow(dead_code)]
impl FixedSchema {
    ///
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    ///
    pub fn row_len(&self) -> usize {
        self.columns.iter().map(|c| c.length).sum()
    }

    ///
    pub fn has_nullable_cols(&self) -> bool {
        self.columns.iter().any(|c| c.is_nullable)
    }

    ///
    pub fn into_arrow_schema(self) -> Schema {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(c.name.to_owned(), c.arrow_dtype().unwrap(), c.is_nullable))
            .collect();

        Schema::from(fields)
    }
}

#[cfg(test)]
mod tests_schema {
    use super::*;
    use std::path::PathBuf;
    use std::{fs, io};

    #[test]
    fn test_from_custom_string() {
        let data = r#"
        {
            "name": "John Cena",
            "age": "1498"
        }"#;
        let schema = schema_from_str(data, Metadata::default());
        println!("schema: {:?}", schema);
    }

    #[test]
    fn test_fixed_to_arrow_schema_ok() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schema/test_schema.json");

        let json = fs::File::open(path).unwrap();
        let reader = io::BufReader::new(json);

        let fixed_schema: FixedSchema = serde_json::from_reader(reader).unwrap();

        let arrow_schema: Schema = fixed_schema.into_arrow_schema();

        assert_eq!(4, arrow_schema.fields.len());
    }

    #[test]
    fn test_derive_from_file_ok() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schema/test_schema.json");

        let json = fs::File::open(path).unwrap();
        let reader = io::BufReader::new(json);

        let schema: FixedSchema = serde_json::from_reader(reader).unwrap();

        assert_eq!(4, schema.num_columns());
        assert_eq!(74, schema.row_len());
    }

    #[test]
    #[should_panic]
    fn test_derive_from_file_trailing_commas() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schema/test_schema_trailing_commas.json");

        let json = fs::File::open(path).unwrap();
        let reader = io::BufReader::new(json);

        let _schema: FixedSchema = serde_json::from_reader(reader).unwrap();
    }
}
