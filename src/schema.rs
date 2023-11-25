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
* Last updated: 2023-11-25
*/

use arrow2::datatypes::{DataType, Field, Metadata, Schema};
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

#[cfg(test)]
mod tests_schema {
    use super::*;

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
}
