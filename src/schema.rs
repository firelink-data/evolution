/*
* MIT License
*
* Copyright (c) 2024 Firelink Data
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
* Last updated: 2024-05-03
*/

use arrow::array::{BooleanBuilder, Float16Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::error::Error;
use serde::{Deserialize, Serialize};

use std::path::PathBuf;
use std::{fs, io};

use crate::builder::{ColumnBuilder, BooleanBuilderHandler, Float16BuilderHandler, Float32BuilderHandler, Float64BuilderHandler, Int16BuilderHandler, Int32BuilderHandler, Int64BuilderHandler, StringBuilderHandler};
use crate::mocker;

///
#[derive(Default, Debug, Deserialize, Serialize, Clone)]
pub struct FixedColumn {
    /// The symbolic name of the column.
    name: String,
    /// The starting offset index for the column.
    offset: usize,
    /// The length of the column value.
    length: usize,
    /// The datatype of the column.
    dtype: String,
    // Whether or not the column can contain [`None`] values.
    is_nullable: bool,
}

///
#[allow(dead_code)]
impl FixedColumn {
    /// Create a new [`FixedColumn`] providing all its required attributes.
    /// No input sanitation is done in this stage. The user can provide
    /// an arbitrary datatype but the problem will then later crash when
    /// trying to call [`FixedColumn::arrow_dtype()`] if it is not known.
    pub fn new(
        name: String,
        offset: usize,
        length: usize,
        dtype: String,
        is_nullable: bool,
    ) -> Self {
        Self {
            name,
            offset,
            length,
            dtype,
            is_nullable,
        }
    }

    /// Get the name of the column.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Get the index offset for where the column starts in each row.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get the length of the column.
    pub fn length(&self) -> usize {
        self.length
    }

    /// Get the datatype of the column.
    /// NOTE: not as a [`arrow2::datatypes::DataType`].
    pub fn dtype(&self) -> &String {
        &self.dtype
    }

    /// Check whether or not the column can contain null values.
    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    /// Find the matching [`arrow2::datatypes::DataType`]s corresponding
    /// to the [`FixedColumn`]s defined datatypes.
    /// # Error
    /// Iff the [`FixedColumn`] datatype is not known.
    ///
    /// For a full list of defined datatype mappings, see the documentation
    /// or the file "resources/schema/valid_schema_dtypes.json".
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
    
    ///
    pub fn as_column_builder(&self) -> Box<dyn ColumnBuilder + '_> {
        match self.dtype.as_str() {
            "bool" => Box::new(BooleanBuilderHandler { builder: BooleanBuilder::new(), runes: self.length(), name: self.name().as_str() }),
            "boolean" => Box::new(BooleanBuilderHandler { builder: BooleanBuilder::new(), runes: self.length(), name: self.name().as_str() }),
            "i16" => Box::new(Int16BuilderHandler { builder: Int16Builder::new(), runes: self.length(), name: self.name().as_str() }),
            "i32" => Box::new(Int32BuilderHandler { builder: Int32Builder::new(), runes: self.length(), name: self.name().as_str() }),
            "i64" => Box::new(Int64BuilderHandler { builder: Int64Builder::new(), runes: self.length(), name: self.name().as_str() }),
            "f16" => Box::new(Float16BuilderHandler { builder: Float16Builder::new(), runes: self.length(), name: self.name().as_str() }),
            "f32" => Box::new(Float32BuilderHandler { builder: Float32Builder::new(), runes: self.length(), name: self.name().as_str() }),
            "f64" => Box::new(Float64BuilderHandler { builder: Float64Builder::new(), runes: self.length(), name: self.name().as_str() }),
            "utf8" => Box::new(StringBuilderHandler { builder: StringBuilder::new(), runes: self.length(), name: self.name().as_str() }),
            "string" => Box::new(StringBuilderHandler { builder: StringBuilder::new(), runes: self.length(), name: self.name().as_str() }),
            "lutf8" => Box::new(StringBuilderHandler { builder: StringBuilder::new(), runes: self.length(), name: self.name().as_str() }),
            "lstring" => Box::new(StringBuilderHandler { builder: StringBuilder::new(), runes: self.length(), name: self.name().as_str() }),
            _ => panic!("Could not find matching FixedColumn dtype when creating ColumnBuilder!"),
        }
    }
}

///
impl FixedColumn {
    pub fn mock<'a>(&self) -> Result<&'a str, Error> {
        // TODO: properly mock the different datatypes!
        let string = match self.dtype.as_str() {
            "bool" => mocker::mock_bool(std::cmp::max(self.length, 0)),
            "boolean" => mocker::mock_bool(std::cmp::max(self.length, 0)),
            "i16" => mocker::mock_number(std::cmp::max(self.length, 0)),
            "i32" => mocker::mock_number(std::cmp::max(self.length, 0)),
            "i64" => mocker::mock_number(std::cmp::max(self.length, 0)),
            "f16" => mocker::mock_number(std::cmp::max(self.length, 0)),
            "f32" => mocker::mock_number(std::cmp::max(self.length, 0)),
            "f64" => mocker::mock_number(std::cmp::max(self.length, 0)),
            "utf8" => mocker::mock_string(std::cmp::max(self.length, 0)),
            "string" => mocker::mock_string(std::cmp::max(self.length, 0)),
            "lutf8" => mocker::mock_string(std::cmp::max(self.length, 0)),
            "lstring" => mocker::mock_string(std::cmp::max(self.length, 0)),
            _ => return Err(Error::ExternalFormat("xd".to_string())),
        };

        Ok(string)
    }
}

///
#[derive(Deserialize, Serialize, Clone)]
pub struct FixedSchema {
    name: String,
    version: i32,
    columns: Vec<FixedColumn>,
}

///
#[allow(dead_code)]
impl FixedSchema {
    /// Explicitly create a new [`FixedSchema`] by providing all its requried attributes.
    pub fn new(name: String, version: i32, columns: Vec<FixedColumn>) -> Self {
        Self {
            name,
            version,
            columns,
        }
    }

    /// Implicitly create a new [`FixedSchema`] by reading a json path
    /// and deserializing the schema into the epxected struct fields.
    /// # Error
    /// If the file does not exist or if the schema in the file
    /// does not adhere to the above struct definition.
    pub fn from_path(path: PathBuf) -> Self {
        let json = fs::File::open(path).unwrap();
        let reader = io::BufReader::new(json);

        serde_json::from_reader(reader).unwrap()
    }

    /// Get the number of columns in the schema.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Get the total length of the fixed-length row.
    pub fn row_len(&self) -> usize {
        self.columns.iter().map(|c| c.length).sum()
    }

    /// Get the names of the columns.
    pub fn column_names(&self) -> Vec<&String> {
        self.columns
            .iter()
            .map(|c| &c.name)
            .collect::<Vec<&String>>()
    }

    /// Get the index offsets for each column.
    pub fn column_offsets(&self) -> Vec<usize> {
        self.columns
            .iter()
            .map(|c| c.offset)
            .collect::<Vec<usize>>()
    }

    /// Get the column lengths.
    pub fn column_lengths(&self) -> Vec<usize> {
        self.columns
            .iter()
            .map(|c| c.length)
            .collect::<Vec<usize>>()
    }

    /// Check whether any column can contain null values.
    pub fn has_nullable_cols(&self) -> bool {
        self.columns.iter().any(|c| c.is_nullable)
    }

    /// Consume the [`FixedSchema`] and produce a new [`arrow2::datatypes::Schema`].
    /// All of the [`FixedColumn`]s will tried to be parsed as their
    /// corresponding [`arrow2::datatypes::DataType`]s, but may fail.
    /// # Error
    /// Iff any of the column datatypes can not be parsed to its
    /// corresponding [`arrow2::datatypes::DataType`].
    pub fn into_arrow_schema(self) -> Schema {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(c.name.to_owned(), c.arrow_dtype().unwrap(), c.is_nullable))
            .collect();

        Schema::from(fields)
    }

    /// Borrow the stored [`FixedColumn`]s and iterate over them.
    pub fn iter(&self) -> FixedSchemaIterator {
        FixedSchemaIterator {
            columns: &self.columns,
            index: 0,
        }
    }
}

/// Intermediary struct which holds state necessary for
/// iterating a [`FixedSchema`], borrows the [`FixedColumn`]s.
pub struct FixedSchemaIterator<'a> {
    columns: &'a Vec<FixedColumn>,
    index: usize,
}

/// Iterate the [`FixedColumn`]s of a [`FixedSchema`].
/// Only borrows the values, nothing is moved.
impl<'a> Iterator for FixedSchemaIterator<'a> {
    type Item = &'a FixedColumn;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.columns.len() {
            Some(
                &self.columns[{
                    self.index += 1;
                    self.index - 1
                }],
            )
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests_schema {
    use super::*;

    #[test]
    fn test_new_fixed_column_ok() {
        let schema = FixedColumn::new(
            "coolSchema2000Elin".to_string(),
            5,
            20,
            "utf8".to_string(),
            false,
        );
        assert_eq!(DataType::Utf8, schema.arrow_dtype().unwrap());
    }

    #[test]
    fn test_fixed_to_arrow_schema_ok() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schema/test_schema.json");

        let fixed_schema: FixedSchema = FixedSchema::from_path(path);
        let arrow_schema: Schema = fixed_schema.into_arrow_schema();

        assert_eq!(4, arrow_schema.fields.len());
    }

    #[test]
    fn test_derive_from_file_ok() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schema/test_schema.json");

        let schema: FixedSchema = FixedSchema::from_path(path);
        let offsets: Vec<usize> = vec![0, 9, 41, 73];
        let lengths: Vec<usize> = vec![9, 32, 32, 5];

        assert_eq!(4, schema.num_columns());
        assert_eq!(78, schema.row_len());
        assert_eq!(offsets, schema.column_offsets());
        assert_eq!(lengths, schema.column_lengths());
    }

    #[test]
    #[should_panic]
    fn test_derive_from_file_trailing_commas() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schema/test_schema_trailing_commas.json");

        let _schema: FixedSchema = FixedSchema::from_path(path);
    }
}
