//
// MIT License
//
// Copyright (c) 2024 Firelink Data
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// File created: 2023-11-25
// Last updated: 2024-05-14
//

use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef as ArrowSchemaRef};
use padder::{Alignment, Symbol};
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use crate::builder::{
    BooleanColumnBuilder, ColumnBuilder, Float16ColumnBuilder, Float32ColumnBuilder,
    Float64ColumnBuilder, Int16ColumnBuilder, Int32ColumnBuilder, Int64ColumnBuilder,
    LargeUtf8ColumnBuilder, Utf8ColumnBuilder,
};
use crate::datatype::DataType;
use crate::error::Result;
use crate::mocking::{mock_bool, mock_float, mock_integer, mock_string};
use crate::parser::{
    BooleanParser, Float16Parser, Float32Parser, Float64Parser, Int16Parser, Int32Parser,
    Int64Parser, LargeUtf8Parser, Utf8Parser,
};

/// A struct representing a column in a fixed-length file.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FixedColumn {
    /// The symbolic name of the column.
    name: String,
    /// The starting offset index for the column.
    offset: usize,
    /// The length of the column value.
    length: usize,
    /// The datatype of the column.
    dtype: DataType,
    /// The type of alignment the column has.
    #[serde(default)]
    alignment: Alignment,
    /// The symbol used to pad the column to its expected length.
    #[serde(default)]
    pad_symbol: Symbol,
    /// Whether or not the column can contain null values.
    is_nullable: bool,
}

impl FixedColumn {
    #[cfg(feature = "rayon")]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[cfg(feature = "rayon")]
    pub fn dtype(&self) -> DataType {
        self.dtype
    }

    /// Get the length of the column.
    pub fn length(&self) -> usize {
        self.length
    }

    /// Get the alignment mode of the column.
    pub fn alignment(&self) -> Alignment {
        self.alignment
    }

    /// Get the padding symbol for the column.
    pub fn pad_symbol(&self) -> Symbol {
        self.pad_symbol
    }

    /// Find the matching [`ArrowDataType`] for the [`FixedColumn`]s dtype.
    pub fn as_arrow_dtype(&self) -> ArrowDataType {
        match self.dtype {
            DataType::Boolean => ArrowDataType::Boolean,
            DataType::Float16 => ArrowDataType::Float16,
            DataType::Float32 => ArrowDataType::Float32,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::Int16 => ArrowDataType::Int16,
            DataType::Int32 => ArrowDataType::Int32,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Utf8 => ArrowDataType::Utf8,
            DataType::LargeUtf8 => ArrowDataType::LargeUtf8,
        }
    }

    /// Create a new [`ColumnBuilder`] on the heap from the [`FixedColumn`] fields.
    ///
    /// # Performance
    /// Here it is ok to clone the [`String`] name because the [`ColumnBuilder`]s should
    /// be initialized at the start of the program, and not in any loop or thread.
    pub fn as_column_builder(&self) -> Box<dyn ColumnBuilder> {
        match self.dtype {
            DataType::Boolean => Box::new(BooleanColumnBuilder::new(
                self.length,
                self.name.clone(),
                BooleanParser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Float16 => Box::new(Float16ColumnBuilder::new(
                self.length,
                self.name.clone(),
                Float16Parser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Float32 => Box::new(Float32ColumnBuilder::new(
                self.length,
                self.name.clone(),
                Float32Parser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Float64 => Box::new(Float64ColumnBuilder::new(
                self.length,
                self.name.clone(),
                Float64Parser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Int16 => Box::new(Int16ColumnBuilder::new(
                self.length,
                self.name.clone(),
                Int16Parser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Int32 => Box::new(Int32ColumnBuilder::new(
                self.length,
                self.name.clone(),
                Int32Parser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Int64 => Box::new(Int64ColumnBuilder::new(
                self.length,
                self.name.clone(),
                Int64Parser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Utf8 => Box::new(Utf8ColumnBuilder::new(
                self.length,
                self.name.clone(),
                Utf8Parser::new(self.alignment, self.pad_symbol),
            )),
            DataType::LargeUtf8 => Box::new(LargeUtf8ColumnBuilder::new(
                self.length,
                self.name.clone(),
                LargeUtf8Parser::new(self.alignment, self.pad_symbol),
            )),
        }
    }

    /// Randomly generate data for the [`FixedColumn`] based on its datatype.
    pub fn mock(&self, rng: &mut ThreadRng) -> String {
        match self.dtype {
            DataType::Boolean => mock_bool(rng),
            DataType::Float16 => mock_float(rng),
            DataType::Float32 => mock_float(rng),
            DataType::Float64 => mock_float(rng),
            DataType::Int16 => mock_integer(rng),
            DataType::Int32 => mock_integer(rng),
            DataType::Int64 => mock_integer(rng),
            DataType::Utf8 => mock_string(self.length, rng),
            DataType::LargeUtf8 => mock_string(self.length, rng),
        }
    }
}

/// A struct representing an entire schema for a fixed-length file.
/// This can be created using the [`serde_json`] crate since this struct and the 
/// [`FixedColumn`] struct derives the [`Deserialize`] and [`Serialize`] traits.
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct FixedSchema {
    name: String,
    version: i32,
    columns: Vec<FixedColumn>,
}

#[allow(dead_code)]
impl FixedSchema {
    /// Implicitly create a new [`FixedSchema`] by reading a json path
    /// and deserializing the schema into the epxected struct fields.
    ///
    /// # Panics
    /// If the file does not exist or if the schema in the file
    /// does not adhere to the above struct definition.
    pub fn from_path(path: PathBuf) -> Result<Self> {
        let json = fs::File::open(path)?;
        let reader = io::BufReader::new(json);

        match serde_json::from_reader(reader) {
            Ok(s) => Ok(s),
            Err(e) => Err(Box::new(e)),
        }
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

    /// Consume the [`FixedSchema`] and produce a new [`Schema`].
    pub fn into_arrow_schema(self) -> Schema {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|c| Field::new(c.name.to_owned(), c.as_arrow_dtype(), c.is_nullable))
            .collect();

        Schema::new(fields)
    }

    ///
    /// # Performance
    /// This function will clone each [`FixedColumn`] name attribute which might
    /// incurr heavy performance costs for very large [`FixedSchema`]s. This
    /// method should only really be called at the start of the program when initializing
    /// required resources (like [`crate::mocker::Mocker`] or [`crate::converter::Converter`].
    pub fn as_arrow_schema(&self) -> Schema {
        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|column| {
                Field::new(
                    column.name.clone(),
                    column.as_arrow_dtype(),
                    column.is_nullable,
                )
            })
            .collect();

        Schema::new(fields)
    }

    pub fn as_arrow_schema_ref(&self) -> ArrowSchemaRef {
        Arc::new(self.as_arrow_schema())
    }

    /// Create a vec of [`ColumnBuilder`]s for each [`FixedColumn`] in the schema.
    pub fn as_column_builders(&self) -> Vec<Box<dyn ColumnBuilder>> {
        self.columns
            .iter()
            .map(|c| c.as_column_builder())
            .collect::<Vec<Box<dyn ColumnBuilder>>>()
    }

    /// Borrow the schemas [`FixedColumn`]s and iterate over them.
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
    fn test_fixed_to_arrow_schema_ok() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schemas/test_valid_schema_all.json");

        let fixed_schema: FixedSchema = FixedSchema::from_path(path).unwrap();
        let arrow_schema: Schema = fixed_schema.into_arrow_schema();

        assert_eq!(5, arrow_schema.fields.len());
    }

    #[test]
    fn test_derive_from_file_ok() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schemas/test_valid_schema_booleans.json");

        let schema: FixedSchema = FixedSchema::from_path(path).unwrap();
        let offsets: Vec<usize> = vec![0, 9, 14];
        let lengths: Vec<usize> = vec![9, 5, 32];

        assert_eq!(3, schema.num_columns());
        assert_eq!(46, schema.row_len());
        assert_eq!(offsets, schema.column_offsets());
        assert_eq!(lengths, schema.column_lengths());
    }

    #[test]
    #[should_panic]
    fn test_derive_from_file_trailing_commas() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("resources/schemas/test_invalid_schema_trailing_commas.json");

        let _schema: FixedSchema = FixedSchema::from_path(path).unwrap();
    }
}
