//
// MIT License
//
// Copyright (c) 2023-2024 Firelink Data
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
// Last updated: 2024-10-19
//

use arrow::datatypes::DataType as ArrowDataType;
use deltalake::kernel::DataType as DeltaDataType;
use evolution_builder::parquet::ParquetColumnBuilderRef;
use evolution_builder::datatype::{
    BooleanColumnBuilder, Float16ColumnBuilder, Float32ColumnBuilder, Float64ColumnBuilder,
    Int16ColumnBuilder, Int32ColumnBuilder, Int64ColumnBuilder, Utf8ColumnBuilder,
};
use evolution_common::datatype::DataType;
use evolution_parser::datatype::{BooleanParser, FloatParser, IntParser, Utf8Parser};
use log::warn;
use padder::{Alignment, Symbol};
use serde::{Deserialize, Serialize};

/// Unified trait for all types of schema columns.
pub trait Column {}
pub type ColumnRef = Box<dyn Column>;

/// Representation of a column in a fixed-length file (.flf), containing the only allowed fields.
///
/// # Note
/// This struct is meant to be created when deserializing a .json schema file representing a [`crate::schema::FixedSchema`],
/// and as such, capitalization of the field values is extremely important. For example, the dtype field has to be exactly
/// one of the [`DataType`] enum variants, spelled exactly the same. Otherwise, serde can't deserialize the values.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct FixedColumn {
    /// The symbolic name of the column.
    name: String,
    /// The starting offset index for the column (in runes).
    offset: usize,
    /// The total length of the column (in runes).
    length: usize,
    /// The datatype of the column.
    dtype: DataType,
    /// The type of alignment that the column has (default is [`Alignment::Right`]).
    #[serde(default)]
    alignment: Alignment,
    /// The symbol used to pad the column to its expected length (default is [`Symbol::Whitespace`]).
    #[serde(default)]
    pad_symbol: Symbol,
    /// Whether or not the column can contain null values.
    is_nullable: bool,
}

impl FixedColumn {
    /// Create a new [`FixedColumn`] from the provided field values.
    pub fn new(
        name: String,
        offset: usize,
        length: usize,
        dtype: DataType,
        alignment: Alignment,
        pad_symbol: Symbol,
        is_nullable: bool,
    ) -> Self {
        Self {
            name,
            offset,
            length,
            dtype,
            alignment,
            pad_symbol,
            is_nullable,
        }
    }

    /// Get the name of the column.
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Get the starting offset of the column (in runes).
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Get the total length of the column (in runes).
    pub fn length(&self) -> usize {
        self.length
    }

    /// Get the datatype of the column (as a [`DataType`] variant).
    pub fn dtype(&self) -> DataType {
        self.dtype
    }

    /// Get the alignment mode of the column (as an [`Alignment`] variant).
    pub fn alignment(&self) -> Alignment {
        self.alignment
    }

    /// Get the symbol used to pad the column to its expected length (as a [`Symbol`] variant).
    pub fn pad_symbol(&self) -> Symbol {
        self.pad_symbol
    }

    /// Get whether or not the column is allowed to be nullable.
    ///
    /// # Note
    /// If a column is defined as not nullable and parsing of a fixed-length file column fails
    /// to find a valid value, then a NULL value will not be appended to the column builder.
    /// Instead, the program will terminate due to this occurring.
    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    /// Get the datatype of the column as a [`ArrowDataType`] variant.
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

    /// Get the datatype of the column as a [`DeltaDataType`] variant.
    ///
    /// # Note
    /// Currently this method will map [`DataType::Float16`] to the [`DataType::Float32`] variant. This
    /// is because the [`deltalake`] crate does not yet define a Float16 variant in its [`DeltaDataType`].
    pub fn as_delta_dtype(&self) -> DeltaDataType {
        match self.dtype {
            DataType::Boolean => DeltaDataType::BOOLEAN,
            DataType::Float16 => {
                warn!("Casting Float16 to Float32 for deltalake compatibility.");
                DeltaDataType::FLOAT
            }
            DataType::Float32 => DeltaDataType::FLOAT,
            DataType::Float64 => DeltaDataType::DOUBLE,
            DataType::Int16 => DeltaDataType::SHORT,
            DataType::Int32 => DeltaDataType::INTEGER,
            DataType::Int64 => DeltaDataType::LONG,
            DataType::Utf8 => DeltaDataType::STRING,
            DataType::LargeUtf8 => DeltaDataType::STRING,
        }
    }

    /// Create a new [`ColumnBuilderRef`] based on the datatype of the column.
    ///
    /// # Performance
    /// This method will clone the String which contains the name of the column.
    /// You should only use this during setup of the program, and not during any
    /// performance critical parts of the program.
    pub fn as_column_builder(&self) -> ParquetColumnBuilderRef {
        match self.dtype {
            DataType::Boolean => Box::new(BooleanColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                BooleanParser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Float16 => Box::new(Float16ColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                FloatParser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Float32 => Box::new(Float32ColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                FloatParser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Float64 => Box::new(Float64ColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                FloatParser::new(self.alignment, self.pad_symbol),
            )),
            DataType::Int16 => Box::new(Int16ColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                IntParser::new(),
            )),
            DataType::Int32 => Box::new(Int32ColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                IntParser::new(),
            )),
            DataType::Int64 => Box::new(Int64ColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                IntParser::new(),
            )),
            DataType::Utf8 => Box::new(Utf8ColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                Utf8Parser::new(self.alignment, self.pad_symbol),
            )),
            DataType::LargeUtf8 => Box::new(Utf8ColumnBuilder::new(
                self.name.clone(),
                self.length,
                self.is_nullable,
                Utf8Parser::new(self.alignment, self.pad_symbol),
            )),
        }
    }
}

impl Column for FixedColumn {}

#[cfg(test)]
mod tests_column {
    use super::*;

    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_deserialize_column_from_file() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("res/test_valid_column.json");

        let a: FixedColumn = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        let b: FixedColumn = FixedColumn::new(
            String::from("NotCoolColumn"),
            0 as usize,
            2 as usize,
            DataType::Float16,
            Alignment::Center,
            Symbol::Asterisk,
            true,
        );

        assert_ne!(a.name(), b.name());
        assert_ne!(a.offset(), b.offset());
        assert_ne!(a.length(), b.length());
        assert_ne!(a.dtype(), b.dtype());
        assert_eq!(a.alignment(), b.alignment());
        assert_ne!(a.pad_symbol(), b.pad_symbol());
        assert_ne!(a.is_nullable(), b.is_nullable());

        // This is hopefully subject to change...
        assert_ne!(a.as_arrow_dtype(), b.as_arrow_dtype());
        assert_eq!(a.as_delta_dtype(), b.as_delta_dtype());
    }

    #[test]
    #[should_panic]
    fn test_deserialize_invalid_column_from_file() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("res/test_invalid_column.json");
        let _: FixedColumn = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    }
}
