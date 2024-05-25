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
// Last updated: 2024-05-25
//

use arrow::datatypes::DataType as ArrowDataType;
use deltalake::kernel::DataType as DeltaDataType;
use evolution_common::datatype::DataType;
use padder::{Alignment, Symbol};
use serde::{Deserialize, Serialize};

/// Representation of a column in a fixed-length file (.flf), containing all allowed fields.
/// 
/// # Note
/// This struct is meant to be deserialzied from a .json schema file, and as such, capitalization
/// of the field values is important. For example, the dtype field has to be exactly one of the 
/// [`DataType`] enum variants, spelled exactly the same. Otherwise, the serde crate can't
/// deserialize the values when initializing a new struct.
#[derive(Deserialize, Serialize)]
pub struct Column {
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


impl Column {
    /// Get the name of the column.
    pub fn name(&self) -> &str {
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

    /// Get the datatype of the column as a [`ArrowDataType`] variant.
    /// 
    /// # Note
    /// This method is primarily used when creating a new [`arrow::datatypes::Schema`] from
    /// an existing [`evolution_schema::schema::Schema`].
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
    /// Currently this method will map the `half` (Float16) datatype to the float32 variant.
    /// This is because [`deltalake`] does define a 
    pub fn as_delta_dtype(&self) -> DeltaDataType {
        match self.dtype {
            DataType::Boolean => DeltaDataType::BOOLEAN,
            DataType::Float16 => DeltaDataType::FLOAT,
            DataType::Float32 => DeltaDataType::FLOAT,
            DataType::Float64 => DeltaDataType::DOUBLE,
            DataType::Int16 => DeltaDataType::SHORT,
            DataType::Int32 => DeltaDataType::INTEGER,
            DataType::Int64 => DeltaDataType::LONG,
            DataType::Utf8 => DeltaDataType::STRING,
            DataType::LargeUtf8 => DeltaDataType::STRING,
        }
    }
}


#[cfg(test)]
mod tests_column {
    use super::*;

    use std::fs;
    use std::io;
    use std::path::PathBuf;

    #[test]
    fn test_deserialize_column_from_file() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("res/test_valid_column.json");

        let json = fs::File::open(path).unwrap();
        let reader = io::BufReader::new(json);

        let _: Column = serde_json::from_reader(reader).unwrap();
    }
}