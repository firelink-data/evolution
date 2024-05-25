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

use crate::column::FixedColumn;

use evolution_common::datatype::DataType;
use evolution_common::error::Result;
use serde::{Deserialize, Serialize};

use std::fs;
use std::path::PathBuf;

/// A blank trait to allow developers to create their own schema implementations.
pub trait Schema {}
pub type SchemaRef = Box<dyn Schema>;

/// Representation of the schema for a fixed-length file (.flf), containing the only allowed fields.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct FixedSchema {
    /// The symbolic name of the schema.
    name: String,
    /// The version of the schema.
    version: usize,
    /// The columns that make up the fixed-length file.
    columns: Vec<FixedColumn>,
}

impl FixedSchema {
    /// Create a new [`FixedSchema`] from the provided field values.
    pub fn new(name: String, version: usize, columns: Vec<FixedColumn>) -> Self {
        Self {
            name,
            version,
            columns,
        }
    }

    /// Create a new [`FixedSchema`] by reading a .json file at the provided path.
    ///
    /// # Errors
    /// This function will return an error under a number of different circumstances. These error conditions
    /// are listed below.
    ///
    /// * [`NotFound`]: The specified file does not exist and neither `create` or `create_new` is set.
    /// * [`NotFound`]: One of the directory components of the file path does not exist.
    /// * [`PermissionDenied`]: The user lacks permission to get the specified access rights for the file.
    /// * [`PermissionDenied`]: The user lacks permission to open one of the directory components of the specified path.
    /// * [`AlreadyExists`]: `create_new` was specified and the file already exists.
    /// * [`InvalidInput`]: Invalid combinations of open options (truncate without write access, no access mode set, etc.).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use evolution_schema::schema::FixedSchema;
    /// use std::path::PathBuf;
    ///
    /// let path: PathBuf = PathBuf::from(r"/path/to/my/schema.json");
    /// let schema: FixedSchema = FixedSchema::from_path(path).unwrap();
    ///
    /// println!("This is my cool schema: {:?}", schema);
    /// ```
    ///
    /// [`AlreadyExists`]: io::ErrorKind::AlreadyExists
    /// [`InvalidInput`]: io::ErrorKind::InvalidInput
    /// [`NotFound`]: io::ErrorKind::NotFound
    /// [`PermissionDenied`]: io::ErrorKind::PermissionDenied
    pub fn from_path(path: PathBuf) -> Result<Self> {
        let schema: Self = serde_json::from_slice(&fs::read(path)?)?;
        Ok(schema)
    }

    /// Get the name of the schema.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the version of the schema.
    pub fn version(&self) -> usize {
        self.version
    }

    /// Get the columns of the schema.
    pub fn columns(&self) -> &Vec<FixedColumn> {
        &self.columns
    }

    /// Get the number of columns in the schema.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Get the total length of a fixed-length row.
    pub fn row_length(&self) -> usize {
        self.columns
            .iter()
            .map(|c| c.length())
            .sum()
    }

    /// Get the names of the columns.
    pub fn column_names(&self) -> Vec<&String> {
        self.columns
            .iter()
            .map(|c| c.name())
            .collect::<Vec<&String>>()
    }

    /// Get the offset indices for each column (in number of runes).
    pub fn column_offsets(&self) -> Vec<usize> {
        self.columns
            .iter()
            .map(|c| c.offset())
            .collect::<Vec<usize>>()
    }

    /// Get the lengths of each column (in number of runes).
    pub fn column_lengths(&self) -> Vec<usize> {
        self.columns
            .iter()
            .map(|c| c.length())
            .collect::<Vec<usize>>()
    }

    /// Get the columns that are nullable.
    pub fn nullable_columns(&self) -> Vec<&FixedColumn> {
        self.columns
            .iter()
            .filter(|c| c.is_nullable())
            .collect::<Vec<&FixedColumn>>()
    }

    /// Get the columns that are not nullable.
    pub fn not_nullable_columns(&self) -> Vec<&FixedColumn> {
        self.columns
            .iter()
            .filter(|c| !c.is_nullable())
            .collect::<Vec<&FixedColumn>>()
    }

    /// Get the datatype of each column.
    pub fn dtypes(&self) -> Vec<DataType> {
        self.columns
            .iter()
            .map(|c| c.dtype())
            .collect::<Vec<DataType>>()
    }
}

impl Schema for FixedSchema {}

#[cfg(test)]
mod tests_schema {
    use super::*;
    use padder::{Alignment, Symbol};

    #[test]
    fn test_deserialize_schema_from_file() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("res/test_valid_schema.json");

        let columns: Vec<FixedColumn> = vec![
            FixedColumn::new(
                String::from("id"),
                0 as usize,
                9 as usize,
                DataType::Int32,
                Alignment::Right,
                Symbol::Whitespace,
                false,
            ),
            FixedColumn::new(
                String::from("NotCoolColumn"),
                9 as usize,
                149 as usize,
                DataType::LargeUtf8,
                Alignment::Left,
                Symbol::Five,
                false,
            ),
        ];

        let a: FixedSchema = FixedSchema::from_path(path).unwrap();
        let b: FixedSchema = FixedSchema::new(
            String::from("ValidTestSchema"),
            8914781578,
            columns,
        );

        assert_eq!(a.name(), b.name());
        assert_ne!(a.version(), b.version());

        let a_columns: &Vec<FixedColumn> = a.columns();
        let b_columns: &Vec<FixedColumn> = b.columns();

        assert_eq!(a_columns[0].name(), b_columns[0].name());
        assert_eq!(a_columns[0].offset(), b_columns[0].offset());
        assert_eq!(a_columns[0].length(), b_columns[0].length());
        assert_eq!(a_columns[0].dtype(), b_columns[0].dtype());
        assert_eq!(a_columns[0].alignment(), b_columns[0].alignment());
        assert_eq!(a_columns[0].pad_symbol(), b_columns[0].pad_symbol());
        assert_eq!(a_columns[0].is_nullable(), b_columns[0].is_nullable());

        assert_ne!(a_columns[1].name(), b_columns[1].name());
        assert_eq!(a_columns[1].offset(), b_columns[1].offset());
        assert_ne!(a_columns[1].length(), b_columns[1].length());
        assert_ne!(a_columns[1].dtype(), b_columns[1].dtype());
        assert_ne!(a_columns[1].alignment(), b_columns[1].alignment());
        assert_ne!(a_columns[1].pad_symbol(), b_columns[1].pad_symbol());
        assert_eq!(a_columns[1].is_nullable(), b_columns[1].is_nullable());

        assert_ne!(a.num_columns(), b.num_columns());
        assert_eq!(a.row_length(), b.row_length());
        assert_ne!(a.column_names(), b.column_names());
        assert_ne!(a.column_offsets(), b.column_offsets());
        assert_ne!(a.column_lengths(), b.column_lengths());
        assert_ne!(a.nullable_columns(), b.nullable_columns());
        assert_ne!(a.not_nullable_columns(), b.not_nullable_columns());
        assert_ne!(a.dtypes(), b.dtypes());
    }

    #[test]
    #[should_panic]
    fn test_deserialize_invalid_schema_from_file() {
        let mut path: PathBuf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("res/test_invalid_schema.json");

        let _: FixedSchema = FixedSchema::from_path(path).unwrap();
    }
}