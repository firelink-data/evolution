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
* File created: 2023-11-21
* Last updated: 2024-02-17
*/

use std::path::PathBuf;

use arrow2::array::MutablePrimitiveArray;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::ipc::write::Record;

use crate::builder_datatypes::ColumnBuilderType;
use crate::schema;

///
#[allow(dead_code)]
struct FixedField {
    /// The destination field.
    field: Field,

    /// The source datatype.
    dtype: DataType,

    ///
    len: u32,

    ///
    id: u32,
}

///
#[allow(dead_code)]
struct FixedRow<'a> {
    ///
    fixed_fields: Vec<&'a FixedField>,
}

///
#[allow(dead_code)]
struct FixedTableChunk<'a> {
    ///
    chunk_idx: u32,

    ///
    fixed_table: &'a FixedTable<'a>,

    column_builders: Vec<Box<dyn ColumnBuilder>>,

    // record_builder: Vec<Box< ??? >>
    records: Vec<&'a Record<'a>>,

    bytes: Vec<u8>,
}

///
#[allow(dead_code)]
struct FixedTable<'a> {
    ///
    bytes: Vec<u8>,

    ///
    fixed_table_chunks: Vec<&'a FixedTableChunk<'a>>,

    ///
    row: FixedRow<'a>,

    ///
    schemas: Vec<&'a Schema>,

    ///
    table_n_cols: Vec<u32>,

    ///
    header: Option<String>,

    ///
    footer: Option<String>,

    ///
    encoding: String,
}

///
pub trait ColumnBuilder {
    ///
    fn parse_value(&mut self, name: &str);

    ///
    fn finish_column(&mut self);

    /// I think this function won't be necessary.
    /// `[arrow2]` supports bitmap nulling out-of-the-box.
    fn nullify(&mut self);
}

pub(crate) fn parse_from_schema(
    schema_path: PathBuf,
    _in_file_path: PathBuf,
    _out_file_path: PathBuf,
    _n_threads: i16,
) {
    let mut builders: Vec<Box<dyn ColumnBuilder>> = Vec::new();
    for val in schema::FixedSchema::from_path(schema_path.into()).iter() {
        match val.dtype().as_str() {
            "i32" => builders.push(Box::new(ColumnBuilderType::<i32> {
                rows: MutablePrimitiveArray::new(),
            })),
            "i64" => builders.push(Box::new(ColumnBuilderType::<i64> {
                rows: MutablePrimitiveArray::new(),
            })),

            &_ => {}
        };
    }
}
/*

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

*/
