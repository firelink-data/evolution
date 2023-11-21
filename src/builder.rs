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
* File created: 2023-11-21
* Last updated: 2023-11-21
*/


use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::ipc::write::Record;

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
struct FixedRow {

    ///
    fixed_fields: Vec<Box<FixedField>>,
}

///
#[allow(dead_code)]
struct FixedTableChunk<'a> {

    ///
    chunk_idx: u32,

    ///
    fixed_table: Box<FixedTable<'a>>,

    column_builders: Vec<Box<dyn ColumnBuilder>>,

    // record_builder: Vec<Box< ??? >>
    
    records: Vec<Box<Record<'a>>>,

    bytes: Vec<u8>,
}

///
#[allow(dead_code)]
struct FixedTable<'a> {

    ///
    bytes: Vec<u8>,

    ///
    fixed_table_chunks: Vec<Box<FixedTableChunk<'a>>>,

    ///
    row: FixedRow,

    ///
    schemas: Vec<Box<Schema>>,

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
    fn parse_value(&self, name: String) -> bool;

    ///
    fn finish_column(&self) -> bool;

    /// I think this function won't be necessary.
    /// `[arrow2]` supports bitmap nulling out-of-the-box.
    fn nullify(&self);
}
