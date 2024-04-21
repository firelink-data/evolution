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
* Last updated: 2023-11-21
*/

use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::write::{FileWriter};

use crate::schema;

use crate::converters::{Converter, MasterBuilders};
use crate::slicers::{FnFindLastLineBreak, FnLineBreakLen};
use arrow2::array::MutablePrimitiveArray;
use arrow2::types::NativeType;
use rayon::prelude::*;
use std::fs::File;
use std::path::PathBuf;
use std::str;
use std::sync::Arc;
use str::from_utf8_unchecked;
use arrow2::io::ipc::write::Record;
use arrow2::io::parquet::write::{CompressionOptions, Version, WriteOptions};
use arrow_array::ArrayRef;
use parquet::arrow::ArrowWriter;

pub(crate) struct Slice2Arrow2<'a> {
    //    pub(crate) file_out: File,
    pub(crate) writer: ArrowWriter<File>,
    pub(crate) fn_line_break: FnFindLastLineBreak<'a>,
    pub(crate) fn_line_break_len: FnLineBreakLen,
    pub(crate) masterbuilders: MasterBuilders<String>,
}

impl<'a> Converter<'a> for Slice2Arrow2<'a> {
    fn set_line_break_handler(&mut self, fnl: FnFindLastLineBreak<'a>) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnFindLastLineBreak<'a> {
        self.fn_line_break
    }

    fn process(&mut self, slices: Vec<&[u8]>) -> (usize, usize) {
        let bytes_processed: usize = 0;

        let arc_masterbuilder = Arc::new(&self.masterbuilders);

        slices.par_iter().enumerate().for_each(|(i, n)| {
            let arc_mastbuilder_clone = Arc::clone(&arc_masterbuilder);
            parse_slice(i, n, &arc_mastbuilder_clone);
        });

        (bytes_processed, 0)
    }

    fn finish(&mut self) -> Result<(), &str> {
        todo!()
    }

    fn get_finish_bytes_written(&mut self) -> usize {
        todo!()
    }
}
fn parse_slice(i: usize, n: &&[u8], _master_builder: &Arc<&MasterBuilders<String>>) {
    println!("index {} {}", i, n.len());

    // TODO make safe/unsafe configurable
    let _text: &str = unsafe { from_utf8_unchecked(&n) };

    let _offset = 0;
}

pub(crate) struct ColumnBuilderType<T1: NativeType> {
    pub rows: MutablePrimitiveArray<T1>,
}

impl ColumnBuilder for ColumnBuilderType<i32> {
    fn parse_value(&mut self, name: &str)
    where
        Self: Sized,
    {
        match name.parse::<i32>() {
            Ok(n) => {
                self.rows.push(Some(n));
                n
            }
            Err(_e) => {
                self.nullify();
                0
            }
        };
    }

    fn finish_column(&mut self)
    where
        Self: Sized,
    {
        todo!()
    }

    fn nullify(&mut self)
    where
        Self: Sized,
    {
        self.rows.push(None);
    }
}

impl ColumnBuilder for ColumnBuilderType<i64> {
    fn parse_value(&mut self, name: &str)
    where
        Self: Sized,
    {
        match name.parse::<i64>() {
            Ok(n) => {
                self.rows.push(Some(n));
                n
            }
            Err(_e) => {
                self.nullify();
                0
            }
        };
    }

    fn finish_column(&mut self)
    where
        Self: Sized,
    {
        todo!()
    }

    fn nullify(&mut self)
    where
        Self: Sized,
    {
        self.rows.push(None);
    }
}

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

/*
pub(crate) struct MasterBuilder<'a> {
    #[allow(dead_code)]
    builders: Vec<Box<dyn Sync + Send + 'a + ColumnBuilder>>,
}
*/

unsafe impl Send for MasterBuilders<String> {}
unsafe impl Sync for MasterBuilders<String> {}

impl MasterBuilders<FileWriter<File>> {
    pub fn writer_factory<'a>(&mut self, out_file: &PathBuf) -> FileWriter<File> {
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit: None,
        };

        let b: &mut Vec<Box<dyn Sync + Send + crate::converters::ColumnBuilder<String>>> = self.builders.get_mut(0).unwrap();
        let mut br: Vec<String> = vec![];
        for bb in b.iter_mut() {
            br.push(bb.finish());
        }


//        let schema = Schema::from(vec![field]);
        let schema=todo!();
        let mut writer = FileWriter::try_new(out_file, schema, options)?;
    }


        pub fn builder_factory2<'a>(schema_path: PathBuf, instances: i16) -> Self {
        //    builders: &mut Vec<Box<dyn ColumnBuilder>>
        let schema = schema::FixedSchema::from_path(schema_path.into());
        let antal_col = schema.num_columns();
        let mut builders: Vec<Vec<Box<dyn crate::converters::ColumnBuilder<String> + Sync + Send>>> = Vec::new();

        for _i in 1..=instances {
            let mut buildersmut: Vec<Box<dyn crate::converters::ColumnBuilder<String> + Sync + Send>> =
                Vec::with_capacity(antal_col);


            for val in schema.iter() {
                match val.dtype().as_str() {
                    "i32" => buildersmut.push(Box::new(ColumnBuilderType::<i32> {
                        rows: MutablePrimitiveArray::new(),
                    })),
                    "i64" => buildersmut.push(Box::new(ColumnBuilderType::<i64> {
                        rows: MutablePrimitiveArray::new(),
                    })),

                    &_ => {}
                };
            }
            builders.push(buildersmut);

        }

        MasterBuilders { builders }
    }
}
