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

use crate::{converters, schema};

use crate::converters::{ColumnBuilder, Converter, MasterBuilders};
use crate::slicers::{FnFindLastLineBreak, FnLineBreakLen};
use arrow2::array::{MutableArray, MutablePrimitiveArray, MutableUtf8Array, Utf8Array};
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
use arrow_array::builder::BooleanBuilder;
use parquet::arrow::ArrowWriter;

pub(crate) struct Slice2Arrow2 {
    //    pub(crate) file_out: File,
    pub(crate) writer: FileWriter<File>,
    pub(crate) fn_line_break: FnFindLastLineBreak,
    pub(crate) fn_line_break_len: FnLineBreakLen,
    pub(crate) masterbuilders: MasterBuilders<String>,
}

impl Converter<'_> for Slice2Arrow2 {
    fn set_line_break_handler(&mut self, fnl: FnFindLastLineBreak) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnFindLastLineBreak {
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

//pub(crate) struct ColumnBuilderType<T1: NativeType> {
//    pub rows: MutablePrimitiveArray<T1>,
//}
struct Handleri32Builder {
    i32_rows:  MutablePrimitiveArray<i32>,
    runes_in_column: usize,
    name: String,
}
impl  ColumnBuilder<String> for Handleri32Builder {
    fn parse_value(&mut self, data: &[u8]) -> usize {
        let (start, stop) =
            converters::column_length_num_rightaligned(data, self.runes_in_column as i16);
        match atoi_simd::parse::<i32>(&data[start..stop]) {
            Ok(n) => {
                self.i32_rows.push(Some(n));
            }
            Err(_e) => {
                self.i32_rows.push_null();
            }
        };
        // todo fix below
        self.runes_in_column
    }

    fn finish(&mut self) -> String {
        todo!()
    }
}
struct HandleriUtf8Builder {
    utf8_rows:  MutableUtf8Array<i32>,
    runes_in_column: usize,
    name: String,
}
impl  ColumnBuilder<String> for crate::converters::arrow2_converter::HandleriUtf8Builder {

    fn parse_value(&mut self, data: &[u8])-> usize {

        let column_length: usize = converters::column_length(data, self.runes_in_column as i16);
        // Me dont like ... what is the cost ? Could it be done once for the whole chunk ?
        let text: &str = unsafe { from_utf8_unchecked(&data[..column_length]) };

        match column_length {
            0 => {self.utf8_rows.push_null()},
            _ => {self.utf8_rows.push(Some(text))}
        }
        column_length
    }

    fn finish(&mut self) -> String {
        todo!()
    }
}

struct Handleri64Builder {
    i64_rows:  MutablePrimitiveArray<i64>,
    runes_in_column: usize,
    name: String,
}
impl  ColumnBuilder<String> for crate::converters::arrow2_converter::Handleri64Builder {
    fn parse_value(&mut self, data: &[u8]) -> usize {
        let (start, stop) =
            converters::column_length_num_rightaligned(data, self.runes_in_column as i16);
        match atoi_simd::parse::<i64>(&data[start..stop]) {
            Ok(n) => {
                self.i64_rows.push(Some(n));
            }
            Err(_e) => {
                self.i64_rows.push_null();
            }
        };
        // todo fix below
        self.runes_in_column
    }

    fn finish(&mut self) -> String {
        todo!()
    }
}


unsafe impl<'a> Send for MasterBuilders<String> {}
unsafe impl<'a> Sync for MasterBuilders<String> {}

impl MasterBuilders<String> {
    pub fn writer_factory2(&mut self, out_file: &PathBuf) -> FileWriter<File> {
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit: None,
        };



//        let schema = Schema::from(vec![field]);
        let schema=todo!();
        let file = File::create(out_file).expect("");
        let mut writer = FileWriter::try_new(file, schema, options).expect("");
        writer
    }


        pub fn builder_factory2(schema_path: PathBuf, instances: i16) -> Self {
        //    builders: &mut Vec<Box<dyn ColumnBuilder>>
        let schema = schema::FixedSchema::from_path(schema_path.into());
        let antal_col = schema.num_columns();
        let mut builders: Vec<Vec<Box<dyn crate::converters::ColumnBuilder<String> + Sync + Send>>> = Vec::new();

        for _i in 1..=instances {
            let mut buildersmut: Vec<Box<dyn crate::converters::ColumnBuilder<String> + Sync + Send>> =
                Vec::with_capacity(antal_col);


            for val in schema.iter() {
                match val.dtype().as_str() {
                    "i32" => buildersmut.push(Box::new(Handleri32Builder {
                        i32_rows: Default::default(),
                        runes_in_column: val.length(),
                        name: val.name().clone(),
                    } )),

                    "i64" => buildersmut.push(Box::new(Handleri64Builder {
                        i64_rows: Default::default(),
                        runes_in_column: val.length(),
                        name: val.name().clone(),
                    })),

                    "utf8" => buildersmut.push(Box::new(HandleriUtf8Builder {
                        utf8_rows: Default::default(),
                        runes_in_column: val.length(),
                        name: val.name().clone(),
                    })),

                    &_ => {}
                };
            }
            builders.push(buildersmut);

        }

        MasterBuilders { builders }
    }
}
