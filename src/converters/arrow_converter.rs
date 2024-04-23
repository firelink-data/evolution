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
use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::str::from_utf8_unchecked;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::iter::IndexedParallelIterator;
use rayon::prelude::*;

use crate::converters::{ColumnBuilder, Converter, MasterBuilders};
use crate::slicers::{FnFindLastLineBreak, FnLineBreakLen};
use crate::{converters, schema};
use debug_print::debug_println;

pub(crate) struct Slice2Arrow<'a> {
    //    pub(crate) file_out: File,
    pub(crate) writer: ArrowWriter<File>,
    pub(crate) fn_line_break: FnFindLastLineBreak,
    pub(crate) fn_line_break_len: FnLineBreakLen,
    pub(crate) masterbuilders: MasterBuilders<(&'a str, ArrayRef)>,
}

unsafe impl Send for MasterBuilders<(&str, ArrayRef)> {}
unsafe impl Sync for MasterBuilders<(&str, ArrayRef)> {}


impl MasterBuilders<(&str, ArrayRef)> {
    pub fn writer_factory<'a>(&mut self, out_file: &PathBuf) -> ArrowWriter<File> {
        let _out_file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(out_file)
            .expect("aaa");

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let b: &mut Vec<Box<dyn Sync + Send + ColumnBuilder<(&str, ArrayRef)>>> = self.builders.get_mut(0).unwrap();
        let mut br: Vec<(&str, ArrayRef)> = vec![];
        for bb in b.iter_mut() {
            br.push(bb.finish());
        }

        let batch = RecordBatch::try_from_iter(br).unwrap();
        let writer: ArrowWriter<File> =
            ArrowWriter::try_new(_out_file, batch.schema(), Some(props.clone())).unwrap();
        writer
    }

    pub fn builders_factory<'a>(schema_path: PathBuf, instances: i16) -> Self {
        let schema = schema::FixedSchema::from_path(schema_path.into());
        let antal_col = schema.num_columns();
        let mut builders: Vec<Vec<Box<dyn ColumnBuilder<(&str, ArrayRef)> + Sync + Send>>> = Vec::new();

        for _i in 1..=instances {
            let mut buildersmut: Vec<Box<dyn ColumnBuilder<(&str, ArrayRef)> + Sync + Send>> =
                Vec::with_capacity(antal_col);
            for val in schema.iter() {
                match val.dtype().as_str() {
                    "i32" => buildersmut.push(Box::new(HandlerInt32Builder {
                        int32builder: Int32Builder::new(),
                        runes_in_column: val.length(),
                        name: val.name().clone(),
                    })),
                    "i64" => buildersmut.push(Box::new(HandlerInt64Builder {
                        int64builder: Int64Builder::new(),
                        runes_in_column: val.length(),
                        name: val.name().clone(),
                    })),
                    "boolean" => buildersmut.push(Box::new(HandlerBooleanBuilder {
                        boolean_builder: BooleanBuilder::new(),
                        runes_in_column: val.length(),
                        name: val.name().clone(),
                    })),
                    "utf8" => buildersmut.push(Box::new(HandlerStringBuilder {
                        string_builder: StringBuilder::new(),
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

impl<'a> Converter<'a> for Slice2Arrow<'a> {
    fn set_line_break_handler(&mut self, fnl: FnFindLastLineBreak) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnFindLastLineBreak {
        self.fn_line_break
    }

    fn process(&mut self, slices: Vec<&'a [u8]>) -> (usize, usize) {
        let mut bytes_in: usize = 0;
        let mut bytes_out: usize = 0;

        let arc_slices = Arc::new(&slices);
        self.masterbuilders
            .builders
            .par_iter_mut()
            .enumerate()
            .for_each(|(i, n)| {
                let arc_slice_clone = Arc::clone(&arc_slices);
                match arc_slice_clone.get(i) {
                    None => {}
                    Some(_) => {
                        parse_slice(
                            i,
                            arc_slice_clone.get(i).unwrap(),
                            n,
                            (self.fn_line_break_len)(),
                        );
                    }
                }
            });

        for ii in slices.iter() {
            bytes_in += ii.len();
        }

        for b in self.masterbuilders.builders.iter_mut() {
            let mut br: Vec<(&str, ArrayRef)> = vec![];

            for bb in b.iter_mut() {
                br.push(bb.finish());
            }

            let batch = RecordBatch::try_from_iter(br).unwrap();

            self.writer.write(&batch).expect("Error Writing batch");
            bytes_out += self.writer.bytes_written();
            debug_println!("Batch write: accumulated bytes_written {}", bytes_out);
        }

        (bytes_in, bytes_out)
    }

    fn finish(&mut self) -> Result<(), &str> {
        
        match self.writer.finish() {
            Ok(_) => {Result::Ok(()) }
            Err(_) => {Result::Err("Could not finish write parquet") }
        }
    }

    fn get_finish_bytes_written(&mut self) -> usize {
        self.writer.bytes_written()
    }
}

fn parse_slice(
    _i: usize,
    n: &[u8],
    builders: &mut Vec<Box<dyn ColumnBuilder<(&str, ArrayRef)> + Send + Sync>>,
    linebreak: usize,
) {
    debug_println!("index slice={} slice len={}", _i, n.len());

    let mut cursor: usize = 0;
    while cursor < n.len() {
        for cb in &mut *builders {
            let bytelen = cb.parse_value(&n[cursor..]);
            cursor += bytelen;
        }
        cursor += linebreak; // TODO adjust to CR/LF mode
    }
}

struct HandlerInt32Builder {
    int32builder: Int32Builder,
    runes_in_column: usize,
    name: String,
}

impl ColumnBuilder<(&str, ArrayRef)> for HandlerInt32Builder {
    fn parse_value(&mut self, data: &[u8]) -> usize
    where
        Self: Sized,
    {
        let (start, stop) =
            converters::column_length_num_rightaligned(data, self.runes_in_column as i16);

        match atoi_simd::parse(&data[start..stop]) {
            Ok(n) => {
                self.int32builder.append_value(n);
            }
            Err(_e) => {
                self.int32builder.append_null();
            }
        };
        self.runes_in_column
    }

    fn finish(&mut self) -> (& str, ArrayRef) {
        (
            self.name.as_str(),
            Arc::new(self.int32builder.finish()) as ArrayRef,
        )
    }
}
struct HandlerInt64Builder {
    int64builder: Int64Builder,
    runes_in_column: usize,
    name: String,
}
impl ColumnBuilder<(&str, ArrayRef)> for HandlerInt64Builder {
    fn parse_value(&mut self, data: &[u8]) -> usize
    where
        Self: Sized,
    {
        let (start, stop) =
            converters::column_length_num_rightaligned(data, self.runes_in_column as i16);
        match atoi_simd::parse(&data[start..stop]) {
            Ok(n) => {
                self.int64builder.append_value(n);
            }
            Err(_e) => {
                self.int64builder.append_null();
            }
        };
        // todo fix below
        self.runes_in_column
    }

    fn finish(&mut self) -> (&'static str, ArrayRef) {
        (
            self.name.as_str(),
            Arc::new(self.int64builder.finish()) as ArrayRef,
        )
    }
}
// Might be better of to copy the actual data to array<str>[colnr]

struct HandlerStringBuilder {
    string_builder: StringBuilder,
    runes_in_column: usize,
    name: String,
}
impl ColumnBuilder<(&str, ArrayRef)> for HandlerStringBuilder {
    fn parse_value(&mut self, data: &[u8]) -> usize
    where
        Self: Sized,
    {
        let column_length: usize = converters::column_length(data, self.runes_in_column as i16);
        // Me dont like ... what is the cost ? Could it be done once for the whole chunk ?
        let text: &str = unsafe { from_utf8_unchecked(&data[..column_length]) };

        match text.is_empty() {
            false => {
                self.string_builder.append_value(text);
            }
            true => {
                self.string_builder.append_null();
            }
        };
        // todo fix below
        column_length
    }

    fn finish(&mut self) -> (&'static str, ArrayRef) {
        (
            self.name.as_str(),
            Arc::new(self.string_builder.finish()) as ArrayRef,
        )
    }
}

struct HandlerBooleanBuilder {
    boolean_builder: BooleanBuilder,
    runes_in_column: usize,
    name: String,
}

impl ColumnBuilder<(&str, ArrayRef)> for HandlerBooleanBuilder {
    fn parse_value(&mut self, data: &[u8]) -> usize
    where
        Self: Sized,
    {
        let (start, stop) =
            converters::column_length_char_rightaligned(data, self.runes_in_column as i16);

        let text: &str = unsafe { from_utf8_unchecked(&data[start..stop]) };

        match text.parse::<bool>() {
            Ok(n) => {
                self.boolean_builder.append_value(n);
            }
            Err(_e) => {
                self.boolean_builder.append_null();
            }
        };
        // todo fix below
        self.runes_in_column
    }

    fn finish(&mut self) -> (&'static str, ArrayRef) {
        (
            self.name.as_str(),
            Arc::new(self.boolean_builder.finish()) as ArrayRef,
        )
    }
}
