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
// File created: 2023-12-11
// Last updated: 2024-05-15
//

use crate::chunked::trimmer::{trimmer_factory, ColumnTrimmer};
use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;
use log::{debug, info};
use ordered_channel::bounded;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::format;
use rayon::iter::IndexedParallelIterator;
use rayon::prelude::*;

use std::fs;
use std::fs::File;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::from_utf8_unchecked;
use std::sync::Arc;

use super::{
    arrow_file_output, trimmer, ColumnBuilder, Converter, FnFindLastLineBreak, FnLineBreakLen,
    Stats,
};
use crate::chunked;
use crate::chunked::threaded_file_output::{ipc_file_out, output_factory, parquet_file_out};
use crate::cli::Targets;
use crate::datatype::DataType;
use crate::schema;
use arrow::datatypes::{Field, Schema, SchemaRef};
use atomic_counter::{AtomicCounter, ConsistentCounter};
use crossbeam::atomic::AtomicConsume;
use libc::bsearch;
use ordered_channel::Sender;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::FileMetaData;
use rayon::join;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::sync::mpsc::{sync_channel, Receiver, RecvError, SyncSender};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use thread::spawn;
//use ordered_channel::Sender;
//use crossbeam::channel::{Receiver, Sender};

static GLOBAL_COUNTER: AtomicUsize = ATOMIC_USIZE_INIT;

pub(crate) struct Slice2Arrow<'a> {
    //    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnFindLastLineBreak<'a>,
    pub(crate) fn_line_break_len: FnLineBreakLen,
    pub(crate) masterbuilders: MasterBuilders,
    pub(crate) consistent_counter: ConsistentCounter,
    pub(crate) target: Targets,
}

pub(crate) struct MasterBuilders {
    builders: Vec<Vec<Box<dyn Sync + Send + ColumnBuilder>>>,
    outfile: PathBuf,
    sender: Option<Sender<RecordBatch>>,
}

unsafe impl Send for MasterBuilders {}
unsafe impl Sync for MasterBuilders {}

impl MasterBuilders {
    pub fn schema_factory(&mut self) -> SchemaRef {
        let b: &mut Vec<Box<dyn Sync + Send + ColumnBuilder>> = self.builders.get_mut(0).unwrap();
        let mut br: Vec<(&str, ArrayRef)> = vec![];
        for bb in b.iter_mut() {
            br.push(bb.finish());
        }

        let batch = RecordBatch::try_from_iter(br).unwrap();
        batch.schema()
    }

    pub fn builders_factory(out_file: PathBuf, schema_path: PathBuf, instances: i16) -> Self {
        let schema = schema::FixedSchema::from_path(schema_path).unwrap();
        let antal_col = schema.num_columns();
        let mut builders: Vec<Vec<Box<dyn ColumnBuilder + Sync + Send>>> = Vec::new();

        for _i in 1..=instances {
            let mut buildersmut: Vec<Box<dyn ColumnBuilder + Sync + Send>> =
                Vec::with_capacity(antal_col);
            for col in schema.iter() {
                match col.dtype() {
                    DataType::Boolean => buildersmut.push(Box::new(HandlerBooleanBuilder {
                        boolean_builder: BooleanBuilder::new(),
                        runes_in_column: col.length(),
                        name: col.name().to_string().clone(),
                        trimmer: trimmer_factory(
                            DataType::Boolean,
                            col.alignment(),
                            col.pad_symbol(),
                        ),
                    })),
                    DataType::Float16 => todo!(),
                    DataType::Float32 => todo!(),
                    DataType::Float64 => todo!(),
                    DataType::Int16 => todo!(),
                    DataType::Int32 => buildersmut.push(Box::new(HandlerInt32Builder {
                        int32builder: Int32Builder::new(),
                        runes_in_column: col.length(),
                        name: col.name().to_string().clone(),
                        trimmer: trimmer_factory(
                            DataType::Int32,
                            col.alignment(),
                            col.pad_symbol(),
                        ),
                    })),
                    DataType::Int64 => buildersmut.push(Box::new(HandlerInt64Builder {
                        int64builder: Int64Builder::new(),
                        runes_in_column: col.length(),
                        name: col.name().to_string().clone(),
                        trimmer: trimmer_factory(
                            DataType::Int64,
                            col.alignment(),
                            col.pad_symbol(),
                        ),
                    })),
                    DataType::Utf8 => buildersmut.push(Box::new(HandlerStringBuilder {
                        string_builder: StringBuilder::new(),
                        runes_in_column: col.length(),
                        name: col.name().to_string().clone(),
                        trimmer: trimmer_factory(DataType::Utf8, col.alignment(), col.pad_symbol()),
                    })),
                    DataType::LargeUtf8 => buildersmut.push(Box::new(HandlerStringBuilder {
                        string_builder: StringBuilder::new(),
                        runes_in_column: col.length(),
                        name: col.name().to_string().clone(),
                        trimmer: trimmer_factory(
                            DataType::LargeUtf8,
                            col.alignment(),
                            col.pad_symbol(),
                        ),
                    })),
                }
            }
            builders.push(buildersmut);
        }
        MasterBuilders {
            builders: builders,
            outfile: out_file,
            sender: None,
        }
    }
}

impl<'a> Converter<'a> for Slice2Arrow<'a> {
    fn set_line_break_handler(&mut self, fnl: FnFindLastLineBreak<'a>) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnFindLastLineBreak<'a> {
        self.fn_line_break
    }

    fn process(&mut self, slices: Vec<&'a [u8]>) -> (usize, usize, Duration, Duration) {
        let mut bytes_in: usize = 0;
        let mut bytes_out: usize = 0;
        let mut parse_duration: Duration = Duration::new(0, 0);
        let mut builder_write_duration: Duration = Duration::new(0, 0);

        let start_parse = Instant::now();
        let offset: usize = self.consistent_counter.get();

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
                        let mut br: Vec<(&str, ArrayRef)> = vec![];

                        for bb in n.iter_mut() {
                            br.push(bb.finish());
                        }
                        let record_batch = RecordBatch::try_from_iter(br).unwrap();

                        let _ = self
                            .masterbuilders
                            .sender
                            .clone()
                            .unwrap()
                            .send(offset + i, record_batch);
                    }
                }
            });

        for ii in slices.iter() {
            bytes_in += ii.len();
        }
        let offset: usize = self
            .consistent_counter
            .add(self.masterbuilders.builders.len());

        parse_duration = start_parse.elapsed();
        (bytes_in, bytes_out, parse_duration, builder_write_duration)
    }

    fn setup(&mut self) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        output_factory(
            self.target.clone(),
            self.masterbuilders.schema_factory(),
            self.masterbuilders.outfile.clone(),
        )
    }

    fn shutdown(&mut self) {
        //        converter.shutdown();
        let schema = Schema::new(vec![Field::new(
            "id",
            arrow::datatypes::DataType::Int32,
            false,
        )]);

        let emptyrb = arrow::record_batch::RecordBatch::new_empty(Arc::new(schema));
        let c = self.consistent_counter.get();
        let _ = &self.masterbuilders.sender.clone().unwrap().send(c, emptyrb);
    }
}

fn parse_slice(
    _i: usize,
    n: &[u8],
    builders: &mut Vec<Box<dyn ColumnBuilder + Send + Sync>>,
    linebreak: usize,
) {
    debug!("index slice={} slice len={}", _i, n.len());

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
    trimmer: Box<dyn ColumnTrimmer + Send + Sync>,
}

impl ColumnBuilder for HandlerInt32Builder {
    fn parse_value(&mut self, data: &[u8]) -> usize
    where
        Self: Sized,
    {
        let (start, stop) = self
            .trimmer
            .find_start_stop(data, self.runes_in_column as i16);

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

    fn finish(&mut self) -> (&str, ArrayRef) {
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
    trimmer: Box<dyn ColumnTrimmer + Send + Sync>,
}
impl ColumnBuilder for HandlerInt64Builder {
    fn parse_value(&mut self, data: &[u8]) -> usize
    where
        Self: Sized,
    {
        let (start, stop) = self
            .trimmer
            .find_start_stop(data, self.runes_in_column as i16);
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

    fn finish(&mut self) -> (&str, ArrayRef) {
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
    trimmer: Box<dyn ColumnTrimmer + Send + Sync>,
}
impl ColumnBuilder for HandlerStringBuilder {
    fn parse_value(&mut self, data: &[u8]) -> usize
    where
        Self: Sized,
    {
        let column_length: usize =
            trimmer::count_rune_bytelength(data, self.runes_in_column as i16);
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

    fn finish(&mut self) -> (&str, ArrayRef) {
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
    trimmer: Box<dyn ColumnTrimmer + Send + Sync>,
}

impl ColumnBuilder for HandlerBooleanBuilder {
    fn parse_value(&mut self, data: &[u8]) -> usize
    where
        Self: Sized,
    {
        let (start, stop) = self
            .trimmer
            .find_start_stop(data, self.runes_in_column as i16);

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

    fn finish(&mut self) -> (&str, ArrayRef) {
        (
            self.name.as_str(),
            Arc::new(self.boolean_builder.finish()) as ArrayRef,
        )
    }
}
