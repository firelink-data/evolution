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

use std::fmt::Debug;
use arrow::array::{ArrayRef, RecordBatch};
use parquet::format;

use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;
use std::thread::JoinHandle;
use std::time::Duration;
use arrow::datatypes::SchemaRef;
use ordered_channel::Sender;

use self::residual_slicer::SLICER_IN_CHUNK_SIZE;
use parquet::errors::{ParquetError, Result};

pub(crate) mod arrow_converter;
pub(crate) mod residual_slicer;
pub(crate) mod self_converter;
mod trimmer;
mod threaded_file_output;

pub(crate) struct ChunkAndResidue {
    pub(crate) chunk: Box<[u8; SLICER_IN_CHUNK_SIZE]>,
}
pub(crate) trait Slicer<'a> {
    fn slice_and_convert(
        &mut self,
        converter: Box<dyn 'a + Converter<'a>>,
        infile: fs::File,
        n_threads: usize,
    ) -> Result<crate::chunked::Stats, &str>;
}
pub(crate) struct Stats {
    pub(crate) bytes_in: usize,
    pub(crate) bytes_out: usize,
    pub(crate) num_rows: i64,

    pub(crate) read_duration: Duration,
    pub(crate) parse_duration: Duration,
    pub(crate) builder_write_duration: Duration,
}

pub(crate) type FnLineBreakLen = fn() -> usize;
#[allow(dead_code)]
pub(crate) fn line_break_len_cr() -> usize {
    1_usize
}
#[allow(dead_code)]
pub(crate) fn line_break_len_crlf() -> usize {
    2_usize
}

pub(crate) type FnFindLastLineBreak<'a> = fn(bytes: &'a [u8]) -> (bool, usize);
#[allow(dead_code)]
pub(crate) fn find_last_nlcr(bytes: &[u8]) -> (bool, usize) {
    if bytes.is_empty() {
        return (false, 0); // TODO should report err ...
    }

    let mut p2 = bytes.len() - 1;

    if 0 == p2 {
        return (false, 0); // hmm
    }

    loop {
        if bytes[p2 - 1] == 0x0d && bytes[p2] == 0x0a {
            return (true, p2 + 1);
        }
        if 0 == p2 {
            return (false, 0); // indicate we didnt find nl
        }

        p2 -= 1;
    }
}

#[allow(dead_code)]
pub(crate) fn find_last_nl(bytes: &[u8]) -> (bool, usize) {
    if bytes.is_empty() {
        return (false, 0); // Indicate we didnt found nl.
    }

    let mut p2 = bytes.len() - 1;

    if 0 == p2 {
        return (false, 0); // hmm
    }

    loop {
        if bytes[p2] == 0x0a {
            return (true, p2);
        }
        if 0 == p2 {
            return (false, 0); // indicate we didnt find nl
        }
        p2 -= 1;
    }
}
pub(crate) struct IterRevolver<'a, T> {
    shards: *mut T,
    next: usize,
    len: usize,
    phantom: std::marker::PhantomData<&'a mut [T]>,
}

impl<'a, T> From<&'a mut [T]> for crate::chunked::IterRevolver<'a, T> {
    fn from(shards: &'a mut [T]) -> crate::chunked::IterRevolver<'a, T> {
        IterRevolver {
            next: 0,
            len: shards.len(),
            shards: shards.as_mut_ptr(),
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T> Iterator for IterRevolver<'a, T> {
    type Item = &'a mut T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next < self.len {
            self.next += 1;
        } else {
            self.next = 1;
        }
        unsafe { Some(&mut *self.shards.offset(self.next as isize - 1)) }
    }
}

#[allow(dead_code)]
pub(crate) trait Converter<'a> {
    fn set_line_break_handler(&mut self, fn_line_break: FnFindLastLineBreak<'a>);
    fn get_line_break_handler(&self) -> FnFindLastLineBreak<'a>;

    //    fn process(& mut self, slices: Vec< &'a[u8]>) -> usize;
    fn process(&mut self, slices: Vec<&'a [u8]>) -> (usize, usize, Duration, Duration);
    fn setup(&mut self) -> JoinHandle<Result<Stats>>;
    fn shutdown(&mut self);
}

pub trait ColumnBuilder {
    fn parse_value(&mut self, name: &[u8]) -> usize;
    fn finish(&mut self) -> (&str, ArrayRef);
    //    fn name(&  self) -> &String;
}

pub trait arrow_file_output {
    fn setup(&mut self,schema:SchemaRef,outfile:PathBuf)-> (Sender<RecordBatch>, JoinHandle<Result<Stats>>);
}
