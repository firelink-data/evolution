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

use crate::chunked::Stats;
use log::info;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use arrow::array::RecordBatch;
use ordered_channel::Sender;
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::FileMetaData;
use parquet::format;
use std::fs::File;
use std::io::Write;
use std::sync::mpsc::SyncSender;
use std::time::Duration;
use tokio::runtime;
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

use super::{Converter, FnFindLastLineBreak};

pub struct SampleSliceAggregator<'a> {
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnFindLastLineBreak<'a>,
}

impl<'a> Converter<'a> for SampleSliceAggregator<'a> {
    fn set_line_break_handler(&mut self, fnl: FnFindLastLineBreak<'a>) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnFindLastLineBreak<'a> {
        self.fn_line_break
    }

    fn process(&mut self, slices: Vec<&'a [u8]>) -> (usize, usize, Duration, Duration) {
        let mut bytes_processed: usize = 0;
        let duration = Duration::new(0, 0);

        slices
            .par_iter()
            .enumerate()
            .for_each(|(i, n)| info!("index {} {}", i, n.len()));
        for val in slices {
            self.file_out.write_all(val).expect("dasd");
            let l = val.len();
            bytes_processed += l;
        }
        (bytes_processed, 0, duration, duration)
    }

    fn setup(&mut self, rt: &Runtime) -> (Sender<RecordBatch>, JoinHandle<Result<Stats>>) {
        todo!()
    }

    fn shutdown(&mut self, rt: &runtime::Runtime, jh: JoinHandle<Result<Stats>>) {
        todo!()
    }
}
