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
* File created: 2024-02-05
* Last updated: 2024-02-05
*/

use crate::schema::{self, FixedSchema};
use crossbeam::channel;
use log::{debug, info};
use padder::*;
use rand::distributions::{Alphanumeric, DistString};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::{thread, usize};
use std::time::SystemTime;

pub(crate) static DEFAULT_MOCKED_FILENAME_LEN: usize = 16;
pub(crate) static DEFAULT_ROW_BUFFER_LEN: usize = 1024 * 1024;

///
pub struct Mocker {
    schema: schema::FixedSchema,
    target_file: Option<PathBuf>,
    n_threads: usize,
    multithreaded: bool,
}

///
impl Mocker {
    ///
    pub fn new(schema: schema::FixedSchema, target_file: Option<PathBuf>, n_threads: usize) -> Self {
        Self { schema, target_file, n_threads, multithreaded: n_threads > 1 }
    }

    ///
    pub fn generate(&self, n_rows: usize) {
        if self.multithreaded {
            self.generate_multithreaded(n_rows);
        } else {
            self.generate_single_threaded();
        }
    }

    ///
    fn generate_single_threaded(&self) {
        todo!()
    }

    ///
    fn generate_multithreaded(&self, n_rows: usize) {
        let thread_workload = self.distribute_thread_workload(n_rows);
        xd_mock(n_rows, thread_workload, self.schema.to_owned(), self.n_threads);
    }

    /// Calculate how many rows each thread should work on generating.
    fn distribute_thread_workload(&self, n_rows: usize) -> Vec<usize> {
        let n_rows_per_thread = n_rows / self.n_threads;
        (0..self.n_threads).map(|_| n_rows_per_thread).collect::<Vec<usize>>()
    }
}

pub fn xd_mock(n_rows: usize, thread_workload: Vec<usize>, schema: FixedSchema, n_threads: usize) {
    let remaining_rows = n_rows - thread_workload.iter().sum::<usize>();
    info!("Distributed thread workload: {:?}", thread_workload);
    info!("Remaining rows to handle: {}", remaining_rows);

    let arc_schema = Arc::new(schema);
    let (sender, receiver) = channel::unbounded();

    let threads: Vec<_> = (0..n_threads)
        .map(|t| {
            let arc_clone = Arc::clone(&arc_schema);
            let sender_clone = sender.clone();
            let rows = thread_workload[t];
            thread::spawn(move ||
                generate_from_thread(t, arc_clone, rows, sender_clone)
            )
        })
        .collect();

    drop(sender);

    for handle in threads {
        handle.join().expect("Could not join thread handle...");
    }

}

pub fn generate_from_thread(
    thread: usize,
    schema: Arc<FixedSchema>,
    n_rows: usize,
    sender: channel::Sender<Vec<u8>>,
) {
    let rowlen = schema.row_len();
    let mut buffer: Vec<u8> = Vec::with_capacity(DEFAULT_ROW_BUFFER_LEN * rowlen + DEFAULT_ROW_BUFFER_LEN * 2);

    for row in 0..n_rows {
        info!("Hello from thread {}", thread);
    }
}

pub(crate) fn mock_bool<'a>(len: usize) -> &'a str {
    if len > 3 {
        return "true";
    }
    "false"
}

pub(crate) fn mock_number<'a>(_len: usize) -> &'a str {
    "3"
}

pub(crate) fn mock_string<'a>(_len: usize) -> &'a str {
    "hejj"
}

