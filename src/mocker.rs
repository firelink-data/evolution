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
* Last updated: 2024-02-27
*/

use crate::schema::{self, FixedSchema};
use crossbeam::channel;
use log::{info, warn};
use padder::*;
use rand::distributions::{Alphanumeric, DistString};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{thread, usize};

// This default value should depend on the memory capacity of the system
// running the program. Because the workers produce buffers faster than
// the master can write them to disk we need to bound the worker/master
// channel. If you have a lot of system memory, you can increase this value,
// but if you increase it too much the program will run out of system memory.
pub(crate) static DEFAULT_THREAD_CHANNEL_CAPACITY: usize = 128;
pub(crate) static DEFAULT_MIN_N_ROWS_FOR_MULTITHREADING: usize = 1000;
pub(crate) static DEFAULT_MOCKED_FILENAME_LEN: usize = 8;
pub(crate) static DEFAULT_ROW_BUFFER_LEN: usize = 1024;

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
    pub fn new(
        schema: schema::FixedSchema,
        target_file: Option<PathBuf>,
        n_threads: usize,
    ) -> Self {
        Self {
            schema,
            target_file,
            n_threads,
            multithreaded: n_threads > 1,
        }
    }

    ///
    pub fn generate(&self, n_rows: usize) {
        if self.multithreaded && n_rows > DEFAULT_MIN_N_ROWS_FOR_MULTITHREADING {
            self.generate_multithreaded(n_rows);
        } else {
            if self.multithreaded {
                warn!(
                    "You specified to use multithreading but only want to mock {} rows",
                    n_rows
                );
                warn!("This is done more efficiently single-threaded, ignoring multithreading...");
            }
            self.generate_single_threaded(n_rows);
        }
    }

    ///
    fn generate_single_threaded(&self, n_rows: usize) {
        let rowlen: usize = self.schema.row_len();
        // FOR WINDOWS: We need to add 2 bytes for each row because of "\r\n" (CR-LF)
        // FOR UNIX: We need to add 1 bytes for each row because of "\n" (LF)
        let buffer_size: usize = DEFAULT_ROW_BUFFER_LEN * rowlen + DEFAULT_ROW_BUFFER_LEN * 1;
        let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);

        let mut path: PathBuf = PathBuf::from(randomize_file_name());
        path.set_extension("flf");

        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .expect("Could not open target file!");

        info!("Writing to target file: {}", path.to_str().unwrap());
        for row in 0..n_rows {
            if row % DEFAULT_ROW_BUFFER_LEN == 0 && row != 0 {
                file.write_all(&buffer).expect("Bad buffer, write failed!");
                // Here maybe we should just use the same allocated memory?, but overwrite it..
                // Because re-allocation is slow (::with_capacity will re-allocate on heap).
                buffer.clear(); // Vec::with_capacity(buffer_size);
            }
            for col in self.schema.iter() {
                pad_and_push_to_buffer(
                    col.mock().unwrap().as_bytes().to_vec(),
                    col.length(),
                    Alignment::Right,
                    Symbol::Whitespace,
                    &mut buffer,
                );
            }
            buffer.extend_from_slice("\n".as_bytes());
        }

        // Write the remaining contents of the buffer to file.
        file.write_all(&buffer).expect("Bad buffer, write failed!");
    }

    ///
    fn generate_multithreaded(&self, n_rows: usize) {
        let thread_workload = self.distribute_thread_workload(n_rows);
        threaded_mock(
            n_rows,
            thread_workload,
            self.schema.to_owned(),
            self.n_threads,
            self.target_file.clone(),
        );
    }

    /// Calculate how many rows each thread should work on generating.
    fn distribute_thread_workload(&self, n_rows: usize) -> Vec<usize> {
        let n_rows_per_thread = n_rows / self.n_threads;
        (0..self.n_threads)
            .map(|_| n_rows_per_thread)
            .collect::<Vec<usize>>()
    }
}

/// Worker threads generate mocked data, and pass it to the master thread which writes it
/// to disk, but maybe this will become bottleneck?
///
/// pub struct Arc<T, A = Global>
/// where
///     A: Allocator,
///     T:  ?Sized,
///
/// is a "Thread-safe reference-counting pointer. Arc stands for 'Atomically Reference Counted'."
///
pub fn threaded_mock(
    n_rows: usize,
    thread_workload: Vec<usize>,
    schema: FixedSchema,
    n_threads: usize,
    target_file: Option<PathBuf>,
) {
    let (thread_handles, receiver) = spawn_workers(n_rows, thread_workload, schema, n_threads);
    spawn_master(&receiver, target_file);

    for handle in thread_handles {
        handle.join().expect("Could not join thread handle!");
    }
}

///
pub fn worker_thread_mock(
    thread: usize,
    n_rows: usize,
    schema: Arc<FixedSchema>,
    sender: channel::Sender<Vec<u8>>,
) {
    let rowlen: usize = schema.row_len();
    // We need to add 2 bytes for each row because of "\r\n"
    let buffer_size: usize = DEFAULT_ROW_BUFFER_LEN * rowlen + DEFAULT_ROW_BUFFER_LEN * 2;
    let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);

    for row in 0..n_rows {
        if row % DEFAULT_ROW_BUFFER_LEN == 0 && row != 0 {
            sender
                .send(buffer)
                .expect("Bad buffer, or something, could not send from worker thread!");
            // Here maybe we should just use the same allocated memory?, but overwrite it..
            // Because re-allocation is slow (::with_capacity will re-allocate on heap).
            buffer = Vec::with_capacity(buffer_size);
        }
        for col in schema.iter() {
            pad_and_push_to_buffer(
                col.mock().unwrap().as_bytes().to_vec(),
                col.length(),
                Alignment::Right,
                Symbol::Whitespace,
                &mut buffer,
            );
        }
        buffer.extend_from_slice("\r\n".as_bytes());
    }

    // Send the rest of the remaining buffer to the master thread.
    sender
        .send(buffer)
        .expect("Bad buffer, could not send last buffer from worker thread!");

    info!("Thread {} done!", thread);
    drop(sender);
}

///
pub fn spawn_workers(
    n_rows: usize,
    thread_workload: Vec<usize>,
    schema: FixedSchema,
    n_threads: usize,
) -> (Vec<JoinHandle<()>>, channel::Receiver<Vec<u8>>) {
    let remaining_rows = n_rows - thread_workload.iter().sum::<usize>();
    info!("Distributed thread workload: {:?}", thread_workload);
    info!("Remaining rows to handle: {}", remaining_rows);

    let arc_schema = Arc::new(schema);
    let (sender, receiver) = channel::bounded(DEFAULT_THREAD_CHANNEL_CAPACITY);

    let threads: Vec<JoinHandle<()>> = (0..n_threads)
        .map(|t| {
            let t_rows = thread_workload[t];
            let t_schema = Arc::clone(&arc_schema);
            let t_sender = sender.clone();
            thread::spawn(move || worker_thread_mock(t, t_rows, t_schema, t_sender))
        })
        .collect();

    drop(sender);
    (threads, receiver)
}

pub fn spawn_master(channel: &channel::Receiver<Vec<u8>>, target_file: Option<PathBuf>) {
    let path = match target_file {
        Some(p) => p,
        None => {
            let mut path = PathBuf::from(randomize_file_name());
            path.set_extension("flf");
            path
        }
    };

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("Could not open target file!");

    info!("Writing to target file: {}", path.to_str().unwrap());
    for buff in channel {
        file.write_all(&buff)
            .expect("Got bad buffer from thread, write failed!");
    }
}

pub fn randomize_file_name() -> String {
    let mut path_name: String = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    path_name.push('_');
    path_name.push_str(
        Alphanumeric
            .sample_string(&mut rand::thread_rng(), DEFAULT_MOCKED_FILENAME_LEN)
            .as_str(),
    );
    path_name
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
