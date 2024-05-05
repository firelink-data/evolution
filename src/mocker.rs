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
* Last updated: 2024-05-05
*/

use crossbeam::channel;
use log::{debug, error, info, warn};
use padder::*;
use parquet::data_type::AsBytes;
use rand::rngs::ThreadRng;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{thread, usize};

use crate::error::ExecutionError;
use crate::mocking::randomize_file_name;
use crate::schema::FixedSchema;
use crate::writer::{Writer, writer_from_file_extension};

/// If the user wants to only generate a few number of mocked rows,then multithreading
/// is not a suitable choice, and only introduces extra overhead. This variable specifies
/// the minimum number of rows to be generated to allow enabling multithreading.
/// This value takes priority over the CLI settings regarding number of threads etc.
pub(crate) static MIN_NUM_ROWS_FOR_MULTITHREADING: usize = 1024;

/// The number of messages that can exist in the thread channel at the same time.
/// If the channel buffer grows to this size, incoming messages will be held until
/// previous messages have been consumed. Increasing this variable will significantly
/// increase the amount of system memory allocated by the program.
pub(crate) static mut MOCKER_THREAD_CHANNEL_CAPACITY: usize = 128;

/// Sets the size of the buffer that the mocker utilizes to store rows of mocked data
/// before writing to specified location. A smaller number for this variable means that
/// the program will perform I/O operations more often, which are expensive system calls.
/// A larger number is advisable, however, the host system must be able to hold all
/// rows of mocked data in system memory before writing to its location.
pub(crate) static MOCKER_ROW_BUFFER_SIZE: usize = 1024;

#[cfg(target_os = "windows")]
pub(crate) static NUM_CHARS_FOR_NEWLINE: usize = 2;
#[cfg(not(target_os = "windows"))]
pub(crate) static NUM_CHARS_FOR_NEWLINE: usize = 1;

// Depending on the target os we need specific handling of newlines.
// https://doc.rust-lang.org/reference/conditional-compilation.html
#[cfg(target_os = "windows")]
fn newline<'a>() -> &'a str { "\r\n" }
#[cfg(not(target_os = "windows"))]
fn newline<'a>() -> &'a str { "\n" }

/// A struct for generating mocked data based on a user-defined schema.
#[derive(Debug)]
pub(crate) struct Mocker {
    schema: FixedSchema,
    n_rows: usize,
    n_threads: usize,
    multithreaded: bool,
    writer: Box<dyn Writer>,
}

/// A builder struct that simplifies the creation of a valid [`Mocker`] instance.
#[derive(Debug, Default)]
pub(crate) struct MockerBuilder {
    schema: Option<FixedSchema>,
    n_rows: Option<usize>,
    n_threads: Option<usize>,
    multithreaded: Option<bool>,
    output_file: Option<PathBuf>,
}

impl MockerBuilder {
    /// Set the [`FixedSchema`] to generate data according to with the [`Mocker`].
    pub fn schema(mut self, schema: FixedSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set the number of mocked data rows to generate with the [`Mocker`].
    pub fn num_rows(mut self, n_rows: usize) -> Self {
        self.n_rows = Some(n_rows);
        self
    }

    /// Set the number of threads to use when generating mocked data with the [`Mocker`].
    pub fn num_threads(mut self, n_threads: usize) -> Self {
        let multithreaded = n_threads > 1;
        self.n_threads = Some(n_threads);
        self.multithreaded = Some(multithreaded);
        self
    }

    /// Set the target output file name with the [`Mocker`].
    /// Note: this can be `None`, as this is a CLI option.
    pub fn output_file(mut self, output_file: Option<PathBuf>) -> Self {
        self.output_file = output_file;
        self
    }

    /// Verify that all required fields have been set and explicitly create a new [`Mocker`]
    /// instance based on the provided fields.
    ///
    /// # Error
    /// Iff any of the required fields are `None`.
    pub fn build(self) -> Result<Mocker, ExecutionError> {

        let schema = match self.schema {
            Some(s) => s,
            None => {
                error!("Required field `schema` not provided, exiting...");
                return Err(ExecutionError);
            },
        };

        let n_rows = match self.n_rows {
            Some(n) => n,
            None => {
                error!("Required field `n_rows` not provided, exiting...");
                return Err(ExecutionError);
            }
        };

        let n_threads = match self.n_threads {
            Some(n) => n,
            None => {
                error!("Required field `n_threads` not provided, exiting...");
                return Err(ExecutionError);
            },
        };

        let multithreaded = match self.multithreaded {
            Some(m) => m,
            None => {
                error!("Required field `multithreaded` not provided, exiting...");
                return Err(ExecutionError);
            },
        };

        let output_file = match self.output_file {
            Some(o) => o,
            None => {
                info!("Optional field `output_file` not provided, randomizing a file name.");
                let mut path: PathBuf = PathBuf::from(randomize_file_name());
                path.set_extension("flf");
                info!("Output file name is: {}", path.to_str().unwrap());
                path
            }
        };

        let writer: Box<dyn Writer> = writer_from_file_extension(output_file);

        Ok(Mocker {
            schema,
            n_threads,
            n_rows,
            multithreaded,
            writer,
        })
    }
}

///
impl Mocker {

    /// Create a new instance of a [`MockerBuilder`] struct with default values.
    pub fn builder() -> MockerBuilder {
        MockerBuilder { ..Default::default() }
    }

    /// Start generating mocked data rows based on the provided struct fields.
    /// Checks if the user wants to use multithreading and how many rows they
    /// intend to generate. If the number of desired rows to generate is less 
    /// than [`MIN_NUM_ROWS_FOR_MULTITHREADING`] then the program will run in
    /// single thread mode instead. Employing multithreading when generating
    /// a few number of rows introduces much more computational overhead than
    /// necessary, so it is much more efficient to run in a single thread.
    pub fn generate(&mut self) {
        if self.n_rows >= MIN_NUM_ROWS_FOR_MULTITHREADING  && self.multithreaded {
            self.generate_multithreaded();
        } else {
            if self.multithreaded {
                warn!(
                    "You specified to use {} threads, but you only want to mock {} rows.",
                    self.n_threads,
                    self.n_rows,
                );
                warn!("This is done more efficiently single-threaded, ignoring multithreading!");
            }
            self.generate_single_thread();
        }
    }

    fn generate_multithreaded(&self) {
        todo!();
    }

    fn generate_single_thread(&mut self) {
        let row_len = self.schema.row_len();
        let buffer_size: usize = 
            MOCKER_ROW_BUFFER_SIZE * row_len
            + MOCKER_ROW_BUFFER_SIZE * NUM_CHARS_FOR_NEWLINE;

        let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);
        let mut rng: ThreadRng = rand::thread_rng();

        for row_idx in 0..self.n_rows {
            if (row_idx % MOCKER_ROW_BUFFER_SIZE == 0) && (row_idx != 0) {
                self.writer.write(&buffer);
                buffer.clear();
            }

            for column in self.schema.iter() {
                pad_and_push_to_buffer(
                    column.mock(&mut rng).as_bytes(),
                    column.length(),
                    Alignment::Right,
                    Symbol::Whitespace,
                    &mut buffer,
                );
            }

            buffer.extend_from_slice(newline().as_bytes());
        }

        // Write any remaining contents of the buffer to file.
        self.writer.write(&buffer);
    }

    /*
    ///
    pub fn generate(&self) {
        if self.multithreaded && self.n_rows > DEFAULT_MIN_N_ROWS_FOR_MULTITHREADING {
            self.generate_multithreaded(self.n_rows);
        } else {
            if self.multithreaded {
                warn!(
                    "You specified to use multithreading but only want to mock {} rows.",
                    self.n_rows
                );
                warn!("This is done more efficiently single-threaded, ignoring multithreading.");
            }
            self.generate_single_threaded(self.n_rows);
        }
    }
    */
}

/*
    ///
    fn generate_multithreaded(&self, n_rows: usize) {
        let thread_workload = self.distribute_thread_workload(n_rows);
        threaded_mock(
            n_rows,
            thread_workload,
            self.schema.to_owned(),
            self.n_threads,
            self.output_file.clone(),
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
    output_file: PathBuf,
) {
    let (thread_handles, receiver) = spawn_workers(n_rows, thread_workload, schema, n_threads);
    spawn_master(&receiver, output_file);

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
    let mut rng: ThreadRng = rand::thread_rng();

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
                col.mock(&mut rng).unwrap().as_bytes().to_vec(),
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

pub fn spawn_master(channel: &channel::Receiver<Vec<u8>>, output_file: PathBuf) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output_file)
        .expect("Could not open output file!");

    info!("Writing to output file: {}", output_file.to_str().unwrap());
    for buff in channel {
        file.write_all(&buff)
            .expect("Got bad buffer from thread, write failed!");
    }
}
*/
