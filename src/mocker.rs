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
// File created: 2024-02-05
// Last updated: 2024-05-15
//

use crossbeam::channel;
#[cfg(debug_assertions)]
use log::debug;
use log::{error, info, warn};
use padder::*;
use rand::rngs::ThreadRng;
#[cfg(feature = "rayon")]
use rayon::iter::*;

use std::path::PathBuf;
use std::sync::Arc;
#[cfg(not(feature = "rayon"))]
use std::thread::{spawn, JoinHandle};

use crate::error::{Result, SetupError};
use crate::mocking::randomize_file_name;
use crate::schema::FixedSchema;
use crate::writer::{FixedLengthFileWriter, FixedLengthFileWriterProperties, Writer};

/// If the user wants to only generate a few number of mocked rows,then multithreading
/// is not a suitable choice, and only introduces extra overhead. This variable specifies
/// the minimum number of rows to be generated to allow enabling multithreading.
/// This value takes priority over the CLI settings regarding number of threads etc.
pub(crate) static MIN_NUM_ROWS_FOR_MULTITHREADING: usize = 1024;

/// The number of messages that can exist in the thread channel at the same time.
/// If the channel buffer grows to this size, incoming messages will be held until
/// previous messages have been consumed. Increasing this variable will significantly
/// increase the amount of system memory allocated by the program.
pub(crate) static MOCKER_THREAD_CHANNEL_CAPACITY: usize = 128;

/// Sets the size of the buffer that the mocker utilizes to store rows of mocked data
/// before writing to specified location. A smaller number for this variable means that
/// the program will perform I/O operations more often, which are expensive system calls.
/// A larger number is advisable, however, the host system must be able to hold all
/// rows of mocked data in system memory before writing to its location.
pub(crate) static MOCKER_BUFFER_NUM_ROWS: usize = 256 * 1024;

#[cfg(target_os = "windows")]
pub(crate) static NUM_CHARS_FOR_NEWLINE: usize = 2;
#[cfg(not(target_os = "windows"))]
pub(crate) static NUM_CHARS_FOR_NEWLINE: usize = 1;

// Depending on the target os we need specific handling of newlines.
// https://doc.rust-lang.org/reference/conditional-compilation.html
#[cfg(target_os = "windows")]
fn newline<'a>() -> &'a str {
    "\r\n"
}
#[cfg(not(target_os = "windows"))]
fn newline<'a>() -> &'a str {
    "\n"
}

/// A struct for generating mocked data based on a user-defined schema.
#[derive(Debug)]
pub(crate) struct Mocker {
    schema: FixedSchema,
    n_rows: usize,
    writer: Box<dyn Writer>,
    n_threads: usize,
    multithreaded: bool,
    buffer_size: usize,
    thread_channel_capacity: usize,
}

unsafe impl Send for Mocker {}
unsafe impl Sync for Mocker {}

impl Mocker {
    /// Create a new instance of a [`MockerBuilder`] struct with default values.
    pub fn builder() -> MockerBuilder {
        MockerBuilder {
            ..Default::default()
        }
    }

    /// Start generating mocked data rows based on the provided struct fields.
    /// Checks if the user wants to use multithreading and how many rows they
    /// intend to generate. If the number of desired rows to generate is less
    /// than [`MIN_NUM_ROWS_FOR_MULTITHREADING`] then the program will run in
    /// single thread mode instead. Employing multithreading when generating
    /// a few number of rows introduces much more computational overhead than
    /// necessary, so it is much more efficient to run in a single thread.
    pub fn generate(&mut self) -> Result<()> {
        if self.n_rows >= MIN_NUM_ROWS_FOR_MULTITHREADING && self.multithreaded {
            self.generate_multithreaded()?;
        } else {
            if self.multithreaded {
                warn!(
                    "You specified to use {} threads, but you only want to mock {} rows.",
                    self.n_threads, self.n_rows,
                );
                warn!("This is done more efficiently single-threaded, ignoring multithreading!");
            }
            self.generate_single_threaded()?;
        }
        Ok(())
    }

    /// Given n threads, the mocker will use n - 1 for generating data,
    /// and 1 thread for writing data to disk.
    fn distribute_thread_workload(&self) -> Vec<usize> {
        let n_rows_per_thread = self.n_rows / (self.n_threads - 1);
        (0..(self.n_threads - 2))
            .map(|_| n_rows_per_thread)
            .collect::<Vec<usize>>()
    }

    /// Generate mocked data in multithreading mode using [`rayon`] and parallel iteration.
    #[cfg(feature = "rayon")]
    fn generate_multithreaded(&mut self) -> Result<()> {
        // Calculate the workload for each worker thread, if the workload can not evenly
        // be split among the threads, then the last thread will have to take the remainder.
        let mut thread_workloads: Vec<usize> = self.distribute_thread_workload();
        let remainder: usize = self.n_rows - thread_workloads.iter().sum::<usize>();
        thread_workloads.push(remainder);
        let (sender, receiver) = channel::bounded(self.thread_channel_capacity);

        info!("Starting {} worker threads.", thread_workloads.len(),);

        let schema = Arc::new(self.schema.clone());
        thread_workloads
            .into_par_iter()
            .enumerate()
            .for_each_with(&sender, |s, (t, workload)| {
                let t_schema = Arc::clone(&schema);
                worker_thread_generate(s.clone(), t, workload, t_schema, self.buffer_size)
            });

        drop(sender);
        master_thread_write(receiver, &mut self.writer)?;
        Ok(())
    }

    /// Generate mocked data in multithreading mode using the standard library threads.
    #[cfg(not(feature = "rayon"))]
    fn generate_multithreaded(&mut self) -> Result<()> {
        // Calculate the workload for each worker thread, if the workload can not evenly
        // be split among the threads, then the last thread will have to take the remainder.
        let mut thread_workloads: Vec<usize> = self.distribute_thread_workload();
        let remainder: usize = self.n_rows - thread_workloads.iter().sum::<usize>();
        thread_workloads.push(remainder);
        let (sender, receiver) = channel::bounded(self.thread_channel_capacity);

        info!("Starting {} worker threads.", thread_workloads.len(),);

        let schema = Arc::new(self.schema.clone());
        let threads = thread_workloads
            .into_iter()
            .enumerate()
            .map(|(t, t_workload)| {
                let t_schema = Arc::clone(&schema);
                let t_sender = sender.clone();
                let t_buffer_size = self.buffer_size;
                spawn(move || {
                    worker_thread_generate(t_sender, t, t_workload, t_schema, t_buffer_size)
                })
            })
            .collect::<Vec<JoinHandle<()>>>();

        drop(sender);
        master_thread_write(receiver, &mut self.writer)?;

        for handle in threads {
            handle.join().expect("Could not join worker thread handle!");
        }

        Ok(())
    }

    // Generated mocked data in a single-threaded mode and write to disk.
    // This will never induce any severe bottleneck due to heavy I/O like
    // the multithreading mode can do. However, single-threaded mode will
    // be significantly slower when generating the sweet-spot of rows
    // which do not introduce long thread waiting times due to I/O.
    fn generate_single_threaded(&mut self) -> Result<()> {
        let row_len = self.schema.row_len();
        let buffer_size: usize =
            self.buffer_size * row_len + self.buffer_size * NUM_CHARS_FOR_NEWLINE;

        let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);
        let mut rng: ThreadRng = rand::thread_rng();

        for row_idx in 0..self.n_rows {
            if (row_idx % self.buffer_size == 0) && (row_idx != 0) {
                self.writer.write(&buffer)?;
                buffer.clear();
            }

            for column in self.schema.iter() {
                pad_and_push_to_buffer(
                    column.mock(&mut rng).as_bytes(),
                    column.length(),
                    column.alignment(),
                    column.pad_symbol(),
                    &mut buffer,
                );
            }

            buffer.extend_from_slice(newline().as_bytes());
        }

        // Write any remaining contents of the buffer to disk.
        self.writer.write(&buffer)?;
        self.writer.finish()?;

        Ok(())
    }
}

/// A builder struct that simplifies the creation of a valid [`Mocker`] instance.
#[derive(Debug, Default)]
pub(crate) struct MockerBuilder {
    schema_file: Option<PathBuf>,
    n_rows: Option<usize>,
    out_file: Option<PathBuf>,
    create_new: Option<bool>,
    create: Option<bool>,
    truncate: Option<bool>,
    n_threads: Option<usize>,
    multithreaded: Option<bool>,
    buffer_size: Option<usize>,
    thread_channel_capacity: Option<usize>,
}

impl MockerBuilder {
    /// Set the [`PathBuf`] for the schema to use when generating data with the [`Mocker`].
    pub fn with_schema(mut self, schema_file: PathBuf) -> Self {
        self.schema_file = Some(schema_file);
        self
    }

    /// Set the target output file name with the [`Mocker`].
    pub fn with_out_file(mut self, out_file: Option<PathBuf>) -> Self {
        self.out_file = out_file;
        self
    }

    /// Set the number of mocked data rows to generate with the [`Mocker`].
    pub fn with_num_rows(mut self, n_rows: Option<usize>) -> Self {
        self.n_rows = n_rows;
        self
    }

    pub fn with_create_new(mut self, create_new: bool) -> Self {
        self.create_new = Some(create_new);
        self
    }

    pub fn with_create(mut self, create: bool) -> Self {
        self.create = Some(create);
        self
    }

    pub fn with_truncate(mut self, truncate: bool) -> Self {
        self.truncate = Some(truncate);
        self
    }

    /// Set the number of threads to use when generating mocked data with the [`Mocker`].
    pub fn with_num_threads(mut self, n_threads: usize) -> Self {
        self.n_threads = Some(n_threads);
        self.multithreaded = Some(n_threads > 1);
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: Option<usize>) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn with_thread_channel_capacity(mut self, thread_channel_capacity: Option<usize>) -> Self {
        self.thread_channel_capacity = thread_channel_capacity;
        self
    }

    /// Verify that all required fields have been set and explicitly create a new [`Mocker`]
    /// instance based on the provided fields. Any optional fields are set according to
    /// either random strategies (like output file name) or global static variables.
    ///
    /// # Error
    /// Iff any of the required fields are `None`.
    ///
    /// # Panics
    /// Iff the schema file is invalid and can not be deserialized properly.
    pub fn build(self) -> Result<Mocker> {
        let schema: FixedSchema = match self.schema_file {
            Some(p) => FixedSchema::from_path(p)?,
            None => {
                error!("Required field `schema_path` not provided, exiting...");
                return Err(Box::new(SetupError));
            }
        };

        let n_rows: usize = self
            .n_rows
            .ok_or("Required field 'n_rows' is missing or None.")?;

        let create_new: bool = self
            .create_new
            .ok_or("Required field 'create_new' is missing or None.")?;

        let create: bool = self
            .create
            .ok_or("Required field 'create' is missing or None.")?;

        let truncate: bool = self
            .truncate
            .ok_or("Required field 'truncate' is missing or None.")?;

        let n_threads: usize = self
            .n_threads
            .ok_or("Requried field 'n_threads' is missing or None.")?;

        let multithreaded: bool = self
            .multithreaded
            .ok_or("Required field 'multithreaded' is missing or None.")?;

        //
        // Optional configuration below.
        //
        let out_file: PathBuf = match self.out_file {
            Some(o) => o,
            None => {
                info!("Optional field '--output-file' not provided, randomizing a file name.");
                let mut path: PathBuf = PathBuf::from(randomize_file_name());
                path.set_extension("flf");
                info!("Output file name is now '{}'.", path.to_str().unwrap());
                path
            }
        };

        // Here we divide by the number of threads because each thread will allocated
        // `MOCKER_BUFFER_NUM_ROWS` amount of memory, meaning, if we are not careful
        // with the size of this variable then memory allocation might go through the
        // roof and cause a program crash due to memory overflow and mem-swapping.
        let buffer_size: usize = match self.buffer_size {
            Some(s) => {
                if n_rows >= MIN_NUM_ROWS_FOR_MULTITHREADING && multithreaded {
                    s / (n_threads - 1)
                } else {
                    s
                }
            }
            None => {
                if n_rows >= MIN_NUM_ROWS_FOR_MULTITHREADING && multithreaded {
                    let mocker_buffer_size = MOCKER_BUFFER_NUM_ROWS / (n_threads - 1);
                    #[cfg(debug_assertions)]
                    debug!("Optional field '--buffer-size' not provided, mocker buffer size is now {} rows.", mocker_buffer_size);
                    mocker_buffer_size
                } else {
                    #[cfg(debug_assertions)]
                    debug!("Optional field '--buffer-size' not provided, mocker buffer size is now {} rows.", MOCKER_BUFFER_NUM_ROWS);
                    MOCKER_BUFFER_NUM_ROWS
                }
            }
        };

        let thread_channel_capacity: usize = match self.thread_channel_capacity {
            Some(c) => c,
            None => {
                #[cfg(debug_assertions)]
                debug!(
                    "Optional field '--thread-channel-capacity' not provided, mocker thread channel capacity is now {} messages.",
                    MOCKER_THREAD_CHANNEL_CAPACITY
                );
                MOCKER_THREAD_CHANNEL_CAPACITY
            }
        };

        let properties: FixedLengthFileWriterProperties =
            FixedLengthFileWriterProperties::builder()
                .with_create_new(create_new)
                .with_create(create)
                .with_truncate(truncate)
                .build()?;

        let writer: Box<dyn Writer> = Box::new(
            FixedLengthFileWriter::builder()
                .with_out_file(out_file)
                .with_properties(properties)
                .build()?,
        );

        Ok(Mocker {
            schema,
            n_rows,
            writer,
            n_threads,
            multithreaded,
            buffer_size,
            thread_channel_capacity,
        })
    }
}

fn worker_thread_generate(
    channel: channel::Sender<Vec<u8>>,
    thread: usize,
    n_rows: usize,
    schema: Arc<FixedSchema>,
    n_rows_buffer_size: usize,
) {
    let row_len: usize = schema.row_len();
    let buffer_size: usize =
        n_rows_buffer_size * row_len + n_rows_buffer_size * NUM_CHARS_FOR_NEWLINE;

    let mut rng: ThreadRng = rand::thread_rng();
    let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);

    #[cfg(debug_assertions)]
    debug!("Worker thread {} starting.", thread);

    for row_idx in 0..n_rows {
        if (row_idx % buffer_size == 0) && (row_idx != 0) {
            channel
                .send(buffer)
                .expect("Could not send buffer from worker to master thread!");

            buffer = Vec::with_capacity(buffer_size);
        }

        for column in schema.iter() {
            pad_and_push_to_buffer(
                column.mock(&mut rng).as_bytes(),
                column.length(),
                column.alignment(),
                column.pad_symbol(),
                &mut buffer,
            );
        }

        buffer.extend_from_slice(newline().as_bytes());
    }

    channel
        .send(buffer)
        .expect("Could not send buffer from worker thread to master thread!");

    info!("Worker thread {} done!", thread);
    drop(channel);
}

fn master_thread_write(
    channel: channel::Receiver<Vec<u8>>,
    writer: &mut Box<dyn Writer>,
) -> Result<()> {
    // Write buffer contents to disk.
    for buffer in channel {
        writer.write(&buffer)?;
        drop(buffer);
    }

    info!("Master thread done writing mocked data!");
    Ok(())
}
