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
// File created: 2024-10-18
// Last updated: 2024-10-19
//

use evolution_builder::csv::CsvBuilder;
use evolution_common::error::{Result, SetupError};
use evolution_common::thread::estimate_best_thread_channel_capacity;
use evolution_slicer::slicer::FileSlicer;
use evolution_schema::schema::FixedSchema;
use evolution_writer::csv::{CsvWriter, CsvWriterBuilder};

use std::path::PathBuf;

pub struct CsvConverter {
    slicer: FileSlicer,
    writer: CsvWriter,
    schema: FixedSchema,
    read_buffer_size: usize,
    n_threads: usize,
    thread_channel_capacity: usize,
}

impl CsvConverter {
    /// Create a new instance of a [`CsvConverter`] with default values.
    pub fn builder() -> CsvConverterBuilder {
        CsvConverterBuilder {
            ..Default::default()
        }
    }
}

#[derive(Default)]
pub struct CsvConverterBuilder {
    in_path: Option<PathBuf>,
    schema_path: Option<PathBuf>,
    out_path: Option<PathBuf>,
    n_threads: Option<usize>,
    read_buffer_size: Option<usize>,
    thread_channel_capacity: Option<usize>,
}

impl CsvConverterBuilder {
    /// Set the relative or absolute path to the input file to convert.
    pub fn with_in_file(mut self, in_path: PathBuf) -> Self {
        self.in_path = Some(in_path);
        self
    }

    /// Set the relative or absolute path to the json schema file to use.
    pub fn with_schema(mut self, schema_path: PathBuf) -> Self {
        self.schema_path = Some(schema_path);
        self
    }

    /// Set the relative or absolute path to the output file to produce.
    pub fn with_out_file(mut self, out_path: PathBuf) -> Self {
        self.out_path = Some(out_path);
        self
    }

    /// Set the number of threads (logical cores) to use.
    pub fn with_num_threads(mut self, n_threads: usize) -> Self {
        self.n_threads = Some(n_threads);
        self
    }

    /// Set the buffer size for reading the input file (in bytes).
    pub fn with_read_buffer_size(mut self, buffer_size: usize) -> Self {
        self.read_buffer_size = Some(buffer_size);
        self
    }

    /// Set the maximum message capacity on the multithreaded converter thread channels.
    /// See https://docs.rs/crossbeam/latest/crossbeam/channel/fn.bounded.html for specifics.
    pub fn with_thread_channel_capacity(mut self, capacity: Option<usize>) -> Self {
        self.thread_channel_capacity = capacity;
        self
    }

    /// Set default values for the optional configuration fields.
    ///
    /// # Note
    /// Default conversion mode is always multithreaded utilizing all available threads (logical cores).
    pub fn with_default_values(mut self) -> Self {
        let n_threads: usize = num_cpus::get();
        self.n_threads = Some(n_threads);
        self.read_buffer_size = Some(500 * 1024 * 1024); // 500 MB
        self.thread_channel_capacity = Some(n_threads);
        self
    }

    /// Try creating a new [`CsvConverterBuilder`] from the previously set values.
    ///
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * If any of the required fields are `None`.
    /// * If the schema deserialization failed.
    /// * If any I/O error occured when trying to open the input and output files.
    pub fn try_build(self) -> Result<CsvConverter> {
        let in_file: PathBuf = self.in_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'in_path' was not provided, exiting...",
            ))
        })?;

        let schema_path: PathBuf = self.schema_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'schema_path' was not provided, exiting...",
            ))
        })?;

        let out_path: PathBuf = self.out_path.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'out_path' was not provided, exiting...",
            ))
        })?;

        let n_threads: usize = self.n_threads.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'n_threads' was not provided, exiting...",
            ))
        })?;

        let read_buffer_size: usize = self.read_buffer_size.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'read_buffer_size' was not provided, exiting...",
            ))
        })?;

        let thread_channel_capacity: usize = self
            .thread_channel_capacity
            .unwrap_or(estimate_best_thread_channel_capacity(n_threads));

        let slicer: FileSlicer = FileSlicer::try_from_path(in_file)?;
        let schema: FixedSchema = FixedSchema::from_path(schema_path)?;

        let builder: CsvBuilder = schema.clone().into_builder::<CsvBuilder>();
        let writer: CsvWriter = CsvWriter::builder()
            .with_out_file(out_path)
            .with_properties(builder.properties)
            .try_build()?;
    }
}