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
// Last updated: 2024-05-28
//

use evolution_common::error::{Result, SetupError};
use evolution_common::{newline, NUM_BYTES_FOR_NEWLINE};
use evolution_schema::schema::FixedSchema;
use evolution_writer::writer::{FixedLengthFileWriter, FixedLengthFileWriterProperties, Writer};
use log::{info, warn};
use padder::pad_and_push_to_buffer;
use rand::rngs::ThreadRng;

use std::path::PathBuf;

use crate::mock_column;

/// If the user only wants to generate a small amount of mocked .flf rows then multithreading
/// is not a stuiable choice and probably only introduces extra overhead. This variable
/// specifies the minimum number of rows to be mocked to allow enabling multithreading.
///
/// # Note
/// This value takes priority over any CLI options regarding number of threads to use.
pub static MIN_NUM_ROWS_FOR_MULTITHREADING: usize = 100000;

pub trait Mocker {}
pub type MockerRef = Box<dyn Mocker>;

/// The mocker struct for fixed-length files (.flf).
pub struct FixedLengthFileMocker {
    /// The schema to mock data based on.
    schema: FixedSchema,
    /// The writer to use when writing the mocked data to file.
    writer: FixedLengthFileWriter,
    /// The number of mocked rows to generate.
    n_rows: usize,
    /// The number of threads (logical cores) to use.
    n_threads: usize,
    /// The size of the writer buffer (in number of rows).
    write_buffer_size: usize,
}

impl FixedLengthFileMocker {
    /// Create a new instance of a [`FixedLengthFileMockerBuilder`] with default values.
    pub fn builder() -> FixedLengthFileMockerBuilder {
        FixedLengthFileMockerBuilder {
            ..Default::default()
        }
    }

    /// Generate mocked data based on the provided [`FixedSchema`]. This function will
    /// either run in single-threaded mode or in multithreaded mode depending on:
    /// * the number of available threads (logical cores) on the host system,
    /// * and the number of mocked rows to generate.
    pub fn try_mock(&mut self) -> Result<()> {
        if self.n_threads > 1 {
            self.try_mock_multithreaded()?;
        } else {
            self.try_mock_single_threaded()?;
        }
        Ok(())
    }

    ///
    fn try_mock_multithreaded(&mut self) -> Result<()> {
        todo!()
    }

    ///
    fn try_mock_single_threaded(&mut self) -> Result<()> {
        let n_runes_in_row = self.schema.row_length();
        // Here we multiply by 4 because a valid UTF-8 encoded character can at most be
        // exactly 4 bytes. Thus, we will always allocate enough memory for the writer buffer.
        // https://en.wikipedia.org/wiki/UTF-8
        let writer_buffer_size: usize = 4 * self.write_buffer_size * NUM_BYTES_FOR_NEWLINE
            + self.write_buffer_size * n_runes_in_row;

        let mut buffer: Vec<u8> = Vec::with_capacity(writer_buffer_size);
        let mut rng: ThreadRng = rand::thread_rng();

        info!("Mocking {} rows in single-threaded mode.", self.n_rows);

        for ridx in 0..self.n_rows {
            if (ridx % self.write_buffer_size == 0) && (ridx != 0) {
                self.writer.try_write(&buffer)?;
                buffer.clear();
            }

            for column in self.schema.iter() {
                pad_and_push_to_buffer(
                    mock_column(column, &mut rng).as_bytes(),
                    column.length(),
                    column.alignment(),
                    column.pad_symbol(),
                    &mut buffer,
                );
            }

            buffer.extend_from_slice(newline().as_bytes());
        }


        info!("Done mocking, flushing any remaining buffers.");
        self.writer.try_write(&buffer)?;
        self.writer.try_finish()?;

        Ok(())
    }
}

impl Mocker for FixedLengthFileMocker {}

/// A helper struct for building an instance of a [`FixedLengthFileMocker`] struct.
#[derive(Default)]
pub struct FixedLengthFileMockerBuilder {
    schema_path: Option<PathBuf>,
    out_path: Option<PathBuf>,
    n_rows: Option<usize>,
    n_threads: Option<usize>,
    write_buffer_size: Option<usize>,

    // File descriptor properties.
    force_create_new: Option<bool>,
    truncate_existing: Option<bool>,
}

impl FixedLengthFileMockerBuilder {
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

    /// Set the number of rows to generate.
    pub fn with_num_rows(mut self, n_rows: usize) -> Self {
        self.n_rows = Some(n_rows);
        self
    }

    /// Set the number of threads (logical cores) to use.
    pub fn with_num_threads(mut self, n_thread: usize) -> Self {
        self.n_threads = Some(n_thread);
        self
    }

    /// Set the buffer size for writing to file (in number of rows).
    pub fn with_write_buffer_size(mut self, buffer_size: usize) -> Self {
        self.write_buffer_size = Some(buffer_size);
        self
    }

    /// Set the writer option to return an error if the file already exists.
    pub fn with_force_create_new(mut self, force_create_new: bool) -> Self {
        self.force_create_new = Some(force_create_new);
        self
    }

    /// Set the writer option to truncate the output file if it already exists.
    pub fn with_truncate_existing(mut self, truncate_existing: bool) -> Self {
        self.truncate_existing = Some(truncate_existing);
        self
    }

    /// Try creating a new [`FixedLengthFileMocker`] from the previously set values.
    ///
    /// # Errors
    /// If any of the required fields are `None`, or if the schema deserialization failed.
    pub fn try_build(self) -> Result<FixedLengthFileMocker> {
        let schema: FixedSchema = match self.schema_path {
            Some(p) => FixedSchema::from_path(p)?,
            None => {
                return Err(Box::new(SetupError::new(
                    "Required field 'schema_path' was not provided, exiting...",
                )))
            }
        };

        let out_path: PathBuf = match self.out_path {
            Some(p) => p,
            None => {
                return Err(Box::new(SetupError::new(
                    "Required field 'out_path' was not provided, exiting...",
                )))
            }
        };

        let n_rows: usize = self.n_rows.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'n_rows' was not provided, exiting...",
            ))
        })?;

        let mut n_threads: usize = self.n_threads.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'n_threads' was not provided, exiting...",
            ))
        })?;

        let write_buffer_size: usize = self.write_buffer_size.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'write_buffer_size' was not provided, exiting...",
            ))
        })?;

        let force_create_new: bool = self.force_create_new.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'force_create_new' was not provided, exiting...",
            ))
        })?;

        let truncate_existing: bool = self.truncate_existing.ok_or_else(|| {
            Box::new(SetupError::new(
                "Required field 'truncate_existing' was not provided, exiting...",
            ))
        })?;

        let writer_properties: FixedLengthFileWriterProperties =
            FixedLengthFileWriterProperties::builder()
                .with_force_create_new(force_create_new)
                .with_create_or_open(true)
                .with_truncate_existing(truncate_existing)
                .try_build()?;

        let writer: FixedLengthFileWriter = FixedLengthFileWriter::builder()
            .with_out_path(out_path)
            .with_properties(writer_properties)
            .try_build()?;

        let multithreading: bool = (n_rows >= MIN_NUM_ROWS_FOR_MULTITHREADING) && (n_threads > 1);

        if !multithreading && n_threads > 1 {
            warn!(
                "You specified to use {} threads but only want to mock {} rows.",
                n_threads, n_rows
            );
            warn!(
                "This is done much more efficiently single-threaded, ignoring any multithreading!"
            );
            n_threads = 1;
        }

        Ok(FixedLengthFileMocker {
            schema,
            writer,
            n_rows,
            n_threads,
            write_buffer_size,
        })
    }

    /// Creates a new [`FixedLengthFileMocker`] from the previously set values.
    ///
    /// # Note
    /// This method internally calls the [`try_build`] method and simply unwraps the returned
    /// [`Result`]. If you don't care about error propagation, use this method over [`try_build`].
    ///
    /// # Panics
    /// If any of the required fields are `None`, or if the schema deserialization failed.
    ///
    /// [`try_build`]: FixedLengthFileMockerBuilder::try_build
    pub fn build(self) -> FixedLengthFileMocker {
        self.try_build().unwrap()
    }
}
