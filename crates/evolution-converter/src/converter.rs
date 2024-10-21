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
// File created: 2024-02-17
// Last updated: 2024-10-19
//

use evolution_builder::builder::Builder;
use evolution_common::error::Result;
use evolution_schema::schema::Schema;
use evolution_slicer::slicer::Slicer;
use evolution_target::target::Target;
use evolution_writer::writer::Writer;

/// A trait providing functions to convert source data to target.
pub trait Converter {
    fn convert(&mut self);
    fn try_convert(&mut self) -> Result<()>;
}
/// A short-hand notation for a generic [`Converter`] reference.
pub type ConverterRef = Box<dyn Converter>;

pub struct FileConverter<S, B, W, C>
where
    S: Slicer,
    B: Builder,
    W: Writer,
{
    parser: S,
    builder: B,
    writer: W,
    schema: C,
    properties: ConverterProperties,
}

impl FileConverter<_> {
    pub fn new(target: Target) -> Self {
        match target {
            Target::Parquet => {
                FileConverter {
                    slicer: FixedWidthSlicer::new(),
                    builder: ParquetBuilder,
                    writer: ParquetWriter,
                }
            },
            Target::Iceberg => todo!(),
            _ => todo!(),
        }
    }
}

/// Properties for conversion.
pub struct ConverterProperties {
    read_buffer_size: usize,
    thread_channel_capacity: usize,
    multithreaded: bool,
    n_worker_threads: usize,
}

impl ConverterProperties {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Set the buffer size for reading the input (in bytes).
    pub fn with_read_buffer_size(mut self, read_buffer_size: usize) -> Self {
        self.read_buffer_size = read_buffer_size;
        self
    }

    /// Set the maxmimum message capacity on the multithreaded converter channels.
    pub fn with_thread_channel_capacity(mut self, thread_channel_capacity: usize) -> Self {
        self.thread_channel_capacity = thread_channel_capacity;
        self
    }

    /// Set the number of worker threads (logical cores) to use.
    pub fn with_n_worker_threads(mut self, n_worker_threads: usize) -> Self {
        self.n_worker_threads = n_worker_threads;
        if n_worker_threads > 1 {
            self.multithreaded = true;
        }
        self
    }

    /// Get the buffer size for reading the input (in bytes).
    pub fn read_buffer_size(&self) -> usize {
        self.read_buffer_size
    }

    /// Get the maxmimum message capacity on the multithreaded converter channels.
    pub fn thread_channel_capacity(&self) -> usize {
        self.thread_channel_capacity
    }

    /// Get whether the converter is multithreaded.
    pub fn multithreaded(&self) -> bool {
        self.multithreaded
    }

    /// Get the number of worker threads (logical cores) to use.
    pub fn n_worker_threads(&self) -> usize {
        self.n_worker_threads
    }
}

impl Default for ConverterProperties {
    fn default() -> Self {
        let n_worker_threads: usize = num_cpus::get() - 1;
        Self {
            read_buffer_size: 1 * 1024 * 1024 * 1024, // 1 GB
            thread_channel_capacity: n_worker_threads,
            multithreaded: true,
            n_worker_threads,
        }
    }
}