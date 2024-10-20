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
// File created: 2024-10-19
// Last updated: 2024-10-19
//

use evolution_common::error::{ExecutionError, Result};
use evolution_common::NUM_BYTES_FOR_NEWLINE;
use log::warn;

use std::fs::{File, OpenOptions};
use std::io::{BufReader, ErrorKind, Read};
use std::path::PathBuf;

use crate::slicer::Slicer;

///
pub struct FixedWidthSlicer {
    inner: BufReader<File>,
    bytes_to_read: usize,
    remaining_bytes: usize,
    bytes_processed: usize,
    bytes_overlapped: usize,
}

impl FixedWidthSlicer {
    /// Try creating a new [`FixedWidthSlicer`] from a relative or absolute path
    /// to the fixed-length file that is to be sliced.
    ///
    /// # Errors
    /// This function can return an error for the following reasons:
    /// * Any I/O error was returned when trying to open the path as a file.
    /// * Could not read the metadata of the file at the path.
    pub fn try_from_path(in_path: PathBuf) -> Result<Self> {
        let file: File = OpenOptions::new().read(true).open(in_path)?;

        let bytes_to_read: usize = file.metadata()?.len() as usize;
        let remaining_bytes: usize = bytes_to_read;
        let bytes_processed: usize = 0;
        let bytes_overlapped: usize = 0;

        let inner: BufReader<File> = BufReader::new(file);

        Ok(FixedWidthSlicer {
            inner,
            bytes_to_read,
            remaining_bytes,
            bytes_processed,
            bytes_overlapped,
        })
    }

    /// Create a new [`FixedLengthFileSlicer`] from a relative or absolute path to
    /// the fixed-length file that is to be sliced.
    ///
    /// # Panics
    /// This function can panic for the following reasons:
    /// * Any I/O error was returned when trying to open the path as a file.
    /// * Could not read the metadata of the file at the path.
    pub fn from_path(in_path: PathBuf) -> Self {
        FixedWidthSlicer::try_from_path(in_path).unwrap()
    }

    /// Try and find the last linebreak character in a byte slice and return the index
    /// of the character. The function looks specifically for two character, the
    /// carriage-return (CR) and line-feed (LF) characters, represented as the character
    /// sequence '\r\n' on Windows systems.
    ///
    /// # Errors
    /// If either the byte slice to search through was empty, or there existed no linebreak
    /// character in the byte slice.
    #[cfg(target_os = "windows")]
    pub fn try_find_last_line_break(&self, bytes: &[u8]) -> Result<usize> {
        if bytes.is_empty() {
            return Err(Box::new(ExecutionError::new(
                "Byte slice to find newlines in was empty, exiting...",
            )));
        };

        let mut idx: usize = bytes.len() - 1;

        while idx > 1 {
            if (bytes[idx - 1] == 0x0d) && (bytes[idx] == 0x0a) {
                return Ok(idx - 1);
            };

            idx -= 1;
        }

        Err(Box::new(ExecutionError::new(
            "Could not find any newlines in byte slice, exiting...",
        )))
    }

    /// Try and find the last linebreak character in a byte slice and return the index
    /// of the character. The function looks specifically for a line-feed (LF) character,
    /// represented as '\n' on Unix systems.
    ///
    /// # Errors
    /// If either the byte slice to search through was empty, or there existed no linebreak
    /// character in the byte slice.
    #[cfg(not(target_os = "windows"))]
    pub fn try_find_last_line_break(&self, bytes: &[u8]) -> Result<usize> {
        if bytes.is_empty() {
            return Err(Box::new(ExecutionError::new(
                "Byte slice to find newlines in was empty, exiting...",
            )));
        };

        let mut idx: usize = bytes.len() - 1;

        while idx > 0 {
            if bytes[idx] == 0x0a {
                return Ok(idx);
            };

            idx -= 1;
        }

        Err(Box::new(ExecutionError::new(
            "Could not find any newlines in byte slice, exiting...",
        )))
    }

    /// Try and find all occurances of linebreak characters in a byte slice and push
    /// the index of the byte to a provided buffer. The function looks specifically
    /// for two characters, the carriage-return (CR) and line-feed (LF) characters,
    /// represented as the character sequence '\r\n' on Windows systems.
    ///
    /// # Errors
    /// If the byte slice to search through was empty.
    #[cfg(target_os = "windows")]
    pub fn try_find_line_breaks(
        &self,
        bytes: &[u8],
        buffer: &mut Vec<usize>,
        add_starting_idx: bool,
    ) -> Result<()> {
        if bytes.is_empty() {
            return Err(Box::new(ExecutionError::new(
                "Byte slice to find newlines in was empty, exiting...",
            )));
        };

        // We need to also set the starting position of the current buffer, which is on index 0.
        // This is needed for multitthreading when threads need to know the byte indices of their slice.
        if add_starting_idx {
            buffer.push(0);
        }

        (1..bytes.len()).for_each(|idx| {
            if (bytes[idx - 1] == 0x0d) && (bytes[idx] == 0x0a) {
                buffer.push(idx - 1);
            };
        });

        Ok(())
    }
    /// Try and find all occurances of linebreak characters in a byte slice and push
    /// the index of the byte to a provided buffer. The function looks specifically
    /// for a line-feed (LF) character, represented as '\n' on Unix systems.
    ///
    /// # Errors
    /// If the byte slice to search through was empty.
    #[cfg(not(target_os = "windows"))]
    pub fn try_find_line_breaks(
        &self,
        bytes: &[u8],
        buffer: &mut Vec<usize>,
        add_starting_idx: bool,
    ) -> Result<()> {
        if bytes.is_empty() {
            return Err(Box::new(ExecutionError::new(
                "Byte slice to find newlines in was empty, exiting...",
            )));
        };

        // We need to also set the starting position of the current buffer, which is on index 0.
        // This is needed for multitthreading when threads need to know the byte indices of their slice.
        if add_starting_idx {
            buffer.push(0);
        }

        (0..bytes.len()).for_each(|idx| {
            if bytes[idx] == 0x0a {
                buffer.push(idx);
            };
        });

        Ok(())
    }

    /// Get the total number of bytes to read.
    pub fn bytes_to_read(&self) -> usize {
        self.bytes_to_read
    }

    /// Get the number of remaining bytes to read.
    pub fn remaining_bytes(&self) -> usize {
        self.remaining_bytes
    }

    /// Set the number of remaining bytes to read.
    pub fn set_remaining_bytes(&mut self, remaining_bytes: usize) {
        self.remaining_bytes = remaining_bytes;
    }

    /// Get the total number of processed bytes.
    pub fn bytes_processed(&self) -> usize {
        self.bytes_processed
    }

    /// Set the total number of processed bytes.
    pub fn set_bytes_processed(&mut self, bytes_processed: usize) {
        self.bytes_processed = bytes_processed;
    }

    /// Get the total number of overlapped bytes (due to sliding window).
    pub fn bytes_overlapped(&self) -> usize {
        self.bytes_overlapped
    }

    /// Set the total number of overlapped bytes.
    pub fn set_bytes_overlapped(&mut self, bytes_overlapped: usize) {
        self.bytes_overlapped = bytes_overlapped;
    }

    /// Try and evenly distribute the buffer into uniformly sized chunks for each worker thread.
    /// This function expects a [`Vec`] of usize tuples, representing the start and end byte
    /// indices for each worker threads chunk.
    ///
    /// # Note
    /// This function is optimized to spend as little time as possible looking for valid chunks, i.e.,
    /// where there are line breaks, and will not look through the entire buffer. This can have an
    /// effect on the CPU cache hit-rate, however, this depends on the size of the buffer.
    ///
    /// # Panics
    /// This function might panics on an error for the following reasons:
    /// * If the buffer was empty.
    /// * If there were no line breaks in the buffer.
    pub fn distribute_workloads(
        &self,
        buffer: &[u8],
        workloads: &mut Vec<(usize, usize)>,
    ) {
        self.try_distribute_workloads(buffer, workloads).unwrap();
    }

    /// Try and evenly distribute the buffer into uniformly sized chunks for each worker thread.
    /// This function expects a [`Vec`] of usize tuples, representing the start and end byte
    /// indices for each worker threads chunk.
    ///
    /// # Note
    /// This function is optimized to spend as little time as possible looking for valid chunks, i.e.,
    /// where there are line breaks, and will not look through the entire buffer. This can have an
    /// effect on the CPU cache hit-rate, however, this depends on the size of the buffer.
    ///
    /// # Errors
    /// This function might return an error for the following reasons:
    /// * If the buffer was empty.
    /// * If there were no line breaks in the buffer.
    pub fn try_distribute_workloads(
        &self,
        buffer: &[u8],
        thread_workloads: &mut Vec<(usize, usize)>,
    ) -> Result<()> {
        let n_bytes_total: usize = buffer.len();
        let n_worker_threads: usize = thread_workloads.capacity();

        let n_bytes_per_thread: usize = n_bytes_total / n_worker_threads;
        let n_bytes_remaining: usize = n_bytes_total - n_bytes_per_thread * n_worker_threads;

        let mut prev_byte_idx: usize = 0;
        for _ in 0..(n_worker_threads - 1) {
            let next_byte_idx: usize = n_bytes_per_thread + prev_byte_idx;
            thread_workloads.push((prev_byte_idx, next_byte_idx));
            prev_byte_idx = next_byte_idx;
        }

        thread_workloads.push((
            prev_byte_idx,
            prev_byte_idx + n_bytes_per_thread + n_bytes_remaining,
        ));

        let mut n_bytes_to_offset_start: usize = 0;
        for t_idx in 0..n_worker_threads {
            let (mut start_byte_idx, mut end_byte_idx) = thread_workloads[t_idx];
            start_byte_idx -= n_bytes_to_offset_start;
            let n_bytes_to_offset_end: usize = (end_byte_idx - start_byte_idx)
                - self.try_find_last_line_break(&buffer[start_byte_idx..end_byte_idx])?;
            end_byte_idx -= n_bytes_to_offset_end;
            thread_workloads[t_idx].0 = start_byte_idx;
            thread_workloads[t_idx].1 = end_byte_idx;
            n_bytes_to_offset_start = n_bytes_to_offset_end - NUM_BYTES_FOR_NEWLINE;
        }

        Ok(())
    }
}

impl<'a> Slicer<'a> for FixedWidthSlicer {
    type Buffer = &'a mut [u8];

    /// Get whether or not this [`Slicer`] is done reading the input file.
    fn is_done(&self) -> bool {
        self.bytes_processed >= self.bytes_to_read
    }

    /// Read from the buffered reader into the provided buffer. This function reads
    /// enough bytes to fill the buffer, hence, it is up to the caller to ensure that
    /// that buffer has the correct and/or wanted capacity.
    ///
    /// # Panics
    /// If the buffered reader encounters an EOF before completely filling the buffer.
    fn read_to_buffer(&mut self, buffer: Self::Buffer) {
        self.inner.read_exact(buffer).unwrap();
    }

    /// Seek relative to the current position in the buffered reader.
    ///
    /// # Panics
    /// Seeking to a negative offset will cause the program to panic.
    fn seek_relative(&mut self, bytes_to_seek: i64) {
        self.try_seek_relative(bytes_to_seek).unwrap()
    }

    /// Try and read from the buffered reader into the provided buffer. This function
    /// reads enough bytes to fill the buffer, hence, it is up to the caller to
    /// ensure that the buffer has the correct and/or wanted capacity.
    ///
    /// # Errors
    /// If the buffered reader encounters an EOF before completely filling the buffer.
    fn try_read_to_buffer(&mut self, buffer: Self::Buffer) -> Result<()> {
        match self.inner.read_exact(buffer) {
            Ok(()) => Ok(()),
            Err(e) => match e.kind() {
                ErrorKind::UnexpectedEof => {
                    warn!("EOF reached, this should be the last time reading from the file.");
                    Ok(())
                }
                _ => Err(Box::new(e)),
            },
        }
    }
    /// Try and seek relative to the current position in the buffered reader.
    ///
    /// # Errors
    /// Seeking to a negative offset will return an error.
    fn try_seek_relative(&mut self, bytes_to_seek: i64) -> Result<()> {
        self.inner.seek_relative(bytes_to_seek)?;
        Ok(())
    }
}