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
* File created: 2024-02-17
* Last updated: 2024-02-17
*/

use crate::schema::FixedSchema;
use log::{info, warn};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;

pub(crate) static DEFAULT_SLICE_BUFFER_LEN: usize = 1024 * 1024;

///
pub struct Converter {
    file: File,
    schema: FixedSchema,
    n_threads: usize,
    multithreaded: bool,
}

///
impl Converter {

    ///
    pub fn new(file: PathBuf, schema: FixedSchema, n_threads: usize) -> Self {
        Self {
            file: File::open(&file).expect("Could not open file"),
            schema,
            n_threads,
            multithreaded: n_threads > 1,
        }
    }

    ///
    pub fn convert(&mut self) {
        info!("Using {} threads.", self.n_threads);
        if self.multithreaded {
            self.convert_multithreaded();
        } else {
            self.convert_single_threaded();
        }
    }

    ///
    fn convert_single_threaded(&self) {
        todo!()
    }

    /// We can check the total amount of bytes in the file.
    /// Otherwise, we need to read a size N and split the parsing to
    /// the worker threads. Not sure how much speed up this will yield though.
    fn convert_multithreaded(&mut self) {
        // let thread_workload = self.distribute_thread_workload();
        let bytes_to_read = self.file.metadata().expect("Could not read file metadata").len() as usize;
        info!("File is {} bytes total!", bytes_to_read);
        let mut remaining_bytes = bytes_to_read;
        let mut bytes_processed: usize = 0;
        let mut buff_capacity = DEFAULT_SLICE_BUFFER_LEN;

        loop {
            // When we have read all the bytes we break.
            // Should be done at this point.
            if bytes_to_read <= bytes_processed {
                break;
            }

            if remaining_bytes < DEFAULT_SLICE_BUFFER_LEN { buff_capacity = remaining_bytes; };

            // Read part of the file and find index of where to slice the file.
            let mut buffer = vec![0u8; buff_capacity];
            match BufReader::new(&self.file).read_exact(&mut buffer).is_ok() {
                true => {},
                false => {
                    info!("EOF!")
                }
            };

            let ln_break_indices: Vec<(usize, usize)> = find_line_breaks(&buffer);
            let rows: Vec<&[u8]> = vec![];

            info!("Read this many bytes from file:   {}", buffer.len());
            info!("Should have read this many bytes: {}", buff_capacity);
            bytes_processed += buffer.len();
            remaining_bytes -= bytes_processed;
        }
    }

    /// This will be a vector containing each read_exact index offset
    /// for the threads to use when reading the fixed-length file.
    fn distribute_thread_workload(&self) -> Vec<usize> {
        todo!()
    }
}

/// This only works on Unix systems!
/// 0x0a is "\n", also known as LF (line feed)
///
/// On windows this should be to look for 0x0d && 0x0a, "\r\n", CR+LF
pub fn find_line_breaks(bytes: &[u8]) -> Vec<(usize, usize)> {
    if bytes.is_empty() { panic!("UUUH, empty bytes slice!, something went wrong..."); }

    let n_bytes = bytes.len();
    if n_bytes == 0 { panic!("UUUH, bytes is length 1, not good, dont know what to do here..."); }
    
    let mut line_break: Vec<(usize, usize)> = vec![];
    let mut last_idx: usize = 0;
    for idx in 0..n_bytes {
        if bytes[idx] == 0x0a {
            line_break.push((last_idx, idx));
            last_idx = idx;
        }
    }

    line_break
}
