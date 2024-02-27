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
* Last updated: 2024-02-27
*/

use crate::schema::FixedSchema;
use log::{debug, error, info};
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

    ///
    fn convert_multithreaded(&mut self) {
        let bytes_to_read = self.file.metadata().expect("Could not read file metadata").len() as usize;
        info!("File is {} bytes total!", bytes_to_read);
        let mut remaining_bytes = bytes_to_read;
        let mut bytes_processed: usize = 0;
        let mut bytes_overlapped: usize = 0;

        let mut buff_capacity = DEFAULT_SLICE_BUFFER_LEN;
        let mut file_reader = BufReader::new(&self.file);

        loop {
            // When we have read all the bytes we break.
            // We know about this cus we reach EOF in the BufReader.
            if bytes_processed >= bytes_to_read { break; }

            if remaining_bytes < DEFAULT_SLICE_BUFFER_LEN { buff_capacity = remaining_bytes; };

            // Read part of the file and find index of where to slice the file.
            let mut buffer = vec![0u8; buff_capacity];
            match file_reader.read_exact(&mut buffer).is_ok() {
                true => {},
                false => {
                    debug!("EOF, this is the last time reading to buffer...");
                }
            };

            let line_indices: Vec<(usize, usize)> = find_line_breaks_unix(&buffer);
            let (_, byte_idx_last_line_break) = line_indices.last().expect("The line index list was empty!");
            let n_bytes_left_after_line_break = buff_capacity - 1 - byte_idx_last_line_break;

            // We want to move the file descriptor cursor back N bytes so
            // that the next reed starts on a new row! Otherwise we will
            // miss data, however, this means that we are reading some
            // of the data multiple times. A little bit of an overlap but that's fine...
            match file_reader.seek_relative(-(n_bytes_left_after_line_break as i64)).is_ok() {
                true => {},
                false => {
                   error!("Could not move BufReader cursor back!"); 
                }
            };

            // TODO: This is where we should slice the buffer and send it to workers!
            let _rows: Vec<&[u8]> = vec![];

            bytes_processed += buff_capacity - n_bytes_left_after_line_break;
            bytes_overlapped += n_bytes_left_after_line_break;
            remaining_bytes -= buff_capacity - n_bytes_left_after_line_break;

            debug!("We have processed {} bytes!", bytes_processed);
            debug!("We have {} bytes left to read!", remaining_bytes);
        }
        info!("We read {} bytes two times (due to sliding window overlap).", bytes_overlapped);
    }
}

pub fn find_line_breaks_windows(bytes: &[u8]) -> Vec<(usize, usize)> {
    if bytes.is_empty() { panic!("Empty bytes slice!, something went wrong..."); }

    let n_bytes = bytes.len();
    if n_bytes == 0 { panic!("No bytes in buffer!, something went wrong..."); }

    let mut line_breaks: Vec<(usize, usize)> = vec![];
    let mut last_idx: usize = 0;
    for idx in 1..n_bytes {
        if bytes[idx - 1] == 0x0d && bytes[idx] == 0x0a {
            line_breaks.push((last_idx, idx));
            last_idx = idx;
        }
    }

    line_breaks
}

/// This only works on Unix systems!
/// 0x0a is "\n", also known as LF (line feed)
///
/// On windows this should be to look for 0x0d && 0x0a, "\r\n", CR+LF
pub fn find_line_breaks_unix(bytes: &[u8]) -> Vec<(usize, usize)> {
    if bytes.is_empty() { panic!("UUUH, empty bytes slice!, something went wrong..."); }

    let n_bytes = bytes.len();
    // The only way we can have only one byte left to read is if we have read everything
    // and then this byte will represent EOF, most likely a "\n"?..
    if n_bytes == 0 { panic!("UUUH, bytes is length 1, not good, dont know what to do here..."); }
    
    let mut line_breaks: Vec<(usize, usize)> = vec![];
    let mut last_idx: usize = 0;
    for idx in 0..n_bytes {
        if bytes[idx] == 0x0a {
            line_breaks.push((last_idx, idx));
            last_idx = idx;
        }
    }

    line_breaks
}
