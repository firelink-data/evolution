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
* Last updated: 2024-05-02
*/

use crate::builder::{self, ColumnBuilder};
use crate::schema::FixedSchema;
use rayon::prelude::*;
use crossbeam::channel;
use log::{debug, error, info};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::slice::Iter;

pub(crate) static DEFAULT_SLICE_BUFFER_LEN: usize = 4 * 1024 * 1024;
pub(crate) static DEFAULT_SLICER_THREAD_CHANNEL_CAPACITY: usize = 128;

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
            let mut buffer: Vec<u8> = vec![0u8; buff_capacity];
            match file_reader.read_exact(&mut buffer).is_ok() {
                true => {},
                false => {
                    debug!("EOF, this is the last time reading to buffer...");
                }
            };

            let line_indices: Vec<usize> = find_line_breaks_unix(&buffer);
            let byte_idx_last_line_break = line_indices.last().expect("The line index list was empty!");
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

            bytes_processed += buff_capacity - n_bytes_left_after_line_break;
            bytes_overlapped += n_bytes_left_after_line_break;
            remaining_bytes -= buff_capacity - n_bytes_left_after_line_break;

            let mut slices: Vec<&[u8]> = Vec::with_capacity(self.n_threads - 1);

            let thread_workloads: Vec<&[usize]> = self.distribute_worker_thread_workloads(&line_indices);
            debug!("Thread workload: {:?}", thread_workloads);
            for range in thread_workloads.iter() {
                slices.push(&buffer[range[0]..range[range.len() - 1]]);
            }

            spawn_convert_threads(
                &slices,
                &thread_workloads,
                self.schema.to_owned(),
            );
        }
        info!("We read {} bytes two times (due to sliding window overlap).", bytes_overlapped);
    }

    fn distribute_worker_thread_workloads(&self, line_indices: &Vec<usize>) -> Vec<&[usize]> {
        let n_line_indices: usize = line_indices.len();
        let n_rows_per_thread: usize = n_line_indices / (self.n_threads - 1);

        let remainder: usize = line_indices.len() % (self.n_threads - 1);
        debug!("n rows per thread: {:?}", n_rows_per_thread);
        debug!("Last thread will get {} extra bytes to process.", remainder);

        let mut workload: Vec<&[usize]> = Vec::with_capacity(self.n_threads);
        let mut prev_idx: usize = 0;
        for idx in 1..self.n_threads - 1 {
            let next_idx = line_indices[n_rows_per_thread * idx];
            workload.push(&line_indices[prev_idx..n_rows_per_thread * idx]);
            // If this is a window system it should add 2 instead, because of CR+LF!
            prev_idx = next_idx + 1;
        }

        workload.push(&line_indices[prev_idx..n_line_indices - 1]);
        workload
    }
}

pub fn spawn_convert_threads(
    slices: &Vec<&[u8]>,
    thread_workloads: &Vec<&[usize]>,
    schema: FixedSchema,
) {
    let (sender, receiver) = channel::bounded(DEFAULT_SLICER_THREAD_CHANNEL_CAPACITY);

    slices.into_par_iter().enumerate().for_each_with(&sender,|s, (t, slice)| worker_thread_convert(t, *slice, thread_workloads[t], &schema, s));

    drop(sender);
    master_thread_convert(&receiver);
}

pub fn master_thread_convert(
    channel: &channel::Receiver<Vec<u8>>,
) {
    // TODO: write to parquet files, approx 1GB per file might be good
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("bytes-from-workers.dat")
        .expect("Could not open target file!");

    for buff in channel {
        file.write_all(buff.as_slice())
            .expect("Got bad buffer from thread, write failed!");
    }
}


pub fn byte_indices_from_runes(
    line: &[u8],
    total_runes: usize,
    col_lengths: &Vec<usize>,
) -> Vec<usize> {

    let mut acc_runes: usize = 0;
    let mut num_bytes: usize = 0;
    let mut units: usize = 1;
    let mut iterator: Iter<u8> = line.iter();
    let mut byte_indices: Vec<usize> = Vec::with_capacity(col_lengths.len());

    while acc_runes < total_runes {
        let byte = match iterator.nth(units - 1) {
            None => return byte_indices,
            Some(b) => *b,
        };

        units = match byte {
            byte if byte >> 7 == 0 => 1,
            byte if byte >> 5 == 0b110 => 2,
            byte if byte >> 4 == 0b1110 => 3,
            byte if byte >> 3 == 0b1110 => 4,
            _ => {
                panic!("Incorrect UTF-8 sequence!");
            }
        };

        acc_runes += 1;
        num_bytes += units;
        byte_indices.push(num_bytes);
    }

    byte_indices
}

pub fn worker_thread_convert(
    thread: usize,
    slice: &[u8],
    line_breaks: &[usize],
    schema: &FixedSchema,
    sender: &channel::Sender<Vec<u8>>,
) {
    info!("Thread {} converting new slice...", thread);

    let col_lengths: Vec<usize> = schema.column_lengths();
    let mut prev_line_idx: usize = 0;
    let column_builders = schema
        .iter()
        .map(|c| c.as_column_builder())
        .collect::<Vec<Box<dyn ColumnBuilder>>>();

    for line_break_idx in line_breaks.into_iter() {

        let line: &[u8] = &slice[prev_line_idx..*line_break_idx];
        let mut byte_indices: Vec<usize> = byte_indices_from_runes(
            line,
            col_lengths.iter().sum(),
            &col_lengths,
        );

        let mut prev_byte_idx: usize = 0;
        for (byte_idx, builder) in byte_indices.iter_mut().zip(&column_builders) {
            builder.push_bytes(&line[prev_byte_idx..*byte_idx]);
            prev_byte_idx = *byte_idx;
        }


        // for (col_idx, builder) in column_builders.iter_mut().enumerate() {
          //   let col_num_bytes = builder.parse_to_bytes();
        // }


        // for (col_idx, (col_offset, col_len)) in schema.column_offsets().iter().zip(schema.column_lengths()).enumerate() {
            // columns[col_idx].push(line[col_offset..col_offset+col_len]);
        // }
        prev_line_idx = *line_break_idx;
    }

    let _ = sender.send(vec![0u8;10]);
}

#[allow(dead_code)]
/// On windows systems line breaks in files are represented by "\r\n",
/// also called CR+LF (Carriage Return + Line Feed), represented by
/// the bytes 0x0d and 0x0a.
///
/// TODO: this should be used whenever host system is windows!
pub fn find_line_breaks_windows(bytes: &[u8]) -> Vec<(usize, usize)> {
    if bytes.is_empty() { panic!("Empty bytes slice!"); }

    let n_bytes = bytes.len();
    if n_bytes == 0 { panic!("No bytes in buffer!"); }

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

/// On *nix systems line breaks in files are represented by "\n",
/// also called LF (Line Feed), represented by the byte 0x0a.
pub fn find_line_breaks_unix(bytes: &[u8]) -> Vec<usize> {
    if bytes.is_empty() { panic!("Empty bytes slice!"); }

    let n_bytes = bytes.len();
    // The only way we can have only one byte left to read is if we have read everything
    // and then this byte will represent EOF, most likely a "\n"?..
    if n_bytes == 0 { panic!("No bytes in buffer!"); }
    
    let mut line_breaks: Vec<usize> = vec![];
    for idx in 0..n_bytes {
        if bytes[idx] == 0x0a {
            line_breaks.push(idx);
        }
    }

    line_breaks
}

//

