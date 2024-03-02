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
* Last updated: 2024-02-18
*/

use log::{debug, error, info};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use crate::converters::Converter;
use crate::slicers::Slicer;

pub(crate) static DEFAULT_SLICE_BUFFER_LEN: usize = 1024 * 1024;

///
pub struct new_slicer {
    file: File,
    n_threads: usize,
    multithreaded: bool,
}

///
impl Slicer for new_slicer {

    fn slice_and_convert(&mut self,
                         mut converter: Box<dyn Converter>,
                         mut file: File,
                         in_chunk_cores: usize,
    ) {
        let bytes_to_read = file.metadata().expect("Could not read file metadata").len() as usize;
        info!("File is {} bytes total!", bytes_to_read);
        let mut remaining_bytes = bytes_to_read;
        let mut bytes_processed: usize = 0;
        let mut bytes_overlapped: usize = 0;

        let mut buff_capacity = DEFAULT_SLICE_BUFFER_LEN;
        let mut file_reader = BufReader::new(file);

        loop {
            // When we have read all the bytes we break.
            // We know about this cus we reach EOF in the BufReader.
            if bytes_processed >= bytes_to_read { break; }

            debug!("==========================================================");

            if remaining_bytes < DEFAULT_SLICE_BUFFER_LEN { buff_capacity = remaining_bytes; };

            // Read part of the file and find index of where to slice the file.
            let mut buffer = vec![0u8; buff_capacity];
            match file_reader.read_exact(&mut buffer).is_ok() {
                true => {},
                false => {
                    debug!("EOF, this is the last time reading to buffer...");
                }
            };

            let slices: Vec<&[u8]> = find_worker_borders_aligned_to_linebreak(&buffer, in_chunk_cores as i16);
            let  byte_idx_last_line_break = 0 ;// TODO calculate this !
            let n_bytes_left_after_line_break = buff_capacity - 1 - byte_idx_last_line_break;

            debug!("bytes read:                {}", buff_capacity);
            debug!("byte idx last ln break:    {}", byte_idx_last_line_break);
            debug!("bytes left after ln break: {}", n_bytes_left_after_line_break);
            debug!("Last byte in buffer:       {:?}", buffer.last().unwrap());
//            debug!("Byte on last ln break:     {}", buffer[*byte_idx_last_line_break]);

            // We want to move the file descriptor cursor back N bytes so
            // that the next reed starts on a new row! Otherwise we will
            // miss data, however, this means that we are reading some
            // of the data multiple times. A little bit of an overlap but that's fine...
            debug!("we should move cursor back: -{} bytes", n_bytes_left_after_line_break);
            match file_reader.seek_relative(-(n_bytes_left_after_line_break as i64)).is_ok() {
                true => {},
                false => {
                    error!("WTFFFFFFF we could not move BufReader cursor!");
                }
            };

            // TODO: This is where we should slice the buffer and send it to workers!
             let bytes_processed_for_slices = converter.process(slices);

//            let _rows: Vec<&[u8]> = vec![];

            bytes_processed += buff_capacity - n_bytes_left_after_line_break;
            bytes_overlapped += n_bytes_left_after_line_break;
            remaining_bytes -= buff_capacity - n_bytes_left_after_line_break;

            debug!("We have processed {} bytes!", bytes_processed);
            debug!("We have {} bytes left to read!", remaining_bytes);
        }
        info!("We read {} bytes two times (due to sliding window overlap).", bytes_overlapped);
    }
}

/// For 5 worker , split the buffer in 5 simply size. Adjust these 5 line index to not break an lines.
/// There will be a leftover that is up to 1 line long but not a full line. It can be zero with with big luck.
/// This leftover will be seeked backwards from the caller of this function.
pub fn find_worker_borders_aligned_to_linebreak(bytes: &[u8],n_threads:i16) -> Vec<&[u8]> {

    let mut worker_borders: Vec<&[u8]> = vec![];
    let worker_size=bytes.len()/(n_threads as usize);
    let mut worker_start_pos=0;
    let mut worker_stop_pos=worker_size-1;

// Simply calculate the border without care of breaking lines.
    for index in 0..n_threads {
        worker_borders.push(&bytes[worker_start_pos..worker_stop_pos]);
        worker_start_pos+=worker_size;
        worker_stop_pos+=worker_size;
    }

// TODO ADJUST borders
// TODO Since we certainly broke lines , adjust the borders to be aligned with line by seeking for LINEBREAKS.

    worker_borders
}
