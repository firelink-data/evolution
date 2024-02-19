/*
* MIT License
*
* Copyright (c) 2023 Firelink Data
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
* File created: 2023-12-11
* Last updated: 2023-12-14
*/

use std::cmp;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use rayon::prelude::*;
use log::info;
use crate::converters::Converter;
use crate::slicers::{FnLineBreak, Slicer};

/**
GOAL(s)


0) Read a chunk of the file
1) split the chunk that was read in x parts in O(1) including line alignment search, add to chunk-to-be-parsed list
2) Goto 0 read next chunk (bring last fraction of "line" that was left out).


V1 Parameters will will be:
SLICER_IN_CHUNK_SIZE
in_max_chunks
in_chunk_cores (how man splits will be made)

 */

pub(crate) const SLICER_IN_CHUNK_SIZE: usize = 1024 * 1024;

/// Rickard magic!

pub(crate) struct old_slicer {
}

impl Slicer for old_slicer {
     fn slice_and_convert(&mut self,
                          mut converter: Box<dyn Converter>,
                          mut file: File,
                          in_chunk_cores: usize,
    ) {
        let mut bytes_processed = 0;
        let in_max_chunks: i8 = 3;

        let mut remaining_file_length = file.metadata().unwrap().len() as usize;

        let mut chunks = [
            [0_u8; SLICER_IN_CHUNK_SIZE],
            [0_u8; SLICER_IN_CHUNK_SIZE],
            [0_u8; SLICER_IN_CHUNK_SIZE],
        ];

        let mut next_chunk = 0;
        let residue: &mut [u8] = &mut [0_u8; SLICER_IN_CHUNK_SIZE];
        let mut residue_len = 0;

        loop {
            let slices: Vec<&[u8]>;

            let mut chunk_len_toread = SLICER_IN_CHUNK_SIZE;
            if remaining_file_length < SLICER_IN_CHUNK_SIZE {
                chunk_len_toread = remaining_file_length;
            }

            let chunk_len_effective_read: usize;

            (residue_len, chunk_len_effective_read, slices) = read_chunk_and_slice(
                converter.get_line_break_handler(),
                residue,
                &mut chunks[next_chunk],
                &mut file,
                in_chunk_cores,
                residue_len,
                chunk_len_toread,
            );

            remaining_file_length -= chunk_len_effective_read;
            let bytes_processed_for_slices = converter.process(slices);

            bytes_processed += bytes_processed_for_slices;
            bytes_processed += residue_len;

            next_chunk += 1;
            next_chunk %= in_max_chunks as usize;

            if remaining_file_length == 0 {
                if 0 != residue_len {
                    let slice: Vec<&[u8]> = vec![&residue[0..residue_len]];
                    let bytes_processed_for_slices = converter.process(slice);
                    bytes_processed += bytes_processed_for_slices;
                }
                break;
            }
        }

        info!("Bytes processed {}", bytes_processed);
    }


}

fn read_chunk_and_slice<'a>(
    fn_line_break: FnLineBreak,
    residue: &'a mut [u8],
    chunk: &'a mut [u8],
    file: &mut File,
    chunk_cores: usize,
    residue_effective_len: usize,
    chunk_len_toread: usize,
) -> (usize, usize, Vec<&'a [u8]>) {
    #[allow(unused_mut)]
        let mut target_chunk_residue: &mut [u8];
    #[allow(unused_mut)]
        let mut target_chunk_read: &mut [u8];

    (target_chunk_residue, target_chunk_read) = chunk.split_at_mut(residue_effective_len);
    if 0 != residue_effective_len {
        target_chunk_residue.copy_from_slice(&residue[0..residue_effective_len]);
    }
    let target_chunk_read_len = target_chunk_read.len();

    let read_exact_buffer =
        &mut target_chunk_read[0..cmp::min(target_chunk_read_len, chunk_len_toread)];

    let _ = BufReader::new(file).read_exact(read_exact_buffer).is_ok();
    let chunk_len_was_read = read_exact_buffer.len();

    //
    // Below should be separate function called Split !
    //

    let mut r: Vec<&[u8]> = vec![];
    let data_to_split_len = chunk_len_was_read + residue_effective_len;

    let core_block_size = data_to_split_len / chunk_cores;

    let mut p1: usize = 0;
    let mut p2: usize = core_block_size;
    for _i in 0..chunk_cores {
        // Adjust p2 to nearest found newline

        let (mut found, mut line_break_offset) = fn_line_break(&chunk[p1..=p2]);

        if !found {
            // Corner case  to short lines vs core amount ?
            (found, line_break_offset) = fn_line_break(&chunk[p1..=data_to_split_len - 1]);
            if !found {
                panic!("Could not find a linebreak in chunk. Might be due to chunks shorter than line lenght , or missing linebreak in data. Chunk  lenght {}", chunk_len_toread);
            }
        }

        p2 = p1 + line_break_offset;

        r.push(&chunk[p1..=p2]);

        p1 = p2 + 1; // Check we inside chunk before continue!!
        if p1 > data_to_split_len - 1 {
            break; // All data consumed.
        }

        p2 = cmp::min(p1 + core_block_size, data_to_split_len - 1);
    }

    if p1 > data_to_split_len - 1 {
        (0, chunk_len_was_read, r)
    } else {
        let residual = &chunk[p1..=data_to_split_len - 1].to_vec();
        residue[0..residual.len()].copy_from_slice(residual);
        (residual.len(), chunk_len_was_read, r)
    }
}


#[cfg(test)]
mod tests_old_slicer {}
