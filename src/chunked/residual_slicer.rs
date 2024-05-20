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
// File created: 2023-12-11
// Last updated: 2024-05-15
//

use log::info;

use std::cmp;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::{ChunkAndResidue, Converter, FnFindLastLineBreak, IterRevolver, Slicer, Stats};

pub(crate) const SLICER_IN_CHUNK_SIZE: usize = 1024 * 2024;
pub(crate) const SLICER_MAX_RESIDUE_SIZE: usize = SLICER_IN_CHUNK_SIZE;

pub(crate) const IN_MAX_CHUNKS: usize = 2;

pub(crate) struct ResidualSlicer<'a> {
    pub(crate) fn_find_last_nl: FnFindLastLineBreak<'a>,
}

impl<'a> Slicer<'a> for ResidualSlicer<'a> {
    fn slice_and_convert(
        &mut self,
        mut converter: Box<dyn 'a + Converter<'a>>,
        infile: fs::File,
        n_threads: usize,
    ) -> Result<Stats, &str> {
        let in_buffers: &mut [ChunkAndResidue; IN_MAX_CHUNKS] = &mut [
            ChunkAndResidue {
                chunk: Box::new([0_u8; SLICER_IN_CHUNK_SIZE]),
            },
            ChunkAndResidue {
                chunk: Box::new([0_u8; SLICER_IN_CHUNK_SIZE]),
            },
        ];

        let mut bytes_in = 0;
        let mut bytes_out = 0;
        let mut read_duration_tot: Duration = Duration::new(0, 0);
        let mut parse_duration_tot: Duration = Duration::new(0, 0);
        let mut builder_write_duration_tot: Duration = Duration::new(0, 0);

        let mut remaining_file_length = infile.metadata().unwrap().len() as usize;

        let mut residue = [0_u8; SLICER_MAX_RESIDUE_SIZE];
        let mut residue_len = 0;
        let mut slices: Vec<&[u8]>;

        let mut ir = IterRevolver {
            shards: in_buffers.as_mut_ptr(),
            next: 0,
            len: in_buffers.len(),
            phantom: std::marker::PhantomData,
        };


        converter.setup();

        rayon::ThreadPoolBuilder::new()
            .stack_size(((SLICER_IN_CHUNK_SIZE as f32) * 2f32) as usize)
            .build_global()
            .unwrap();

        loop {
            let cr = ir.next().unwrap();

            let mut chunk_len_toread = SLICER_IN_CHUNK_SIZE;
            if remaining_file_length < SLICER_IN_CHUNK_SIZE {
                chunk_len_toread = remaining_file_length;
            }

            let chunk_len_effective_read: usize;

            let start_read = Instant::now();

            (residue_len, chunk_len_effective_read, slices) = read_chunk_and_slice(
                self.fn_find_last_nl,
                &mut residue,
                &mut cr.chunk,
                &infile,
                n_threads,
                residue_len,
                chunk_len_toread,
            );

            read_duration_tot += start_read.elapsed();

            remaining_file_length -= chunk_len_effective_read;
            let (bin, bout, parse_duration, builder_write_duration) = converter.process(slices);
            bytes_in += bin;
            bytes_out += bout;
            parse_duration_tot += parse_duration;
            builder_write_duration_tot += builder_write_duration;

            if remaining_file_length == 0 {
                break;
            }
        }

        let cr = ir.next().unwrap();

        if 0 != residue_len {
            slices = residual_to_slice(&residue, &mut cr.chunk, residue_len);

            let (bin, bout, parse_duration, builder_write_duration) = converter.process(slices);
            bytes_in += bin;
            bytes_out += bout;
            parse_duration_tot += parse_duration;
            builder_write_duration_tot += builder_write_duration;
        }

        info!(
            "Bytes in= {}\n out= {}\nparse duration= {:?}\n \n builder write_duration {:?}\n",
            bytes_in, bytes_out, parse_duration_tot, builder_write_duration_tot
        );


        Ok(Stats {
            bytes_in,
            bytes_out: bytes_out,
            num_rows: 0,
            read_duration: read_duration_tot,
            parse_duration: parse_duration_tot,
            builder_write_duration: builder_write_duration_tot,
        })
        }
    }


fn residual_to_slice<'a>(
    residue: &[u8; SLICER_IN_CHUNK_SIZE],
    chunk: &'a mut [u8; SLICER_IN_CHUNK_SIZE],
    residue_effective_len: usize,
) -> Vec<&'a [u8]> {
    #[allow(unused_mut)]
    let mut target_chunk_residue: &mut [u8];

    (target_chunk_residue, _) = chunk.split_at_mut(residue_effective_len);
    if 0 != residue_effective_len {
        target_chunk_residue.copy_from_slice(&residue[0..residue_effective_len]);
    }
    let r: Vec<&[u8]> = vec![target_chunk_residue];
    r
}

fn read_chunk_and_slice<'a>(
    fn_line_break: fn(&'a [u8]) -> (bool, usize),
    residue: &mut [u8; SLICER_MAX_RESIDUE_SIZE],
    chunk: &'a mut [u8; SLICER_IN_CHUNK_SIZE],
    file: &File,
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
        //        let residual = &chunk[p1..=data_to_split_len].to_vec();

        residue[0..residual.len()].copy_from_slice(residual);
        (residual.len(), chunk_len_was_read, r)
    }
}
