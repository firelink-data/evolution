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
* Last updated: 2023-12-11
*/

use std::{cmp, fs};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};


/**
GOAL(s)


0) Read a chunk of the file
1) split the chunk that was read in x parts in O(1) including line alignment search, add to chunk-to-be-parsed list
2) Goto 0 read next chunk (bring last fraction of "line" that was left out).


V1 Parameters will will be:
IN_CHUNK_SIZE
in_max_chunks
in_chunk_cores (how man splits will be made)


*/
type FnLineBreak = fn(bytes: &[u8]) -> (bool, usize);
type FnProcessSlices = fn(slices: Vec<& [u8]> ) -> (usize,usize);

//let mut file = File::open("src/seks/fixed_width_output.txt").expect("bbb");
// slice_and_process(find_last_nl,dummy_handle_slices_to_file,file,8)
pub(crate) fn slice_and_process(fn_line_break: FnLineBreak,
                   fn_process_slices:FnProcessSlices,
                                mut file:File,
                                in_chunk_cores:i16
)
{
    let mut bytes_processed = 0;
    const IN_CHUNK_SIZE: usize = 1024*1024;
    let in_max_chunks: i8 = 3;
    let mut remaining_file_length=file.metadata().unwrap().len() as usize;
    let mut chunks=[[0_u8; IN_CHUNK_SIZE],[0_u8; IN_CHUNK_SIZE],[0_u8; IN_CHUNK_SIZE]];
    let mut next_chunk = 0;
    let  residue: &mut [u8] = & mut[0_u8; IN_CHUNK_SIZE];
    let mut residue_len =0;

    loop {

        let mut  slices: Vec<& [u8]> = vec![];

        let n: bool = false;
 //       let chunk_len_toread=std::cmp::min(IN_CHUNK_SIZE,remaining_file_length) as usize;

        let mut chunk_len_toread=IN_CHUNK_SIZE;
        if (remaining_file_length<IN_CHUNK_SIZE) {
            chunk_len_toread=remaining_file_length;
        }

        let mut chunk_len_effective_read:usize=0;

         (residue_len,chunk_len_effective_read ,slices)=read_chunk_and_slice (
            fn_line_break,
            residue,
            &mut chunks[next_chunk],
            &mut file,
            in_chunk_cores,
            residue_len,
            chunk_len_toread);

        remaining_file_length-=chunk_len_effective_read;
        fn_process_slices(slices);


        bytes_processed+=residue_len;


        next_chunk += 1;
        next_chunk = next_chunk % (in_max_chunks as usize);

        if remaining_file_length == 0 {
            if(0!=residue_len) {
                // Whoopsie what to do whith
//                file_out.write_all(&residue[0..residue_len]).expect("dasd");
            }
            println!("End of data with a residual ... not handled atm or.. is this never happening ? Drives me insane to think about it :) ");
            break;
        }

    }
    println!("Bytes processed {}", bytes_processed)
}


fn read_chunk_and_slice<'a>(
    fn_line_break: FnLineBreak,
    residue  : &'a mut [u8],
    chunk : &'a mut[u8],
    file: &mut File,
    chunk_cores: i16,
    residue_effective_len: usize,
    chunk_len_toread: usize
) -> (usize,usize, Vec<&'a  [u8]>) {

    let mut target_chunk_residue: &mut [u8] = &mut [];
    let mut target_chunk_read: &mut [u8] = &mut [];

    (target_chunk_residue, target_chunk_read) = chunk.split_at_mut(residue_effective_len);
    if 0 != residue_effective_len {
        target_chunk_residue.copy_from_slice(&residue[0..residue_effective_len]);
    }
    let target_chunk_read_len=    target_chunk_read.len();

    let read_exact_buffer =&mut target_chunk_read[0..cmp::min(target_chunk_read_len, chunk_len_toread)];

    let n = BufReader::new(file)
        .read_exact(read_exact_buffer)
        .is_ok();
    let chunk_len_was_read= read_exact_buffer.len();

    //
    // Below could be separate function called Split !
    //

    let mut r: Vec<&  [u8]> =  vec![];
    let mut data_to_split_len=chunk_len_was_read+residue_effective_len;

    let core_block_size = data_to_split_len / chunk_cores as usize;

    let mut p1: usize = 0;
    let mut p2: usize = core_block_size;
    for i in 0..chunk_cores {
        // Adjust p2 to nearest found newline

        let  (mut found,mut line_break_offset)=fn_line_break( & chunk[p1..=p2]);

        if(false==found) {
            // Corner case  to short lines vs core amount ?
            (found,line_break_offset)=fn_line_break( & chunk[p1..=data_to_split_len -1]);
            if !found {
                panic!("Could not find a linebreak in chunk. Might be due to chunks shorter than line lenght , or missing linebreak in data. Chunk  lenght {}", chunk_len_toread);
            }
        }

        p2 = p1 + line_break_offset;

        r.push(&chunk[p1..=p2]);

        p1 = p2 + 1; // Check we inside chunk before continue!!
        if(p1 > data_to_split_len - 1) {
            break // All data consumed.
        }

        p2 = cmp::min(p1 + core_block_size, data_to_split_len - 1);
    }


    if(p1 > data_to_split_len - 1) {
        (0, chunk_len_was_read, r)
    } else {
        let residual=& chunk[p1..=data_to_split_len - 1].to_vec();
        residue[0..residual.len()].copy_from_slice(residual);
        (residual.len(), chunk_len_was_read, r)
    }


}

fn find_last_nlcr(bytes: &[u8]) -> (bool,usize) {
    if 0 == bytes.len() {
        return (false,0); // TODO should report err ...
    }

    let mut p2 = bytes.len() - 1;

    if 0 == p2 {
        return (false,0); // hmm
    }

    loop {
        if bytes[p2 - 1] == 0x0d && bytes[p2] == 0x0a {
            return (true,p2 + 1);
        }
        if (0==p2) {
            return (false,0); // indicate we didnt find nl
        }

        p2 = p2 - 1;
    }

}
// Returns the INDEX in the u8 byte array
fn find_last_nl(bytes: &[u8]) -> (bool,usize) {
    if 0 == bytes.len() {
        return (false,0); // Indicate we didnt found nl.
    }

    let mut p2 = bytes.len() - 1;

    if 0 == p2 {
        return (false,0); // hmm
    }

    loop {
        if bytes[p2] == 0x0a {
            return (true,p2);
        }
        if (0==p2) {
            return (false,0); // indicate we didnt find nl
        }
        p2 = p2 - 1;
    }

}

fn dummy_handle_slices_to_file(slices:Vec<&[u8]>) -> (usize,usize) {
    let mut bytes_processed:usize=0;

    for val in slices {
        let s = match std::str::from_utf8(val) {
            Ok(v) => v,
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };

        print!("result: {}", s);

        let l = val.len();
        bytes_processed = bytes_processed + l;
    }
    (bytes_processed,0)
}
