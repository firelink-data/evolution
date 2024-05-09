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
* File created: 2023-12-11
* Last updated: 2023-12-14
*/
use crate::converters::Converter;
use crate::slicers::old_slicer::{IN_MAX_CHUNKS, SLICER_IN_CHUNK_SIZE};
use std::fs;


pub mod old_slicer;
//pub mod new_slicer;

pub(crate) struct ChunkAndResidue {
    pub(crate) chunk: Box<[u8; SLICER_IN_CHUNK_SIZE]>,
}
pub(crate) trait Slicer<'a> {
    fn slice_and_convert(
        &mut self,
        converter: Box<dyn 'a + Converter<'a>>,
        infile: fs::File,
        n_threads: usize,
    ) -> Result<Stats, &str>;
}
pub(crate) struct Stats {
    pub(crate) bytes_in: usize,
    pub(crate) bytes_out: usize,

    pub(crate) num_rows: i64,
}

pub(crate) type FnLineBreakLen = fn() -> usize;
#[allow(dead_code)]
pub(crate) fn line_break_len_cr() -> usize {
    1 as usize
}
#[allow(dead_code)]
pub(crate) fn line_break_len_crlf() -> usize {
    2 as usize
}

pub(crate) type FnFindLastLineBreak<'a> = fn(bytes: &'a [u8]) -> (bool, usize);
#[allow(dead_code)]
pub(crate) fn find_last_nlcr(bytes: &[u8]) -> (bool, usize) {
    if bytes.is_empty() {
        return (false, 0); // TODO should report err ...
    }

    let mut p2 = bytes.len() - 1;

    if 0 == p2 {
        return (false, 0); // hmm
    }

    loop {
        if bytes[p2 - 1] == 0x0d && bytes[p2] == 0x0a {
            return (true, p2 + 1);
        }
        if 0 == p2 {
            return (false, 0); // indicate we didnt find nl
        }

        p2 -= 1;
    }
}

#[allow(dead_code)]
pub(crate) fn find_last_nl(bytes: &[u8]) -> (bool, usize) {
    if bytes.is_empty() {
        return (false, 0); // Indicate we didnt found nl.
    }

    let mut p2 = bytes.len() - 1;

    if 0 == p2 {
        return (false, 0); // hmm
    }

    loop {
        if bytes[p2] == 0x0a {
            return (true, p2);
        }
        if 0 == p2 {
            return (false, 0); // indicate we didnt find nl
        }
        p2 -= 1;
    }
}
pub(crate) struct IterRevolver<'a, T> {
    shards: *mut T,
    next: usize,
    len: usize,
    phantom: std::marker::PhantomData<&'a mut [T]>,
}

impl<'a, T> From<&'a mut [T]> for IterRevolver<'a, T> {
    fn from(shards: &'a mut [T]) -> IterRevolver<'a, T> {
        IterRevolver {
            next: 0,
            len: shards.len(),
            shards: shards.as_mut_ptr(),
            phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T> Iterator for IterRevolver<'a, T> {
    type Item = &'a mut T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next < self.len {
            self.next += 1;
        } else {
            self.next = 1;
        }
        unsafe { Some(&mut *self.shards.offset(self.next as isize - 1)) }
    }
}
