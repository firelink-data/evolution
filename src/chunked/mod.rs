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

use arrow::array::ArrayRef;
use parquet::format;

use std::cmp::min;
use std::fs;

use self::slicer::SLICER_IN_CHUNK_SIZE;

pub(crate) mod arrow_converter;
pub(crate) mod converter;
pub(crate) mod slicer;

pub(crate) struct ChunkAndResidue {
    pub(crate) chunk: Box<[u8; SLICER_IN_CHUNK_SIZE]>,
}
pub(crate) trait Slicer<'a> {
    fn slice_and_convert(
        &mut self,
        converter: Box<dyn 'a + Converter<'a>>,
        infile: fs::File,
        n_threads: usize,
    ) -> Result<crate::chunked::Stats, &str>;
}
pub(crate) struct Stats {
    pub(crate) bytes_in: usize,
    pub(crate) bytes_out: usize,

    pub(crate) num_rows: i64,
}

pub(crate) type FnLineBreakLen = fn() -> usize;
#[allow(dead_code)]
pub(crate) fn line_break_len_cr() -> usize {
    1_usize
}
#[allow(dead_code)]
pub(crate) fn line_break_len_crlf() -> usize {
    2_usize
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

impl<'a, T> From<&'a mut [T]> for crate::chunked::IterRevolver<'a, T> {
    fn from(shards: &'a mut [T]) -> crate::chunked::IterRevolver<'a, T> {
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

#[allow(dead_code)]
pub(crate) trait Converter<'a> {
    fn set_line_break_handler(&mut self, fn_line_break: FnFindLastLineBreak<'a>);
    fn get_line_break_handler(&self) -> FnFindLastLineBreak<'a>;

    //    fn process(& mut self, slices: Vec< &'a[u8]>) -> usize;
    fn process(&mut self, slices: Vec<&'a [u8]>) -> (usize, usize);
    fn finish(&mut self) -> parquet::errors::Result<format::FileMetaData>;
    fn get_finish_bytes_written(&mut self) -> usize;
}

pub trait ColumnBuilder {
    fn parse_value(&mut self, name: &[u8]) -> usize;
    fn finish(&mut self) -> (&str, ArrayRef);
    //    fn name(&  self) -> &String;
}

fn column_length_num_rightaligned(data: &[u8], runes: i16) -> (usize, usize) {
    let mut eat = data.iter();
    let mut counted_runes = 0;
    let mut start: usize = 0;
    let stop: usize = min(data.len(), runes as usize);

    while counted_runes < runes as usize {
        let byten = eat.next();
        let bb: u8 = match byten {
            None => {
                //TODO  we ran out of data,this is an error, fix later.
                return (start, stop);
            }
            Some(b) => *b,
        };

        if let 48..=57 = bb {
            return (start, stop);
        }

        start += 1;
        counted_runes += 1;
    }

    (start, stop)
}

fn column_length_char_rightaligned(data: &[u8], runes: i16) -> (usize, usize) {
    let mut eat = data.iter();
    let mut counted_runes = 0;
    let mut start: usize = 0;
    let stop: usize = min(data.len(), runes as usize);

    while counted_runes < runes as usize {
        let byten = eat.next();
        let bb: u8 = match byten {
            None => {
                //TODO  we ran out of data,this is an error, fix later.
                return (start, stop);
            }
            Some(b) => *b,
        };

        match bb {
            101..=132 => return (start, stop),
            141..=172 => return (start, stop),
            _ => {}
        };
        start += 1;
        counted_runes += 1;
    }

    (start, stop)
}

fn column_length(data: &[u8], runes: i16) -> usize {
    let mut eat = data.iter();
    let mut counted_runes = 0;
    let mut len: usize = 0;
    let mut units = 1;

    while counted_runes < runes as usize {
        let byten = eat.nth(units - 1);

        let bb: u8 = match byten {
            None => {
                return len;
            }
            Some(b) => *b,
        };

        units = match bb {
            bb if bb >> 7 == 0 => 1,
            bb if bb >> 5 == 0b110 => 2,
            bb if bb >> 4 == 0b1110 => 3,
            bb if bb >> 3 == 0b11110 => 4,
            _bb => {
                // TODO BAD ERROR HANDL
                panic!("Incorrect UTF-8 sequence");
                #[allow(unreachable_code)]
                0
            }
        };

        len += units;
        counted_runes += 1;
    }

    len
}
