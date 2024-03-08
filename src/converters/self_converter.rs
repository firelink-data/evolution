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

use std::fs::File;
use rayon::iter::IntoParallelRefIterator;
use crate::converters::Converter;
use crate::slicers::{FnLineBreak};
use rayon::prelude::*;
use std::io::{Write};
use rayon::prelude::*;


pub struct SampleSliceAggregator<'a> {
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnLineBreak<'a>,
}

impl<'a> Converter<'a> for SampleSliceAggregator<'a> {
    fn set_line_break_handler(& mut self, fnl: FnLineBreak<'a>) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(& self) -> FnLineBreak<'a> {
        self.fn_line_break
    }

    fn process(& mut self, slices: Vec<&'a [u8]>) -> usize {
        let mut bytes_processed: usize = 0;

        slices.par_iter().enumerate().for_each(|(i, n)| println!("index {} {}", i, n.len()));
        for val in slices {
            self.file_out.write_all(val).expect("dasd");
            let l = val.len();
            bytes_processed += l;
        }
        bytes_processed
    }
}

