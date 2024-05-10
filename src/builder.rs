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
* File created: 2024-05-07
* Last updated: 2024-05-08
*/

use arrow::array::{ArrayRef, BooleanBuilder};
use log::warn;

use std::fmt::Debug;
use std::sync::Arc;

use crate::parser::BooleanParser;

///
pub(crate) trait ColumnBuilder: Debug {
    fn parse_and_push_bytes(&mut self, bytes: &[u8]);
    fn runes(&self) -> usize;
    fn finish(&mut self) -> (&str, ArrayRef);
}

///
#[derive(Debug)]
pub(crate) struct BooleanColumnBuilder {
    inner: BooleanBuilder,
    runes: usize,
    name: String,
    parser: BooleanParser,
}

///
impl BooleanColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: BooleanParser) -> Self {
        Self {
            inner: BooleanBuilder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for BooleanColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(b) => self.inner.append_value(b),
            Err(e) => {
                warn!("Could not convert utf-8 text to bool: {:?}", e);
                self.inner.append_null();
            }
        };
    }

    ///
    fn runes(&self) -> usize {
        self.runes
    }

    ///
    fn finish(&mut self) -> (&str, ArrayRef) {
        (&self.name, Arc::new(self.inner.finish()) as ArrayRef)
    }
}

///
#[derive(Debug)]
pub(crate) struct Float16Builder {
    pub runes: usize,
    pub name: String,
}

///
#[derive(Debug)]
pub(crate) struct Float32Builder {
    pub runes: usize,
    pub name: String,
}

///
#[derive(Debug)]
pub(crate) struct Float64Builder {
    pub runes: usize,
    pub name: String,
}

///
#[derive(Debug)]
pub(crate) struct Int16Builder {
    pub runes: usize,
    pub name: String,
}

///
#[derive(Debug)]
pub(crate) struct Int32Builder {
    pub runes: usize,
    pub name: String,
}

///
#[derive(Debug)]
pub(crate) struct Int64Builder {
    pub runes: usize,
    pub name: String,
}

///
#[derive(Debug)]
pub(crate) struct StringBuilder {
    pub runes: usize,
    pub name: String,
}
