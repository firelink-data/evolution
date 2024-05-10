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
// File created: 2024-05-07
// Last updated: 2024-05-10
//

use arrow::array::{ArrayRef, BooleanBuilder, Float16Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder as Utf8Builder, LargeStringBuilder as LargeUtf8Builder};
use log::warn;

use std::fmt::Debug;
use std::sync::Arc;

use crate::parser::{BooleanParser, Float16Parser, Float32Parser, Float64Parser, Int16Parser, Int32Parser, Int64Parser, Utf8Parser, LargeUtf8Parser};

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
pub(crate) struct Float16ColumnBuilder {
    inner: Float16Builder,
    runes: usize,
    name: String,
    parser: Float16Parser,
}

impl Float16ColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: Float16Parser) -> Self {
        Self {
            inner: Float16Builder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for Float16ColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(f) => self.inner.append_value(f),
            Err(e) => {
                warn!("Could not convert utf-8 text to f16: {:?}", e);
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
pub(crate) struct Float32ColumnBuilder {
    inner: Float32Builder,
    runes: usize,
    name: String,
    parser: Float32Parser,
}

impl Float32ColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: Float32Parser) -> Self {
        Self {
            inner: Float32Builder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for Float32ColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(f) => self.inner.append_value(f),
            Err(e) => {
                warn!("Could not convert utf-8 text to f32: {:?}", e);
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
pub(crate) struct Float64ColumnBuilder {
    inner: Float64Builder,
    runes: usize,
    name: String,
    parser: Float64Parser,
}

impl Float64ColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: Float64Parser) -> Self {
        Self {
            inner: Float64Builder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for Float64ColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(f) => self.inner.append_value(f),
            Err(e) => {
                warn!("Could not convert utf-8 text to f64: {:?}", e);
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
pub(crate) struct Int16ColumnBuilder {
    inner: Int16Builder,
    runes: usize,
    name: String,
    parser: Int16Parser,
}

impl Int16ColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: Int16Parser) -> Self {
        Self {
            inner: Int16Builder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for Int16ColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(f) => self.inner.append_value(f),
            Err(e) => {
                warn!("Could not convert utf-8 text to i16: {:?}", e);
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
pub(crate) struct Int32ColumnBuilder {
    inner: Int32Builder,
    runes: usize,
    name: String,
    parser: Int32Parser,
}

impl Int32ColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: Int32Parser) -> Self {
        Self {
            inner: Int32Builder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for Int32ColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(f) => self.inner.append_value(f),
            Err(e) => {
                warn!("Could not convert utf-8 text to i32: {:?}", e);
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
pub(crate) struct Int64ColumnBuilder {
    inner: Int64Builder,
    runes: usize,
    name: String,
    parser: Int64Parser,
}

impl Int64ColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: Int64Parser) -> Self {
        Self {
            inner: Int64Builder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for Int64ColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(f) => self.inner.append_value(f),
            Err(e) => {
                warn!("Could not convert utf-8 text to i64: {:?}", e);
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
pub(crate) struct Utf8ColumnBuilder {
    inner: Utf8Builder,
    runes: usize,
    name: String,
    parser: Utf8Parser,
}

impl Utf8ColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: Utf8Parser) -> Self {
        Self {
            inner: Utf8Builder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for Utf8ColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(f) => self.inner.append_value(f),
            Err(e) => {
                warn!("Could not convert parse byte as utf-8: {:?}", e);
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
pub(crate) struct LargeUtf8ColumnBuilder {
    inner: LargeUtf8Builder,
    runes: usize,
    name: String,
    parser: LargeUtf8Parser,
}

impl LargeUtf8ColumnBuilder {
    ///
    pub fn new(runes: usize, name: String, parser: LargeUtf8Parser) -> Self {
        Self {
            inner: LargeUtf8Builder::new(),
            runes,
            name,
            parser,
        }
    }
}

///
impl ColumnBuilder for LargeUtf8ColumnBuilder {
    ///
    fn parse_and_push_bytes(&mut self, bytes: &[u8]) {
        match self.parser.parse(bytes) {
            Ok(f) => self.inner.append_value(f),
            Err(e) => {
                warn!("Could not convert parse byte as utf-8: {:?}", e);
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
