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
* File created: 2023-11-21
* Last updated: 2024-05-03
*/

use log::error;
use std::str::from_utf8_unchecked;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float16Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder};

use crate::schema::FixedSchema;

///
pub struct Builder {
    columns: Vec<Box<dyn ColumnBuilder>>,
}

impl Builder {
    pub fn from_schema(schema: &FixedSchema) -> Self {
        let mut columns: Vec<Box<dyn ColumnBuilder>> = Vec::with_capacity(schema.num_columns());
        for col in schema.iter() {
            let runes: usize = col.length();
            let name: &str = col.name().as_str();

            match col.dtype().as_str() {
                "bool" => columns.push(Box::new(BooleanBuilderHandler { builder: BooleanBuilder::new(), runes, name })),
                _ => panic!(""),
            }
        }

        Self { columns }
    }
}

///
pub struct MasterBuilder {
    builders: Vec<Builder>,
}

impl MasterBuilder {

    ///
    pub fn from_schema(num_builders: usize, schema: &FixedSchema) -> Self {
        let builders = (0..num_builders)
            .map(|_| Builder::from_schema(schema))
            .collect::<Vec<Builder>>();

        Self { builders }
    }

    ///
    pub fn num_builders(&self) -> usize {
        self.builders.len()
    }
}

///
pub trait ColumnBuilder: Send + Sync {
    fn push_bytes(&mut self, data: &[u8]);
    fn finish(&mut self) -> (&str, ArrayRef);
}

///
pub struct BooleanBuilderHandler<'a> {
    pub builder: BooleanBuilder,
    pub runes: usize,
    pub name: &'a str,
}

impl<'a> ColumnBuilder for BooleanBuilderHandler<'a> {
    fn push_bytes(&mut self, data: &[u8]) {
        let text: &str = unsafe { from_utf8_unchecked(&data) };
        match text.parse::<bool>() {
            Ok(b) => self.builder.append_value(b),
            Err(e) => {
                error!("Could not convert utf-8 text as boolean: {:?}", e);
                self.builder.append_null();
            },
        }
    }

    fn finish(&mut self) -> (&str, ArrayRef) {
        (
            self.name,
            Arc::new(self.builder.finish()) as ArrayRef,
        )
    }
}

///
pub struct Float16BuilderHandler<'a> {
    pub builder: Float16Builder,
    pub runes: usize,
    pub name: &'a str,
}

impl<'a> ColumnBuilder for Float16BuilderHandler<'a> {
    fn push_bytes(&mut self, _data: &[u8]) {
        todo!();
    }

    fn finish(&mut self) -> (&str, ArrayRef) {
        todo!();
    }
}

///
pub struct Float32BuilderHandler<'a> {
    pub builder: Float32Builder,
    pub runes: usize,
    pub name: &'a str,
}

impl<'a> ColumnBuilder for Float32BuilderHandler<'a> {
    fn push_bytes(&mut self, _data: &[u8]) {
        todo!();
    }

    fn finish(&mut self) -> (&str, ArrayRef) {
        todo!();
    }
}

///
pub struct Float64BuilderHandler<'a> {
    pub builder: Float64Builder,
    pub runes: usize,
    pub name: &'a str,
}

impl<'a> ColumnBuilder for Float64BuilderHandler<'a> {
    fn push_bytes(&mut self, _data: &[u8]) {
        todo!();
    }

    fn finish(&mut self) -> (&str, ArrayRef) {
        todo!();
    }
}

///
pub struct Int16BuilderHandler<'a> {
    pub builder: Int16Builder,
    pub runes: usize,
    pub name: &'a str,
}

impl<'a> ColumnBuilder for Int16BuilderHandler<'a> {
    fn push_bytes(&mut self, data: &[u8]) {
        match atoi_simd::parse(data) {
            Ok(i) => self.builder.append_value(i),
            Err(e) => {
                error!("Could not parse byte slice as Int16: {:?}", e);
                self.builder.append_null();
            },
        }
    }

    fn finish(&mut self) -> (&str, ArrayRef) {
        (
            self.name,
            Arc::new(self.builder.finish()) as ArrayRef,
        )
    }
}

///
pub struct Int32BuilderHandler<'a> {
    pub builder: Int32Builder,
    pub runes: usize,
    pub name: &'a str,
}

impl<'a> ColumnBuilder for Int32BuilderHandler<'a> {
    fn push_bytes(&mut self, data: &[u8]) {
        match atoi_simd::parse(data) {
            Ok(i) => self.builder.append_value(i),
            Err(e) => {
                error!("Could not parse byte slice as Int32: {:?}", e);
                self.builder.append_null();
            },
        }
    }

    fn finish(&mut self) -> (&str, ArrayRef) {
        (
            self.name,
            Arc::new(self.builder.finish()) as ArrayRef,
        )
    }
}

///
pub struct Int64BuilderHandler<'a> {
    pub builder: Int64Builder,
    pub runes: usize,
    pub name: &'a str,
}

impl<'a> ColumnBuilder for Int64BuilderHandler<'a> {
    fn push_bytes(&mut self, data: &[u8]) {
        match atoi_simd::parse(data) {
            Ok(i) => self.builder.append_value(i),
            Err(e) => {
                error!("Could not parse byte slice as Int64: {:?}", e);
                self.builder.append_null();
            },
        }
    }

    fn finish(&mut self) -> (&str, ArrayRef) {
        (
            self.name,
            Arc::new(self.builder.finish()) as ArrayRef,
        )
    }
}

///
pub struct StringBuilderHandler<'a> {
    pub builder: StringBuilder,
    pub runes: usize,
    pub name: &'a str,
}

impl<'a> ColumnBuilder for StringBuilderHandler<'a> {
    fn push_bytes(&mut self, data: &[u8]) {
        let text: &str = unsafe { from_utf8_unchecked(&data) };
        match text.is_empty() {
            false => self.builder.append_value(text),
            true => {
                error!("Byte slice as utf-8 was empty!");
                self.builder.append_null();
            },
        }
    }

    fn finish(&mut self) -> (&str, ArrayRef) {
        (
            self.name,
            Arc::new(self.builder.finish()) as ArrayRef,
        )
    }
}

