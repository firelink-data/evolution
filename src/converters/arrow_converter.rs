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
* File created: 2023-11-21
* Last updated: 2023-11-21
*/

use std::path::PathBuf;
use arrow2::datatypes::DataType;
use arrow::array::{ArrayBuilder, Int32Builder, Int64Builder, PrimitiveArray};
use arrow::datatypes::ArrowNativeType;
use crate::converters::ColumnBuilder;
use crate::schema;
use arrow::array::PrimitiveBuilder;
use arrow::array::types::ArrowPrimitiveType;


pub fn builder_factory<'a>(schema_path: &PathBuf) -> Vec<Box<dyn ArrayBuilder>> {
    let schema=schema::FixedSchema::from_path(schema_path.into());
    let antal_col=schema.num_columns();

    let mut buildersmut: Vec<Box<dyn ArrayBuilder >>=Vec::with_capacity(antal_col);


    for val in schema.iter() {
        match val.dtype().as_str() {
            "i32" => buildersmut.push(Box::new(Int32Builder::new() )),
            "i64" => buildersmut.push(Box::new(Int64Builder::new() )),

            &_ => {}
        };
    }

    let builders = buildersmut;
    builders
}


impl ColumnBuilder for Int32Builder {
    fn parse_value(&mut self, name: &str)
        where
            Self: Sized,
    {
        match name.parse::<i32>() {
            Ok(n) => {
                self.append_value(n);
                n
            }
            Err(_e) => {
//                self.nullify();
                0
            }
        };
    }


    fn lenght_in_chars(&mut self) -> i16 {
        todo!()
    }
}

impl ColumnBuilder for Int64Builder {
    fn parse_value(&mut self, name: &str)
        where
            Self: Sized,
    {
        match name.parse::<i64>() {
            Ok(n) => {
                self.append_value(n);
                n
            }
            Err(_e) => {
//                self.nullify();
                0
            }
        };
    }

    fn lenght_in_chars(&mut self) -> i16 {
        todo!()
    }
}
