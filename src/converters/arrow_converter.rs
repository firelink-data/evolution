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
* Last updated: 2023-11-21
*/

use std::fmt::Debug;
use std::fs::File;
use std::path::PathBuf;
use std::str::from_utf8_unchecked;
use rayon::prelude::*;
use rayon::iter::IndexedParallelIterator;
use std::sync::Arc;
use arrow::array::{ArrayBuilder, BooleanBuilder, Int32Builder, Int64Builder, PrimitiveArray, StringBuilder};
use rayon::iter::plumbing::{bridge, Producer};
use crate::converters::{ColumnBuilder, Converter};
use crate::converters::arrow2_converter::MasterBuilder;
use crate::schema;
use crate::slicers::FnLineBreak;
use rayon::iter::IntoParallelRefIterator;
use rayon::prelude::*;

pub(crate) struct Slice2Arrow<'a> {
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnLineBreak<'a>,
    pub(crate) masterbuilders:  MasterBuilders
}

pub(crate) struct MasterBuilders {
     builders: Vec<Vec<Box<dyn  Sync + Send   + ArrayBuilder>>>
}

unsafe impl Send for MasterBuilders {}
unsafe impl Sync for MasterBuilders {}

impl MasterBuilders {

    pub fn builders_factory<'a>(schema_path: PathBuf, instances: i16) -> Self {
        let schema = schema::FixedSchema::from_path(schema_path.into());
        let antal_col = schema.num_columns();
//    let in_out_instances:&'a mut Vec<InOut<'a>>=    let  in_out_arrow:& mut Vec<InOut> = &mut vec![];
//    ;
        let mut builders:Vec<Vec<Box<dyn ArrayBuilder + Sync + Send>>>=Vec::new();

//    let mut in_out_instances : Vec<InOut<'a>>=Vec::new();

        for i in 1..instances {
            let mut buildersmut:  Vec<Box<dyn ArrayBuilder + Sync + Send>> =  Vec::with_capacity(antal_col);
            for val in schema.iter() {
                match val.dtype().as_str() {
                    "i32" => buildersmut.push(Box::new(Int32Builder::new())),
                    "i64" => buildersmut.push(Box::new(Int64Builder::new())),
                    "boolean" => buildersmut.push(Box::new(BooleanBuilder::new())),
                    "utf8" => buildersmut.push(Box::new(StringBuilder::new())),

                    &_ => {}
                };
            }
            builders.push(buildersmut);
//            let in_out_instance: InOut = InOut { /*in_slice: &mut [],*/ out_builders:  buildersmut };
//            in_out_instances.push(in_out_instance);
        }
        MasterBuilders { builders }
    }
}


impl<'a> Converter<'a> for Slice2Arrow<'a> {
    fn set_line_break_handler(& mut self, fnl: FnLineBreak<'a>) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(& self) -> FnLineBreak<'a> {
        self.fn_line_break
    }

    fn process(& mut  self,  slices: Vec<&'a [u8]>) -> usize {
        let mut bytes_processed: usize = 0;

//        let  in_out_arrow: Vec<slice<'a>> = vec![];

        let arc_slices = Arc::new(& slices);
        self.masterbuilders.builders.par_iter().enumerate().for_each(|(i, n)| {

            let arc_slice_clone = Arc::clone(&arc_slices);
            parse_slice(i, arc_slice_clone.get(i).unwrap(),n);

        });

        bytes_processed
    }
}


fn
parse_slice(i:usize, n: &&[u8], builders: &Vec<Box<dyn ArrayBuilder +Send + Sync>>)  {


    println!("index {} {}", i, n.len());
//    let builders: Vec<Box<dyn ColumnBuilder>>;
    let start_byte_pos=0;

    // TODO make safe/unsafe configurable
    let text:&str = unsafe {
        from_utf8_unchecked(&n)
    };

    let offset=0;
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




