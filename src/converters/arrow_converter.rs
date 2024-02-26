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

use std::fs::File;
use std::path::PathBuf;
use std::str::from_utf8_unchecked;
use std::sync::Arc;
use arrow::array::{ArrayBuilder, BooleanBuilder, Int32Builder, Int64Builder, PrimitiveArray, StringBuilder};
use crate::converters::{ColumnBuilder, Converter};
use crate::schema;
use crate::slicers::FnLineBreak;
use rayon::iter::IndexedParallelIterator;

pub(crate) struct Slice2Arrow {
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnLineBreak,
//    pub(crate) schema: FixedSchema,
    pub(crate) in_out_arrow: Vec<Box<in_out>>
//    pub(crate) master_arrow: MasterBuilder<'a>
}
/// Will this name win the Pulitzer Prize
pub(crate) struct in_out {
     in_slice:Box<[u8]>,
     out_builders:Vec<Box<dyn ArrayBuilder>>
//    builders: Vec<Box<dyn Sync + Send + 'a + crate::converters::arrow2_builder::ColumnBuilder>>
}

pub(crate) struct ArrowBuffers<'a> {
    builders: Vec<Box<dyn Sync + Send + 'a + ColumnBuilder>>
}


impl Converter for Slice2Arrow {
    fn set_line_break_handler(&mut self, fnl: FnLineBreak) {
        self.fn_line_break = fnl;
    }
    fn get_line_break_handler(&self) -> FnLineBreak {
        self.fn_line_break
    }

    fn process<'a>(&mut self, slices: Vec<&[u8]>) -> usize {
        let mut bytes_processed: usize = 0;
        let io:Vec<in_out> = Vec::new();
        //let a:&[u8]=    slices.get(0).unwrap();
        for slice in slices.iter() {

        }

//        let arc_builders = Arc::new(&self.builders);
//        let chunks:Chunk<?>;
//        let arc_masterbuilder = Arc::new(&self.master_builder);
        // TODO declare a array of chunks[slices.len]  , pass it on to the parse_slice funktion
//        slices.par_iter().enumerate().for_each(|(i, n)| {
//            let arc_builders_clone = Arc::clone(&arc_builders);
//            parse_slice(i, n, &arc_builders_clone);
//        });

        bytes_processed
    }
}


fn parse_slice(i:usize, n: &&[u8], builders: &Arc<&Vec<Box<dyn ArrayBuilder>>>)  {


    println!("index {} {}", i, n.len());
//    let builders: Vec<Box<dyn ColumnBuilder>>;
    let start_byte_pos=0;

    // TODO make safe/unsafe configurable
    let text:&str = unsafe {
        from_utf8_unchecked(&n)
    };

    let offset=0;
}
/// All threads
//pub(crate) struct in_out {
//    in_slice:Box<[u8]>,
//    out_builders:Vec<Box<dyn ArrayBuilder>>
//    builders: Vec<Box<dyn Sync + Send + 'a + crate::converters::arrow2_builder::ColumnBuilder>>
//}


pub fn in_out_instance_factory<'a>(schema_path: &PathBuf,instances:i16) -> Vec<Box<in_out>> {
    let schema=schema::FixedSchema::from_path(schema_path.into());
    let antal_col=schema.num_columns();


    let mut in_out_instances : Vec<Box<in_out>>=Vec::new();

    for i in 1..instances {
        let mut buildersmut: Vec<Box<dyn ArrayBuilder >>=Vec::with_capacity(antal_col);
        for val in schema.iter() {
            match val.dtype().as_str() {
                "i32" => buildersmut.push(Box::new(Int32Builder::new())),
                "i64" => buildersmut.push(Box::new(Int64Builder::new())),
                "boolean" => buildersmut.push(Box::new(BooleanBuilder::new())),
                "utf8" => buildersmut.push(Box::new(StringBuilder::new())),

                &_ => {}
            };
        }
        let mut in_out_instance:in_out = in_out { in_slice: Box::new([]), out_builders: buildersmut };
        in_out_instances.push(Box::new( in_out_instance));

    }

    in_out_instances
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
