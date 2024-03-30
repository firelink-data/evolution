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

use arrow2::error::Error;
use std::any::Any;
use std::fmt::{Debug, Pointer};
use std::fs::File;
use std::os::unix::raw::uid_t;
use std::path::PathBuf;
use std::slice::Iter;
use std::str::from_utf8_unchecked;
use rayon::prelude::*;
use rayon::iter::IndexedParallelIterator;
use std::sync::Arc;
use arrow::array::{ArrayBuilder, BooleanBuilder, Int32Builder, Int64Builder, PrimitiveArray, PrimitiveBuilder, StringBuilder};
use arrow::datatypes::Int32Type;
use rayon::iter::plumbing::{bridge, Producer};
use crate::converters::{ColumnBuilder, Converter};
use crate::converters::arrow2_converter::MasterBuilder;
use crate::schema;
use crate::slicers::FnLineBreak;
use rayon::iter::IntoParallelRefIterator;
use rayon::prelude::*;
use substring::Substring;

pub(crate) struct Slice2Arrow<'a> {
    pub(crate) file_out: File,
    pub(crate) fn_line_break: FnLineBreak<'a>,
    pub(crate) masterbuilders:  MasterBuilders
}

pub(crate) struct MasterBuilders {
      builders:  Vec<Vec<Box<dyn  Sync + Send   + ColumnBuilder>>>
}

unsafe impl Send for MasterBuilders {}
unsafe impl Sync for MasterBuilders {}

impl MasterBuilders {

    pub fn builders_factory<'a>(schema_path: PathBuf, instances: i16) -> Self {
        let schema = schema::FixedSchema::from_path(schema_path.into());
        let antal_col = schema.num_columns();
//    let in_out_instances:&'a mut Vec<InOut<'a>>=    let  in_out_arrow:& mut Vec<InOut> = &mut vec![];
//    ;
//        let mut builders:Vec<Vec<Box<dyn ArrayBuilder + Sync + Send>>>=Vec::new();
        let mut builders:Vec<Vec<Box<dyn ColumnBuilder + Sync + Send>>>=Vec::new();

//    let mut in_out_instances : Vec<InOut<'a>>=Vec::new();

        for i in 1..=instances {
            let mut buildersmut:  Vec<Box<dyn ColumnBuilder + Sync + Send>> =  Vec::with_capacity(antal_col);
            for val in schema.iter() {
                match val.dtype().as_str() {
                    "i32" => buildersmut.push(Box::new(HandlerInt32Builder { int32builder: Int32Builder::new(), runes_in_column: val.length() }   )),
                    "i64" => buildersmut.push(Box::new(HandlerInt64Builder { int64builder: Int64Builder::new(), runes_in_column: val.length() }   )),
                    "boolean" => buildersmut.push(Box::new( HandlerBooleanBuilder  { boolean_builder: BooleanBuilder::new(), runes_in_column: val.length() })),
                    "utf8" => buildersmut.push(Box::new( HandlerStringBuilder {string_builder: StringBuilder::new(), runes_in_column: val.length() })),

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
        self.masterbuilders.builders.par_iter_mut().enumerate().for_each(|(i, mut n)| {

            let arc_slice_clone = Arc::clone(&arc_slices);
            match arc_slice_clone.get(i) {
                None => {}
                Some(_) => {            parse_slice(i, arc_slice_clone.get(i).unwrap(),n);}
            }


        });

        bytes_processed
    }
}



fn
parse_slice(i:usize, n: &[u8], mut builders: &mut Vec<Box<dyn ColumnBuilder +Send + Sync>>)  {


    println!("index {} {}", i, n.len());
//    let builders: Vec<Box<dyn ColumnBuilder>>;
    let start_byte_pos=0;

    // TODO make safe/unsafe configurable
    let mut text:&str = unsafe {
        from_utf8_unchecked(&n)
    };
    print!("hhahah {}",&n[1]);
    let mut cursor:usize = 0;

    let bytelen:usize=0;
    for mut cb in builders {
         cursor=cb.parse_value(n);
        cursor=cursor-bytelen;
    }
//    println!("texten={}",text);
    let offset=0;
}

struct HandlerInt32Builder {
    int32builder: Int32Builder,
    runes_in_column: usize
}

impl ColumnBuilder for HandlerInt32Builder {
    fn parse_value(&mut self, data: &[u8]) ->usize
        where
            Self: Sized,
    {
        let columnLength:usize=columnLenght(self.runes_in_column, data, self.runes_in_column as i16);

        match atoi_simd::parse(&data[..columnLength]) {
            Ok(n) => {
                self.int32builder.append_value(n);
            }
            Err(_e) => {
                self.int32builder.append_null();
            }
        };
        columnLength
    }
    // todo fix below


}
struct HandlerInt64Builder {
    int64builder: Int64Builder,
    runes_in_column: usize
}
impl ColumnBuilder for HandlerInt64Builder {
    fn parse_value(&mut self, data: &[u8]) -> usize
        where
            Self: Sized,
    {
        let columnLength:usize=columnLenght(self.runes_in_column, data,self.runes_in_column as i16 );
        match atoi_simd::parse(&data[..columnLength]) {
            Ok(n) => {
                self.int64builder.append_value(n);
            }
            Err(_e) => {
                self.int64builder.append_null();
            }
        };
        // todo fix below
    columnLength
    }
}
// Might be better of to copy the actual data to array<str>[colnr]

fn columnLenght(cursor: usize, data: &[u8], runes: i16) -> usize {
//    let mut eat=data.iter().peekable();
    let mut eat=data.iter();

    let mut len:usize =0;
    let mut units=1;

//    for b in data {
//    while eat.peek().is_some() {
//       while eat.nth(units)? {
        let byten=    eat.nth(units);

    let bb:u8=match byten {
        None => {
            return len;
        }
        Some(b) => {
            *b
        }
    };

        while len< runes as usize {
        units = match bb {
            bb if bb >> 7 == 0 => 1,
            bb if bb >> 5 == 0b110 =>  2,
            bb if bb >> 4 == 0b1110 =>  3,
            bb if bb >> 3 == 0b11110 => 4,
            bb => {
// TODO BAD ERROR HANDL
                0
            }
        };

        len+=units;
    }
/*

                if *eat.next().unwrap() >> 7 == 0 {
                {
                    if eat.peek().is_none() {
                       len
                    }

                    if *eat.next().unwrap() >> 5 == 0b110 {
                        if eat.peek().is_none() {
                           len
                        }
                     if {
                            *eat.next().unwrap() >> 4 == 0b1110
                         {
                            if eat.peek().is_some() if { *eat.next().unwrap() >> 3 == 0b11110 {}

                        }
                    }
                }

            }

    }
*/
    len
}


struct HandlerStringBuilder {
    string_builder: StringBuilder,
    runes_in_column: usize
}
impl ColumnBuilder for HandlerStringBuilder {
    fn parse_value(&mut self, data: &[u8]) -> usize
        where
            Self: Sized,
    {
        let columnLength:usize=columnLenght(self.runes_in_column, data,self.runes_in_column as i16 );
// Me dont like ... what is the cost ? Could it be done once for the whole chunk ?
        let mut text:&str = unsafe {
            from_utf8_unchecked(&data)
        };

        match text[..columnLength].is_empty() {
            false => {
                self.string_builder.append_value(&text[..columnLength]);
            }
            true => {
                self.string_builder.append_null();
            }
        };
        // todo fix below
    columnLength
    }

}

struct HandlerBooleanBuilder {
    boolean_builder: BooleanBuilder,
    runes_in_column: usize
}


impl ColumnBuilder for HandlerBooleanBuilder {
    fn parse_value(&mut self, data: &[u8]) -> usize
        where
            Self: Sized,
    {
        let columnLength:usize=columnLenght(self.runes_in_column, data,self.runes_in_column as i16 );

        let mut text:&str = unsafe {
            from_utf8_unchecked(&data)
        };

        match text[..columnLength]. parse::<bool>() {
            Ok(n) => {
                self.boolean_builder.append_value(n);
            }
            Err(_e) => {
                self.boolean_builder.append_null();
            }
        };
        // todo fix below
    columnLength
    }



}


