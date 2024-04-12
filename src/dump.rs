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
use std::fs;
use arrow_array::{Array, Datum};
use arrow_array::cast::AsArray;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub(crate) fn dump(infile: fs::File) -> usize {

    let builder = ParquetRecordBatchReaderBuilder::try_new(infile).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();

    loop {
        let record_batch_result = reader.next();
        match record_batch_result {
            None => { break }
            Some(r) => {
                let record_batch = r.unwrap();
                println!("Read {} records.", record_batch.num_rows());

                for rnr in 0..record_batch.num_rows() {
                    let slice=record_batch.slice(rnr,1);
                    for c in slice.columns() {
                        print!("{:?}",c.get())
                    }

//                    println!("ROW {}  {:?}",rnr,slice)
//                    for col in slice.columns() {
//                        println!("col={}", col.data_type());
//                        //println!("col={:#?}", col.get());
//                    }

                }
            }
        }
    }
    0
}
