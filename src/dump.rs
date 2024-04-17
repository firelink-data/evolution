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
use std::fs;

use arrow_array::{
    Array, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, StringArray, StringViewArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub(crate) fn dump(infile: fs::File) -> usize {
    let builder = ParquetRecordBatchReaderBuilder::try_new(infile).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();

    loop {
        let record_batch_result = reader.next();
        match record_batch_result {
            None => {
                break;
            }
            Some(r) => {
                let record_batch = r.unwrap();

                for rnr in 0..record_batch.num_rows() {
                    let sliced_record_batch = record_batch.slice(rnr, 1);
                    for cnr in 0..record_batch.columns().len() {
                        let s: String = match sliced_record_batch.column(cnr as usize).data_type() {
                            DataType::Null => ("Null").to_string(),
                            DataType::Boolean => "Not implemented".to_string(),
                            DataType::Int8 => {
                                let i8 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<Int8Array>()
                                    .unwrap();
                                i8.value(0).to_string()
                            }
                            DataType::Int16 => {
                                let i16 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<Int16Array>()
                                    .unwrap();
                                i16.value(0).to_string()
                            }
                            DataType::Int32 => {
                                let i32 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<Int32Array>()
                                    .unwrap();
                                i32.value(0).to_string()
                            }
                            DataType::Int64 => {
                                let i64 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap();
                                i64.value(0).to_string()
                            }
                            DataType::UInt8 => {
                                let u8 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<UInt8Array>()
                                    .unwrap();
                                u8.value(0).to_string()
                            }
                            DataType::UInt16 => {
                                let u16 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<UInt16Array>()
                                    .unwrap();
                                u16.value(0).to_string()
                            }
                            DataType::UInt32 => {
                                let u32 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<UInt32Array>()
                                    .unwrap();
                                u32.value(0).to_string()
                            }
                            DataType::UInt64 => {
                                let u64 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .unwrap();
                                u64.value(0).to_string()
                            }
                            DataType::Float16 => {
                                let f16 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<Float16Array>()
                                    .unwrap();
                                f16.value(0).to_string()
                            }
                            DataType::Float32 => {
                                let f32 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<Float32Array>()
                                    .unwrap();
                                f32.value(0).to_string()
                            }
                            DataType::Float64 => {
                                let f64 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<Float64Array>()
                                    .unwrap();
                                f64.value(0).to_string()
                            }
                            DataType::Timestamp(_, _) => {
                                ("NOT IMPLEMENTED").to_string().to_string()
                            }
                            DataType::Date32 => ("NOT IMPLEMENTED").to_string(),
                            DataType::Date64 => ("NOT IMPLEMENTED").to_string(),
                            DataType::Time32(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Time64(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Duration(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Interval(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Binary => ("NOT IMPLEMENTED").to_string(),
                            DataType::FixedSizeBinary(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::LargeBinary => ("NOT IMPLEMENTED").to_string(),
                            DataType::BinaryView => ("NOT IMPLEMENTED").to_string(),
                            DataType::Utf8 => {
                                let utf8 = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                utf8.value(0).to_string()
                            }
                            DataType::LargeUtf8 => {
                                let utf8large = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<LargeStringArray>()
                                    .unwrap();
                                utf8large.value(0).to_string()
                            }
                            DataType::Utf8View => {
                                let utf8view = sliced_record_batch
                                    .column(cnr as usize)
                                    .as_any()
                                    .downcast_ref::<StringViewArray>()
                                    .unwrap();
                                utf8view.value(0).to_string()
                            }
                            DataType::List(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::ListView(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::FixedSizeList(_, _) => ("NOT IMPLEMENTED").to_string(),
                            DataType::LargeList(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::LargeListView(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Struct(_) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Union(_, _) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Dictionary(_, _) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Decimal128(_, _) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Decimal256(_, _) => ("NOT IMPLEMENTED").to_string(),
                            DataType::Map(_, _) => ("NOT IMPLEMENTED").to_string(),
                            DataType::RunEndEncoded(_, _) => ("NOT IMPLEMENTED").to_string(),
                        };
                        print!("{}", s);
                    }
                    println!("");

                    //                        sliced_record_batch.column(cnr as usize).
                }
            }
        }
    }
    0
}
