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

use arrow2::array::MutablePrimitiveArray;
use arrow2::types::NativeType;

use crate::converters::arrow2_builder::ColumnBuilder;

/*

  "bool" => Ok(DataType::Boolean),
           "boolean" => Ok(DataType::Boolean),
           "i16" => Ok(DataType::Int16),
           "i32" => Ok(DataType::Int32),
           "i64" => Ok(DataType::Int64),
           "f16" => Ok(DataType::Float16),
           "f32" => Ok(DataType::Float32),
           "f64" => Ok(DataType::Float64),
           "utf8" => Ok(DataType::Utf8),
           "string" => Ok(DataType::Utf8),
           "lutf8" => Ok(DataType::LargeUtf8),
           "lstring" => Ok(DataType::LargeUtf8),

*/

pub(crate) struct ColumnBuilderType<T1: NativeType> {
    pub rows: MutablePrimitiveArray<T1>,
}

impl ColumnBuilder for ColumnBuilderType<i32> {
    fn parse_value(&mut self, name: &str)
    where
        Self: Sized,
    {
        match name.parse::<i32>() {
            Ok(n) => {
                self.rows.push(Some(n));
                n
            }
            Err(_e) => {
                self.nullify();
                0
            }
        };
    }

    fn finish_column(&mut self)
    where
        Self: Sized,
    {
        todo!()
    }

    fn nullify(&mut self)
    where
        Self: Sized,
    {
        self.rows.push(None);
    }

    fn lenght_in_chars(&mut self) -> i16 {
        todo!()
    }
}

impl ColumnBuilder for ColumnBuilderType<i64> {
    fn parse_value(&mut self, name: &str)
    where
        Self: Sized,
    {
        match name.parse::<i64>() {
            Ok(n) => {
                self.rows.push(Some(n));
                n
            }
            Err(_e) => {
                self.nullify();
                0
            }
        };
    }

    fn finish_column(&mut self)
    where
        Self: Sized,
    {
        todo!()
    }

    fn nullify(&mut self)
    where
        Self: Sized,
    {
        self.rows.push(None);
    }

    fn lenght_in_chars(&mut self) -> i16 {
        todo!()
    }
}
