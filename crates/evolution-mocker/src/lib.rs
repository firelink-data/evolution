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
// File created: 2024-05-25
// Last updated: 2024-05-27
//

use evolution_common::datatype::DataType;
use evolution_schema::column::FixedColumn;
use faker_rand::en_us::names::FirstName;
use rand::Rng;
use rand::rngs::ThreadRng;

pub static MOCKED_F16_MAX: f32 = 256.0;
pub static MOCKED_F32_MAX: f32 = 1_000_000.0;
pub static MOCKED_F64_MAX: f64 = 1_000_000_000.0;
pub static MOCKED_I16_MAX: i16 = 10_000;
pub static MOCKED_I32_MAX: i32 = 1_000_000;
pub static MOCKED_I64_MAX: i64 = 1_000_000_000;

///
pub fn mock_column(column: &FixedColumn, rng: &mut ThreadRng) -> String {
    match column.dtype() {
        DataType::Boolean => mock_bool(rng),
        DataType::Float16 => mock_f16(rng),
        DataType::Float32 => mock_f32(rng),
        DataType::Float64 => mock_f64(rng),
        DataType::Int16 => mock_i16(rng),
        DataType::Int32 => mock_i32(rng),
        DataType::Int64 => mock_i64(rng),
        DataType::Utf8 => mock_utf8(rng),
        DataType::LargeUtf8 => mock_utf8(rng),
    }
}

///
fn mock_bool(rng: &mut ThreadRng) -> String {
    rng.gen_bool(0.5).to_string()
}

///
fn mock_f16(rng: &mut ThreadRng) -> String {
    rng.gen_range(-MOCKED_F16_MAX..=MOCKED_F16_MAX).to_string()
}

///
fn mock_f32(rng: &mut ThreadRng) -> String {
    rng.gen_range(-MOCKED_F32_MAX..=MOCKED_F32_MAX).to_string()
}

///
fn mock_f64(rng: &mut ThreadRng) -> String {
    rng.gen_range(-MOCKED_F64_MAX..=MOCKED_F64_MAX).to_string()
}

///
fn mock_i16(rng: &mut ThreadRng) -> String {
    rng.gen_range(-MOCKED_I16_MAX..=MOCKED_I16_MAX).to_string()
}

///
fn mock_i32(rng: &mut ThreadRng) -> String {
    rng.gen_range(-MOCKED_I32_MAX..=MOCKED_I32_MAX).to_string()
}

///
fn mock_i64(rng: &mut ThreadRng) -> String {
    rng.gen_range(-MOCKED_I64_MAX..=MOCKED_I64_MAX).to_string()
}

///
fn mock_utf8(rng: &mut ThreadRng) -> String {
    rng.gen::<FirstName>().to_string()
}

pub mod mocker;
