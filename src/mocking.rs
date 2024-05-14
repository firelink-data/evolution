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
// File created: 2024-05-05
// Last updated: 2024-05-14
//

use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::ThreadRng;
use rand::Rng;

pub(crate) static MOCKED_FILENAME_LEN: usize = 8;
pub(crate) static MOCKED_FLOAT_SIZE: f32 = 1_000.0;
pub(crate) static MOCKED_INTEGER_SIZE: i32 = 1_000;

/// Randomly generate a string from the [`Alphanumeric`] distribution with desired length
/// [`MOCKED_FILENAME_LEN`].
pub fn randomize_file_name() -> String {
    let mut path_name: String = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    path_name.push('_');
    path_name.push_str(
        Alphanumeric
            .sample_string(&mut rand::thread_rng(), MOCKED_FILENAME_LEN)
            .as_str(),
    );
    path_name
}

/// Randomly generate a String `true` or `false` with exactly
/// 50% chance of either, sampled from a Bernoulli distribution.
pub fn mock_bool(rng: &mut ThreadRng) -> String {
    rng.gen_bool(0.5).to_string()
}

/// Uniformly sample a floating point number from a range and return it as a String.
pub fn mock_float(rng: &mut ThreadRng) -> String {
    rng.gen_range(-MOCKED_FLOAT_SIZE..=MOCKED_FLOAT_SIZE)
        .to_string()
}

/// Uniformly sample an integer number from a range and return it as a String.
pub fn mock_integer(rng: &mut ThreadRng) -> String {
    rng.gen_range(-MOCKED_INTEGER_SIZE..=MOCKED_INTEGER_SIZE)
        .to_string()
}

/// Uniformly sample from ASCII characters and numbers to create a String with length `len`.
pub fn mock_string(len: usize, rng: &mut ThreadRng) -> String {
    Alphanumeric.sample_string(rng, len)
}
