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
* File created: 2024-05-05
* Last updated: 2024-05-05
*/

use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::ThreadRng;
use rand::Rng;

use crate::mocker::DEFAULT_MOCKED_FILENAME_LEN;

///
pub fn randomize_file_name() -> String {
    let mut path_name: String = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    path_name.push('_');
    path_name.push_str(
        Alphanumeric
            .sample_string(&mut rand::thread_rng(), DEFAULT_MOCKED_FILENAME_LEN)
            .as_str(),
    );
    path_name
}

///
pub fn mock_bool<'a> (rng: &'a mut ThreadRng) -> String {
    rng.gen_bool(0.5).to_string()
}

///
pub fn mock_float<'a>(rng: &'a mut ThreadRng) -> String {
    rng.gen_range(-1_000.0..=1_000.0).to_string()
}

///
pub fn mock_integer(rng: &mut ThreadRng) -> String {
    rng.gen_range(-1_000..=1_000).to_string()
}

///
pub fn mock_string(len: usize, rng: &mut ThreadRng) -> String {
    Alphanumeric.sample_string(rng, len)
}

