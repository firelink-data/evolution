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
// File created: 2024-05-07
// Last updated: 2024-05-15
//

#[cfg(debug_assertions)]
use log::debug;
use log::warn;

pub(crate) fn get_available_threads(mut n_wanted_threads: usize) -> usize {
    let n_logical_threads: usize = num_cpus::get();

    if n_wanted_threads > n_logical_threads {
        warn!(
            "You specified to use {} threads, but your CPU only has {} logical threads.",
            n_wanted_threads, n_logical_threads,
        );
        warn!(
            "Will instead use all of the systems available logical threads ({}).",
            n_logical_threads,
        );
        n_wanted_threads = n_logical_threads;
    };

    #[cfg(debug_assertions)]
    debug!("Executing using {} logical threads.", n_wanted_threads);

    n_wanted_threads
}
