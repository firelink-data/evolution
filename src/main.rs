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
// File created: 2023-11-21
// Last updated: 2024-05-15
//

#![cfg_attr(feature = "nightly", allow(internal_features))]
#![cfg_attr(feature = "nightly", feature(str_internals))]

use clap::Parser;
#[cfg(debug_assertions)]
use log::debug;
use log::{error, info};

mod builder;
#[cfg(feature = "rayon")]
mod chunked;
mod cli;
mod converter;
mod datatype;
mod error;
mod logger;
mod mocker;
mod mocking;
mod parser;
mod schema;
mod slicer;
mod threads;
mod writer;

use cli::Cli;

/// Run the evolution program, parsing any CLI arguments using [`clap`].
fn main() {
    let cli = Cli::parse();

    match logger::try_init_logging() {
        Ok(_) => {
            #[cfg(debug_assertions)]
            debug!("Logging setup ok!")
        }
        Err(e) => error!("Could not set up env logging: {:?}", e),
    };

    match cli.run() {
        Ok(_) => info!("All done! Bye. 👋🥳"),
        Err(e) => error!("{}", e),
    }
}
