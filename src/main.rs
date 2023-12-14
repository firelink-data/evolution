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
* Last updated: 2023-12-04
*/

use std::fs;
use clap::{value_parser, Arg, ArgAction, Command};
use log::SetLoggerError;
use crate::slicer::{find_last_nl, SampleSliceAggregator};

mod builder;
mod logging;
mod mock;
mod schema;
mod slicer;

///
fn main() -> Result<(), SetLoggerError> {
    logging::setup_log()?;

    let mut matches = Command::new("evolution")
        .author("Wilhelm Ågren <wilhelmagren98@gmail.com>")
        .version("0.2.1")
        .about(
            "🦖 Evolve your fixed length data files into Apache Arrow tables, fully parallelized!",
        )
        .arg(
            Arg::new("schema")
                .short('s')
                .long("schema")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("file")
                .short('f')
                .long("file")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("mock")
                .short('m')
                .long("mock")
                .requires("schema")
                .requires("file")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("n-rows")
                .short('n')
                .long("n-rows")
                .requires("mock")
                .action(ArgAction::Set)
                .default_value("1000")
                .value_parser(value_parser!(usize)),
        )
        .arg(
            Arg::new("slicer")
                .long("slicer")
                .requires("file")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("cores")
                .short('c')
                .long("cores")
                .requires("slicer")
                .action(ArgAction::Set)
                .default_value("8")
                .value_parser(value_parser!(usize)),
        )
        .get_matches();

    if matches.get_flag("mock") {
        mock::mock_from_schema(
            matches.remove_one::<String>("schema").unwrap(),
            matches.remove_one::<usize>("n-rows").unwrap(),
        );
    }

    if matches.get_flag("slicer") {
        let file_name =matches.remove_one::<String>("file").unwrap();

        let file = std::fs::File::open(&file_name).expect("bbb");
        let mut out_file_name  = file_name.clone().to_owned();
        out_file_name.push_str("SLICED");

        let file_out = fs::OpenOptions::new().create(true).append(true).open(&file_name ).expect("aaa");

        let saa: Box<SampleSliceAggregator> =Box::new(slicer::SampleSliceAggregator { file_out: file_out, fn_line_break: find_last_nl });

        slicer::slice_and_process(
            saa,
            file,
            matches.remove_one::<usize>("cores").unwrap() as i16,
        );
    }

    Ok(())
}
