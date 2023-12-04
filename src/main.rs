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

use clap::{value_parser, Arg, ArgAction, Command};
use log::SetLoggerError;

mod builder;
mod logging;
mod mock;
mod schema;

///
fn main() -> Result<(), SetLoggerError> {
    logging::setup_log()?;

    let mut matches = Command::new("evolution")
        .author("Wilhelm Ågren <wilhelmagren98@gmail.com>")
        .version("0.2.0")
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
                .requires("schema")
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("mock")
                .short('m')
                .long("mock")
                .requires("schema")
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
        .get_matches();

    if matches.get_flag("mock") {
        mock::mock_from_schema(
            matches.remove_one::<String>("schema").unwrap(),
            matches.remove_one::<usize>("n-rows").unwrap(),
        );
    }

    Ok(())
}
