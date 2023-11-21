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

use clap::Parser;
use log::{debug, error, info, warn, SetLoggerError};

mod builder;
mod cli;
mod logging;

use cli::CLIArgs;

///
fn main() -> Result<(), SetLoggerError> {
    let _args = CLIArgs::parse();
    let _ = match logging::init_logging() {
        Ok(()) => Ok(()),
        Err(e) => {
            error!("Could not initialize boxed logger, exiting!");
            Err(e)
        }
    };

    debug!("abcdefgh ijklm nopqr stuvw xyzåäö");
    info!("abcdefgh ijklm nopqr stuvw xyzåäö");
    warn!("abcdefgh ijklm nopqr stuvw xyzåäö");
    error!("abcdefgh ijklm nopqr stuvw xyzåäö");

    Ok(())
}
