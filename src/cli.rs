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

///
#[derive(Parser, Debug)]
pub struct CLIArgs {

    /// The fixed length file to parse.
    /// TODO: support multiple files at once.
    #[arg(short = 'f', long = "file", required = true)]
    pub file: String,

    /// The schema file to use for parsing the fixed length file.
    #[arg(short = 's', long = "schema", required = true)]
    pub schema: String,
}

///
impl CLIArgs {}

#[cfg(test)]
mod tests_cli {
    use super::*;

    #[test]
    #[should_panic]
    fn try_parse_panic() {
        CLIArgs::try_parse().unwrap();
    }
}
