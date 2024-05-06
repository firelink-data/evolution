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
* File created: 2024-02-05
* Last updated: 2024-05-07
*/

use std::error;
use std::fmt;
use std::result;

/// Generic result type which allows for dynamic dispatch of our custom error variants.
pub(crate) type Result<T> = result::Result<T, Box<dyn error::Error>>;

/// Error type used during execution when something goes wrong.
#[derive(Debug)]
pub(crate) struct ExecutionError;

impl error::Error for ExecutionError {}
impl fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Execution failed, please refer to any logged error messages or the stack-trace for debugging.")
    }
}

/// Error type used during setup of either [`Mocker`] or [`Converter`] if any
/// required values are invalid or missing.
#[derive(Debug)]
pub(crate) struct SetupError;

impl error::Error for SetupError {}
impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Setup failed, please refer to any logged error messages or the stack-trace for debugging.")
    }
}

#[cfg(test)]
mod tests_error {
    use super::*;

    #[test]
    fn test_execution_error() {
        assert_eq!(
            "Execution failed, please refer to any logged error messages or the stack-trace for debugging.",
            ExecutionError.to_string(),
        );
    }

    #[test]
    fn test_setup_error() {
        assert_eq!(
            "Setup failed, please refer to any logged error messages or the stack-trace for debugging.",
            SetupError.to_string(),
        );
    }
}
