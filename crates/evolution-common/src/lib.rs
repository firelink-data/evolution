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
// File created: 2024-05-24
// Last updated: 2024-05-27
//

/// Get the number of bytes representing a newline character on a Windows system.
#[cfg(target_os = "windows")]
pub static NUM_BYTES_FOR_NEWLINE: usize = 2;
/// Get the number of bytes representing a newline character on a Unix system.
#[cfg(not(target_os = "windows"))]
pub static NUM_BYTES_FOR_NEWLINE: usize = 1;

/// Get the character representing a newline on a Windows system. This is called the
/// Carriage-Return Line-Feed (CR-LF) and is represented as the `\r\n` characters.
#[cfg(target_os = "windows")]
pub fn newline<'a>() -> &'a str {
    "\r\n"
}
/// Get the character representing a newline on a Unix system. This is called the line-feed (LF)
/// and is represented as the `\n` character.
#[cfg(not(target_os = "windows"))]
pub fn newline<'a>() -> &'a str {
    "\n"
}

pub mod datatype;
pub mod error;
pub mod thread;
