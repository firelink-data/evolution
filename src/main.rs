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
* File created: 2023-11-21
* Last updated: 2024-02-28
*/

use clap::Parser;
use log::{debug, error, info};

mod cli;
mod error;
mod logger;
mod mocker;
mod schema;
use cli::Cli;
use crate::slicers::ChunkAndResidue;
use crate::slicers::old_slicer::{IN_MAX_CHUNKS, SLICER_IN_CHUNK_SIZE, SLICER_MAX_RESIDUE_SIZE};

mod converters;
mod slicers;
///
fn main() {
    let cli = Cli::parse();

    match logger::setup_log() {
        Ok(_) => debug!("Logging setup, ok!"),
        Err(e) => error!("Could not set up env logging: {:?}", e),
    };

    /// Effektiv med fixa buffrar men fult att allokeringen ligger här ...känns banalt.
    let  in_out_buffers:  & mut [ChunkAndResidue; IN_MAX_CHUNKS] = & mut [  ChunkAndResidue {chunk: Box::new(   [0_u8; SLICER_IN_CHUNK_SIZE]),residue: Box::new(  [0_u8; SLICER_MAX_RESIDUE_SIZE])},
        ChunkAndResidue {chunk: Box::new(  [0_u8; SLICER_IN_CHUNK_SIZE]),residue: Box::new(  [0_u8; SLICER_MAX_RESIDUE_SIZE])},
        ChunkAndResidue {chunk: Box::new(  [0_u8; SLICER_IN_CHUNK_SIZE]),residue: Box::new(  [0_u8; SLICER_MAX_RESIDUE_SIZE])}  ];


    match cli.run(in_out_buffers) {
        Ok(_) => info!("All done! Bye."),
        Err(e) => error!("Something went wrong during execution: {:?}", e),
    }

}