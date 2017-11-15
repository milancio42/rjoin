extern crate rollbuf;
extern crate csvroll;
extern crate rjoin;
#[macro_use]
extern crate clap;

mod args;

use std::fs::File;
use std::error::Error;
use std::io;

use args::Args;
use rollbuf::RollBuf;
use csvroll::index_builder::IndexBuilder;
use csvroll::parser::Parser;
use rjoin::join::{JoinOptions, join, head};
use rjoin::printer::KeyFirst;
use rjoin::csv::basic::{FirstRec, Group};

fn main() {
    match Args::parse().and_then(run) {
        Ok(_) => {},
        Err(e) => eprintln!("error: {}", e),
    }
}

fn run(args: Args) -> Result<(), Box<Error>> {
    const INBUF_CAP: usize = 4 * (1<<12);
    const OUTBUF_CAP: usize = 4 * (1<<14);
    let file0 = File::open(args.left_path())?;
    let file1 = File::open(args.right_path())?;

    let buf0 = RollBuf::with_capacity(INBUF_CAP, file0);
    let buf1 = RollBuf::with_capacity(INBUF_CAP, file1);
    let idx_builder0 = IndexBuilder::new(args.in_left_delimiter(), args.in_left_terminator());
    let idx_builder1 = IndexBuilder::new(args.in_right_delimiter(), args.in_right_terminator());
    let parser0 = Parser::from_parts(buf0, idx_builder0);
    let parser1 = Parser::from_parts(buf1, idx_builder1);
    let printer = KeyFirst::from_parts(
        args.out_delimiter(),
        args.out_terminator(),
        args.left_key().to_owned(),
        args.right_key().to_owned(),
    );
    let mut out = io::BufWriter::with_capacity(OUTBUF_CAP, io::stdout());
    let opts = JoinOptions::from_options(args.show_left(), args.show_right(), args.show_both());

    let (parser0, parser1) = if args.header() {
        let mut first_rec0 = FirstRec::init(parser0).unwrap();
        let mut first_rec1 = FirstRec::init(parser1).unwrap();
        head(&mut first_rec0, &mut first_rec1, &mut out, printer.clone(), opts)?;
        (first_rec0.into_inner(), first_rec1.into_inner())
    } else {
        (parser0, parser1)
    };

    let mut group0 = Group::init(parser0, args.left_key().to_owned())?;
    let mut group1 = Group::init(parser1, args.right_key().to_owned())?;
    join(&mut group0, &mut group1, &mut out, printer, opts)?;
    Ok(())
}
