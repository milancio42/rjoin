extern crate rjoin;
extern crate csv_core;

use std::fs::File;
use std::error::Error;
use std::io;

use rjoin::join::{JoinOptions, join, header};
use rjoin::args::Args;
use rjoin::record::{RecordBuilder, GroupBuilder};
use rjoin::reader::ReaderBuilder;
use rjoin::printer::KeyFirst;
use csv_core::Terminator;

// stolen from BurntSushi's brilliant ripgrep crate
macro_rules! eprintln {
    ($($tt:tt)*) => {{
        use std::io::Write;
        let _ = writeln!(&mut ::std::io::stderr(), $($tt)*);
    }}
}

fn main() {
    match Args::parse().and_then(run) {
        Ok(_) => {},
        Err(e) => eprintln!("error: {}", e),
    }
}

fn run(args: Args) -> Result<(), Box<Error>> {
    let file0 = File::open(args.left_path())?;
    let file1 = File::open(args.right_path())?;

    let mut rdr0 = ReaderBuilder::default().delimiter(args.in_left_delimiter())
                                           .terminator(Terminator::Any(args.in_left_terminator()))
                                           .from_reader(file0);
    let mut rdr1 = ReaderBuilder::default().delimiter(args.in_right_delimiter())
                                           .terminator(Terminator::Any(args.in_left_terminator()))
                                           .from_reader(file1);

    let mut rec0 = RecordBuilder::default().fields_cap(8 * (1<<10))
                                           .key_fields_cap(8 * (1<<10))
                                           .non_key_fields_cap(8 * (1<<10))
                                           .build()?;
    let mut rec1 = RecordBuilder::default().fields_cap(8 * (1<<10))
                                           .key_fields_cap(8 * (1<<10))
                                           .non_key_fields_cap(8 * (1<<10))
                                           .build()?;

    let printer = KeyFirst::new(args.out_delimiter(), args.out_terminator());
    let opts = JoinOptions::new(args.show_left(), args.show_right(), args.show_both());
    let mut out = io::BufWriter::new(io::stdout());

    if args.header() {
        let _ = header(&mut rdr0, &mut rdr1, &mut rec0, &mut rec1, &mut out, printer)?;
    }

    rec0.clear();
    rec1.clear();
        
    let mut g0 = GroupBuilder::default().fields_cap(8 * (1<<10))
                                        .first_key_fields_cap(8 * (1<<10))
                                        .non_key_fields_cap(8 * (1<<10))
                                        .from_record(rec0);
    
    let mut g1 = GroupBuilder::default().fields_cap(8 * (1<<10))
                                        .first_key_fields_cap(8 * (1<<10))
                                        .non_key_fields_cap(8 * (1<<10))
                                        .from_record(rec1);
    let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, printer, opts)?;
    Ok(())
}
