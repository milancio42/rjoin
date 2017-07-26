use super::record::{Group, RecIter};
use super::reader::Reader;
use std::io;
use std::cmp::Ordering::{Less, Greater, Equal};
use std::error::Error;

pub struct JoinOptions {
    show_left: bool,
    show_right: bool,
    show_both: bool,
    delimiter: u8,
    terminator: u8,
}

pub fn join<R0,R1,W>(
    rdr0: &mut Reader<R0>,
    rdr1: &mut Reader<R1>,
    g0: &mut Group,
    g1: &mut Group,
    w: &mut W,
    opts: JoinOptions,
) -> Result<bool, Box<Error>>
    where R0: io::Read,
          R1: io::Read,
          W: io::Write
{
    let mut ord = Equal;
    let mut l = true;
    let mut r = true;
    loop {
        match ord {
            Less => {
                l = rdr0.read_group(g0)?;
            }
            Greater => {
                r = rdr1.read_group(g1)?;
            }
            Equal => {
                l = rdr0.read_group(g0)?;
                r = rdr1.read_group(g1)?;
            }
        }
        ord = match (l, r) {
            (true, true) => {
                let key_ord = g0.cmp_keys(g1);
                match key_ord {
                    Less => {
                        if opts.show_left {
                            print_left(w, opts.delimiter, opts.terminator, g0)?;
                        }
                    }
                    Greater => {
                        if opts.show_right {
                            print_right(w, opts.delimiter, opts.terminator, g1)?;
                        }
                    }
                    Equal => {
                        if opts.show_both {
                            print_both(w, opts.delimiter, opts.terminator, g0, g1)?;
                        }
                    }
                }
                key_ord
            }
            (true, false) => {
                if opts.show_left {
                    print_left(w, opts.delimiter, opts.terminator, g0)?;
                }
                Less
            }
            (false, true) => {
                if opts.show_right {
                    print_right(w, opts.delimiter, opts.terminator, g1)?;
                }
                Greater
            }
            (false, false) => return Ok(true),
        }
    }
}
                
#[inline]
fn print_left<W:io::Write>(
    w: &mut W,
    delimiter: u8,
    terminator: u8,
    g: &Group
) -> Result<(), Box<Error>> {
    let mut is_first: bool;
    for (rf, rfe) in g.non_key_iter() {
        is_first = true;
        
        for f in g.first_key_iter() {
            if !is_first {
                w.write_all(&[delimiter])?;
            } else {
                is_first = false;
            }
            w.write_all(f)?;
        }
        for f in RecIter::from_fields(rf, rfe) {
            w.write_all(&[delimiter])?;
            w.write_all(f)?;
        }
        w.write_all(&[terminator])?;
    }
    Ok(())
}
            
#[inline]
fn print_right<W:io::Write>(
    w: &mut W,
    delimiter: u8,
    terminator: u8,
    g: &Group
) -> Result<(), Box<Error>> {
    let mut is_first: bool;
    for (rf, rfe) in g.non_key_iter() {
        is_first = true;
        
        for f in g.first_key_iter() {
            if !is_first {
                w.write_all(&[delimiter])?;
            } else {
                is_first = false;
            }
            w.write_all(f)?;
        }
        for f in RecIter::from_fields(rf, rfe) {
            w.write_all(&[delimiter])?;
            w.write_all(f)?;
        }
        w.write_all(&[terminator])?;
    }
    Ok(())
}
            
#[inline]
fn print_both<W:io::Write>(
    w: &mut W,
    delimiter: u8,
    terminator: u8,
    g0: &Group,
    g1: &Group,
) -> Result<(), Box<Error>> {
    let mut is_first: bool;
    for (rf0, rfe0) in g0.non_key_iter() {
        for (rf1, rfe1) in g1.non_key_iter() {
            is_first = true;
            
            for f in g0.first_key_iter() {
                if !is_first {
                    w.write_all(&[delimiter])?;
                } else {
                    is_first = false;
                }
                w.write_all(f)?;
            }
            for f in RecIter::from_fields(rf0, rfe0) {
                w.write_all(&[delimiter])?;
                w.write_all(f)?;
            }
            for f in RecIter::from_fields(rf1, rfe1) {
                w.write_all(&[delimiter])?;
                w.write_all(f)?;
            }
        }
        w.write_all(&[terminator])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{JoinOptions, join};
    use reader::ReaderBuilder;
    use record::{RecordBuilder, GroupBuilder};

    #[test]
    fn inner_join_0() {
        let data0 = "color,red\ncolor,green\ncolor,blue\nshape,circle\nshape,square";
        let data1 = "color,orange\nsize,small\nsize,large";

        let mut rdr0 = ReaderBuilder::default().from_reader(data0.as_bytes());
        let mut rdr1 = ReaderBuilder::default().from_reader(data1.as_bytes());

        let rec0 = RecordBuilder::default().build().unwrap();
        let rec1 = RecordBuilder::default().build().unwrap();

        let mut g0 = GroupBuilder::default().from_record(rec0);
        let mut g1 = GroupBuilder::default().from_record(rec1);


        let opts = JoinOptions {
            show_left: false,
            show_right: false,
            show_both: true,
            delimiter: b',',
            terminator: b'\n',
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, opts).unwrap();

        assert_eq!(&out[..], &b"color,red,orange\ncolor,green,orange\ncolor,blue,orange\n"[..]); 
    }

    #[test]
    fn inner_join_1() {
        let data0 = "altitude,low\naltitude,high\ncolor,red";
        let data1 = "color,orange\nsize,small\nsize,large";

        let mut rdr0 = ReaderBuilder::default().from_reader(data0.as_bytes());
        let mut rdr1 = ReaderBuilder::default().from_reader(data1.as_bytes());

        let rec0 = RecordBuilder::default().build().unwrap();
        let rec1 = RecordBuilder::default().build().unwrap();

        let mut g0 = GroupBuilder::default().from_record(rec0);
        let mut g1 = GroupBuilder::default().from_record(rec1);


        let opts = JoinOptions {
            show_left: false,
            show_right: false,
            show_both: true,
            delimiter: b',',
            terminator: b'\n',
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, opts).unwrap();

        assert_eq!(&out[..], &b"color,red,orange\n"[..]); 
    }

    #[test]
    fn left_outer_join_0() {
        let data0 = "altitude,low\naltitude,high\ncolor,red";
        let data1 = "color,orange\nsize,small\nsize,large";

        let mut rdr0 = ReaderBuilder::default().from_reader(data0.as_bytes());
        let mut rdr1 = ReaderBuilder::default().from_reader(data1.as_bytes());

        let rec0 = RecordBuilder::default().build().unwrap();
        let rec1 = RecordBuilder::default().build().unwrap();

        let mut g0 = GroupBuilder::default().from_record(rec0);
        let mut g1 = GroupBuilder::default().from_record(rec1);


        let opts = JoinOptions {
            show_left: true,
            show_right: false,
            show_both: true,
            delimiter: b',',
            terminator: b'\n',
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, opts).unwrap();

        assert_eq!(&out[..], &b"altitude,low\naltitude,high\ncolor,red,orange\n"[..]); 
    }

    #[test]
    fn left_excl_join_0() {
        let data0 = "altitude,low\naltitude,high\ncolor,red";
        let data1 = "color,orange\nsize,small\nsize,large";

        let mut rdr0 = ReaderBuilder::default().from_reader(data0.as_bytes());
        let mut rdr1 = ReaderBuilder::default().from_reader(data1.as_bytes());

        let rec0 = RecordBuilder::default().build().unwrap();
        let rec1 = RecordBuilder::default().build().unwrap();

        let mut g0 = GroupBuilder::default().from_record(rec0);
        let mut g1 = GroupBuilder::default().from_record(rec1);


        let opts = JoinOptions {
            show_left: true,
            show_right: false,
            show_both: false,
            delimiter: b',',
            terminator: b'\n',
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, opts).unwrap();

        assert_eq!(&out[..], &b"altitude,low\naltitude,high\n"[..]); 
    }

    #[test]
    fn right_outer_join_0() {
        let data0 = "altitude,low\naltitude,high\ncolor,red";
        let data1 = "color,orange\nsize,small\nsize,large";

        let mut rdr0 = ReaderBuilder::default().from_reader(data0.as_bytes());
        let mut rdr1 = ReaderBuilder::default().from_reader(data1.as_bytes());

        let rec0 = RecordBuilder::default().build().unwrap();
        let rec1 = RecordBuilder::default().build().unwrap();

        let mut g0 = GroupBuilder::default().from_record(rec0);
        let mut g1 = GroupBuilder::default().from_record(rec1);


        let opts = JoinOptions {
            show_left: false,
            show_right: true,
            show_both: true,
            delimiter: b',',
            terminator: b'\n',
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, opts).unwrap();

        assert_eq!(&out[..], &b"color,red,orange\nsize,small\nsize,large\n"[..]); 
    }

    #[test]
    fn right_excl_join_0() {
        let data0 = "altitude,low\naltitude,high\ncolor,red";
        let data1 = "color,orange\nsize,small\nsize,large";

        let mut rdr0 = ReaderBuilder::default().from_reader(data0.as_bytes());
        let mut rdr1 = ReaderBuilder::default().from_reader(data1.as_bytes());

        let rec0 = RecordBuilder::default().build().unwrap();
        let rec1 = RecordBuilder::default().build().unwrap();

        let mut g0 = GroupBuilder::default().from_record(rec0);
        let mut g1 = GroupBuilder::default().from_record(rec1);


        let opts = JoinOptions {
            show_left: false,
            show_right: true,
            show_both: false,
            delimiter: b',',
            terminator: b'\n',
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, opts).unwrap();

        assert_eq!(&out[..], &b"size,small\nsize,large\n"[..]); 
    }
}
         
        
