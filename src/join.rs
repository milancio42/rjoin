use super::record::{Record, Group};
use super::reader::Reader;
use super::printer::{PrintRecord, PrintGroup};
use std::io;
use std::cmp::Ordering::{Less, Greater, Equal};
use std::error::Error;

pub struct JoinOptions {
    show_left: bool,
    show_right: bool,
    show_both: bool,
}

impl JoinOptions {
    pub fn new(show_left: bool, show_right: bool, show_both: bool) -> Self {
        JoinOptions {
            show_left: show_left,
            show_right: show_right,
            show_both: show_both,
        }
    }
}

pub fn join<R0,R1,W,P>(
    rdr0: &mut Reader<R0>,
    rdr1: &mut Reader<R1>,
    g0: &mut Group,
    g1: &mut Group,
    w: &mut W,
    mut p: P,
    opts: JoinOptions,
) -> Result<bool, Box<Error>>
    where R0: io::Read,
          R1: io::Read,
          W: io::Write,
          P: PrintGroup<W>,
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
                            p.print_left(w, g0)?;
                        }
                    }
                    Greater => {
                        if opts.show_right {
                            p.print_right(w, g1)?;
                        }
                    }
                    Equal => {
                        if opts.show_both {
                            p.print_both(w, g0, g1)?;
                        }
                    }
                }
                key_ord
            }
            (true, false) => {
                if opts.show_left {
                    p.print_left(w, g0)?;
                }
                Less
            }
            (false, true) => {
                if opts.show_right {
                    p.print_right(w, g1)?;
                }
                Greater
            }
            (false, false) => return Ok(true),
        }
    }
}
                
pub fn header<R0,R1,W,P>(
    rdr0: &mut Reader<R0>,
    rdr1: &mut Reader<R1>,
    r0: &mut Record,
    r1: &mut Record,
    w: &mut W,
    mut p: P,
) -> Result<bool, Box<Error>>
    where R0: io::Read,
          R1: io::Read,
          W: io::Write,
          P: PrintRecord<W>,
{
    let l = rdr0.read_record(r0)?;
    let r = rdr1.read_record(r1)?;

    match (l, r) {
        (true, true) => p.print_both(w, r0, r1)?,
        (true, false) => p.print_left(w, r0)?,
        (false, true) => p.print_right(w, r1)?,
        (false, false) => return Ok(false),
    }
    Ok(true)
}




#[cfg(test)]
mod tests {
    use super::{JoinOptions, join};
    use reader::ReaderBuilder;
    use printer::KeyFirst;
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

        let p = KeyFirst::default();


        let opts = JoinOptions {
            show_left: false,
            show_right: false,
            show_both: true,
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, p, opts).unwrap();

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

        let p = KeyFirst::default();

        let opts = JoinOptions {
            show_left: false,
            show_right: false,
            show_both: true,
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, p, opts).unwrap();

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

        let p = KeyFirst::default();

        let opts = JoinOptions {
            show_left: true,
            show_right: false,
            show_both: true,
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, p, opts).unwrap();

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

        let p = KeyFirst::default();

        let opts = JoinOptions {
            show_left: true,
            show_right: false,
            show_both: false,
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, p, opts).unwrap();

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

        let p = KeyFirst::default();

        let opts = JoinOptions {
            show_left: false,
            show_right: true,
            show_both: true,
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, p, opts).unwrap();

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

        let p = KeyFirst::default();

        let opts = JoinOptions {
            show_left: false,
            show_right: true,
            show_both: false,
        };

        let mut out: Vec<u8> = Vec::new();
        let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, p, opts).unwrap();

        assert_eq!(&out[..], &b"size,small\nsize,large\n"[..]); 
    }
}
         
        
