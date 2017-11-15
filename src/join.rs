use super::printer::Print;
use super::csv::basic::{FirstRec, Group, cmp_records,};
use std::io;
use std::cmp::Ordering;
use std::error::Error;
use std::ops::Range;

/// Options defining the output of the join.
///
/// For those familiar with SQL, you can tweak these to obtain:
///   * INNER JOIN - `show_left: false`, `show_right: false`, `show_both: true`
///   * LEFT OUTER JOIN - `show_left: true`, `show_right: false`, `show_both: true`
///   * RIGHT OUTER JOIN - `show_left: false`, `show_right: true`, `show_both: true`
///   * FULL OUTER JOIN - `show_left: true`, `show_right: true`, `show_both: true`
///
/// and even exclusive joins (outer joins without the inner part).
#[derive(Debug, Clone, Copy)]
pub struct JoinOptions {
    show_left: bool,
    show_right: bool,
    show_both: bool,
}

impl Default for JoinOptions {
    fn default() -> Self {
        JoinOptions {
            show_left: false,
            show_right: false,
            show_both: true,
        }
    }
}
        
impl JoinOptions {
    /// Create a new instance of `JoinOptions`. By default, only `show_both` is enabled. 
    pub fn new() -> Self {
        JoinOptions::default()
    }

    /// Create a new instance of `JoinOptions` with the specified options.
    pub fn from_options(show_left: bool, show_right: bool, show_both: bool) -> Self {
        JoinOptions {
            show_left: show_left,
            show_right: show_right,
            show_both: show_both,
        }
    }
}

//// Join the groups of records `g0` and `g1` parsed by CSV readers `rdr0` and `rdr1`. The output is
//// written into `w` using the provided printer `p`. 
//// # Example
////
//// ```
//// extern crate rjoin;
////
//// use std::error::Error;
//// use rjoin::record::{RecordBuilder, GroupBuilder};
//// use rjoin::reader::ReaderBuilder;
////
//// # fn main() { example().unwrap(); }
////
//// fn example() -> Result<(), Box<Error>> {
////     let data0 = "a,a,0\na,b,1";
////     let data1 = "a,b,2\na,c,3";
////
////     let mut rdr0 = ReaderBuilder::default().from_reader(data0.as_bytes());
////     let mut rdr1 = ReaderBuilder::default().from_reader(data1.as_bytes());
////
////     let rec0 = RecordBuilder::default().keys(&[1,0][..]).build()?;
////     let rec1 = RecordBuilder::default().keys(&[1,0][..]).build()?;
////
////     let mut g0 = GroupBuilder::default().from_record(rec0);
////     let mut g1 = GroupBuilder::default().from_record(rec1);
////
////     let p = KeyFirst::default();
////
////     // show all - equivalent to FULL OUTER JOIN in SQL
////     let opts = JoinOptions::from_options(true, true, true);
////     let mut out: Vec<u8> = Vec::new();
////     let _ = join(&mut rdr0, &mut rdr1, &mut g0, &mut g1, &mut out, p, opts)?;
////
////     assert_eq!(&out[..], &b"a,a,0\nb,a,1,2\nc,a,3\n"[..]); 
////     Ok(())
//// }
//// ```
pub fn join<R0,R1,W,P>(
    group0: &mut Group<R0>,
    group1: &mut Group<R1>,
    w: &mut W,
    mut p: P,
    opts: JoinOptions,
) -> Result<(), Box<Error>>
    where R0: io::Read,
          R1: io::Read,
          W: io::Write,
          P: Print<W>,
{
    let mut ord = Ordering::Equal;
    let mut g0: Option<Range<usize>> = None;
    let mut g1: Option<Range<usize>> = None;
    let mut r0: Range<usize>;
    let mut r1: Range<usize>;
    loop {
        match ord {
            Ordering::Less => {
                g0 = match group0.next_group() {
                    Ok(o) => o,
                    Err(e) => return Err(format!("left input: {}", e).into()),
                };
            }
            Ordering::Greater => {
                g1 = match group1.next_group() {
                    Ok(o) => o,
                    Err(e) => return Err(format!("right input: {}", e).into()),
                };
            }
            Ordering::Equal => {
                g0 = match group0.next_group() {
                    Ok(o) => o,
                    Err(e) => return Err(format!("left input: {}", e).into()),
                };
                g1 = match group1.next_group() {
                    Ok(o) => o,
                    Err(e) => return Err(format!("right input: {}", e).into()),
                };
            }
        }
        ord = match (&g0, &g1) {
            (&Some(ref rng0), &Some(ref rng1)) => {
                let (buf0, idx0) = group0.buf_index();
                let (buf1, idx1) = group1.buf_index();
                r0 = idx0.get_record(rng0.start).unwrap_or(0..0);
                r1 = idx1.get_record(rng1.start).unwrap_or(0..0);
                match cmp_records(
                    buf0,
                    buf1,
                    &idx0.fields()[r0.clone()],
                    &idx1.fields()[r1.clone()],
                    group0.key_idx(),
                    group1.key_idx(),
                    ) {

                    Ok(ord) => {
                        match ord {
                            Ordering::Less => {
                                if opts.show_left {
                                    p.print_left(w, buf0, idx0.fields(), idx0.records(), rng0.clone())?;
                                }
                            }
                            Ordering::Greater => {
                                if opts.show_right {
                                    p.print_right(w, buf1, idx1.fields(), idx1.records(), rng1.clone())?;
                                }
                            }
                            Ordering::Equal => {
                                if opts.show_both {
                                    p.print_both(
                                        w,
                                        buf0,
                                        buf1,
                                        idx0.fields(),
                                        idx1.fields(),
                                        idx0.records(),
                                        idx1.records(),
                                        rng0.clone(),
                                        rng1.clone(),
                                    )?;
                                }
                            }
                        }
                        ord
                    }
                    Err(_) => {
                        return Err("internal: the record was not grouped properly".into());
                    }
                }
            }
            (&Some(ref rng0), &None) => {
                let (buf0, idx0) = group0.buf_index();
                if opts.show_left {
                    p.print_left(w, buf0, idx0.fields(), idx0.records(), rng0.clone())?;
                } else {
                    return Ok(());
                }
                Ordering::Less
            }
            (&None, &Some(ref rng1)) => {
                let (buf1, idx1) = group1.buf_index();
                if opts.show_right {
                    p.print_right(w, buf1, idx1.fields(), idx1.records(), rng1.clone())?;
                } else {
                    return Ok(());
                }
                Ordering::Greater
            }
            (&None, &None) => return Ok(()),
        }
    }
}

pub fn head<R0,R1,W,P>(
    first_rec0: &mut FirstRec<R0>,
    first_rec1: &mut FirstRec<R1>,
    w: &mut W,
    mut p: P,
    opts: JoinOptions,
) -> Result<(), Box<Error>>
    where R0: io::Read,
          R1: io::Read,
          W: io::Write,
          P: Print<W>,
{
    let fr0 = first_rec0.is_present()?;
    let fr1 = first_rec1.is_present()?;

    if opts.show_both || (opts.show_left && opts.show_right) {
        if fr0 && fr1 {
            let (buf0, idx0) = first_rec0.buf_index();
            let (buf1, idx1) = first_rec1.buf_index();
            p.print_both(
                w,
                buf0,
                buf1,
                idx0.fields(),
                idx1.fields(),
                idx0.records(),
                idx1.records(),
                0..1,
                0..1,
            )?;
        }
    } else if opts.show_left {
        if fr0 {
            let (buf0, idx0) = first_rec0.buf_index();
            p.print_left(w, buf0, idx0.fields(), idx0.records(), 0..1)?;
        }
    }
    else {
        if fr1 {
            let (buf1, idx1) = first_rec1.buf_index();
            p.print_right(w, buf1, idx1.fields(), idx1.records(), 0..1)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{JoinOptions, join, head,};
    use printer::KeyFirst;
    use csv::basic::{FirstRec, Group};
    use rollbuf::RollBuf;
    use csvroll::index_builder::IndexBuilder;
    use csvroll::parser::Parser;

    #[test]
    fn test_join() {
        struct TestCase {
            note: String,
            data0: String,
            data1: String,
            opts: JoinOptions,
            want: String,
        }

        let test_cases = vec![
            TestCase {
                note: "inner join with cartesian product".into(),
                data0: "color,red\ncolor,green\ncolor,blue\nshape,circle\nshape,square".into(),
                data1: "color,orange\ncolor,purple\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: false, show_right: false, show_both: true },
                want: 
                    "\
                     color,red,orange\n\
                     color,red,purple\n\
                     color,green,orange\n\
                     color,green,purple\n\
                     color,blue,orange\n\
                     color,blue,purple\n\
                    ".into(),
            },
            TestCase {
                note: "inner join simple".into(),
                data0: "altitude,low\naltitude,high\ncolor,red".into(),
                data1: "color,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: false, show_right: false, show_both: true },
                want: "color,red,orange\n".into(),
            },
            TestCase {
                note: "left outer join simple".into(),
                data0: "altitude,low\naltitude,high\ncolor,red".into(),
                data1: "color,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: true, show_right: false, show_both: true },
                want: 
                    "\
                     altitude,low\n\
                     altitude,high\n\
                     color,red,orange\n\
                    ".into(),
            },
            TestCase {
                note: "left exclusion join simple".into(),
                data0: "altitude,low\naltitude,high\ncolor,red".into(),
                data1: "color,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: true, show_right: false, show_both: false },
                want: 
                    "\
                     altitude,low\n\
                     altitude,high\n\
                    ".into(),
            },
            TestCase {
                note: "right outer join simple".into(),
                data0: "altitude,low\naltitude,high\ncolor,red".into(),
                data1: "color,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: false, show_right: true, show_both: true },
                want: 
                    "\
                     color,red,orange\n\
                     size,small\n\
                     size,large\n\
                    ".into(),
            },
            TestCase {
                note: "right exclusion join simple".into(),
                data0: "altitude,low\naltitude,high\ncolor,red".into(),
                data1: "color,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: false, show_right: true, show_both: false },
                want: 
                    "\
                     size,small\n\
                     size,large\n\
                    ".into(),
            },
            TestCase {
                note: "full outer join simple".into(),
                data0: "altitude,low\naltitude,high\ncolor,red".into(),
                data1: "color,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: true, show_right: true, show_both: true },
                want: 
                    "\
                     altitude,low\n\
                     altitude,high\n\
                     color,red,orange\n\
                     size,small\n\
                     size,large\n\
                    ".into(),
            },
        ];

        for t in test_cases {
            let TestCase {note, data0, data1, opts, want } = t;
            let buf0 = RollBuf::with_capacity(16, data0.as_bytes());
            let buf1 = RollBuf::with_capacity(16, data1.as_bytes());
            let idx_builder0 = IndexBuilder::new(b',', b'\n');
            let idx_builder1 = IndexBuilder::new(b',', b'\n');
            let parser0 = Parser::from_parts(buf0, idx_builder0);
            let parser1 = Parser::from_parts(buf1, idx_builder1);
            let mut group0 = Group::init(parser0, vec![0]).unwrap();
            let mut group1 = Group::init(parser1, vec![0]).unwrap();
            let mut out: Vec<u8> = Vec::new();
            let printer = KeyFirst::from_parts(b',', b'\n', vec![0], vec![0]);

            println!("{}", note);
            join(&mut group0, &mut group1, &mut out, printer, opts).unwrap();
            assert_eq!(out, want.as_bytes());
        }
    }

    #[test]
    fn test_header() {
        struct TestCase {
            note: String,
            data0: String,
            data1: String,
            opts: JoinOptions,
            want: String,
        }

        let test_cases = vec![
            TestCase {
                note: "inner join".into(),
                data0: "col0,col1\naltitude,low\naltitude,high\ncolor,red".into(),
                data1: "col2,col3\ncolor,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: false, show_right: false, show_both: true },
                want: "col0,col1,col3\n".into(),
            },
            TestCase {
                note: "left outer join".into(),
                data0: "col0,col1\naltitude,low\naltitude,high\ncolor,red".into(),
                data1: "col2,col3\ncolor,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: true, show_right: false, show_both: true },
                want: "col0,col1,col3\n".into(),
            },
            TestCase {
                note: "left exclusion join".into(),
                data0: "col0,col1\naltitude,low\naltitude,high\ncolor,red".into(),
                data1: "col2,col3\ncolor,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: true, show_right: false, show_both: false },
                want: "col0,col1\n".into(),
            },
            TestCase {
                note: "right outer join".into(),
                data0: "col0,col1\naltitude,low\naltitude,high\ncolor,red".into(),
                data1: "col2,col3\ncolor,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: false, show_right: true, show_both: true },
                want: "col0,col1,col3\n".into(),
            },
            TestCase {
                note: "right exclusion join".into(),
                data0: "col0,col1\naltitude,low\naltitude,high\ncolor,red".into(),
                data1: "col2,col3\ncolor,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: false, show_right: true, show_both: false },
                want: "col2,col3\n".into(),
            },
            TestCase {
                note: "full outer join".into(),
                data0: "col0,col1\naltitude,low\naltitude,high\ncolor,red".into(),
                data1: "col2,col3\ncolor,orange\nsize,small\nsize,large".into(),
                opts: JoinOptions { show_left: true, show_right: true, show_both: true },
                want: "col0,col1,col3\n".into(),
            },
        ];

        for t in test_cases {
            let TestCase {note, data0, data1, opts, want } = t;
            let buf0 = RollBuf::with_capacity(16, data0.as_bytes());
            let buf1 = RollBuf::with_capacity(16, data1.as_bytes());
            let idx_builder0 = IndexBuilder::new(b',', b'\n');
            let idx_builder1 = IndexBuilder::new(b',', b'\n');
            let parser0 = Parser::from_parts(buf0, idx_builder0);
            let parser1 = Parser::from_parts(buf1, idx_builder1);
            let mut first_rec0 = FirstRec::init(parser0).unwrap();
            let mut first_rec1 = FirstRec::init(parser1).unwrap();
            let mut out: Vec<u8> = Vec::new();
            let printer = KeyFirst::from_parts(b',', b'\n', vec![0], vec![0]);

            println!("{}", note);
            head(&mut first_rec0, &mut first_rec1, &mut out, printer, opts).unwrap();
            assert_eq!(out, want.as_bytes());
        }
    }
}
         
        
