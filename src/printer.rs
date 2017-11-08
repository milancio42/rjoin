use std::io::{self, Write,};
use std::error::Error;
use std::ops::Range;

/// A trait for printing records in a desired format.
pub trait Print<W:io::Write> {
    /// Print the left records into `w`.
    fn print_left(
        &mut self,
        w: &mut W,
        buf: &[u8],
        fields: &[Range<usize>],
        records: &[usize],
        print: Range<usize>
    ) -> Result<(),Box<Error>>;
    /// Print the right records into `w`.
    fn print_right(
        &mut self,
        w: &mut W,
        buf: &[u8],
        fields: &[Range<usize>],
        records: &[usize],
        print: Range<usize>
    ) -> Result<(),Box<Error>>;
    /// Print both left anf right records into `w`.
    fn print_both(
        &mut self,
        w: &mut W,
        buf0: &[u8],
        buf1: &[u8],
        fields0: &[Range<usize>],
        fields1: &[Range<usize>],
        records0: &[usize],
        records1: &[usize],
        print0: Range<usize>,
        print1: Range<usize>
    ) -> Result<(),Box<Error>>;
}

/// Print the records in the following format: first the key fields followed by non-key
/// fields.
#[derive(Clone)]
pub struct KeyFirst {
    delimiter: u8,
    terminator: u8,
    key_idx0: Vec<usize>,
    key_idx0_asc: Vec<usize>,
    key_idx1: Vec<usize>,
    key_idx1_asc: Vec<usize>,
    key_buf: Vec<u8>,
}

impl Default for KeyFirst {
    fn default() -> Self {
        KeyFirst {
            delimiter: b',',
            terminator: b'\n',
            key_idx0: vec![0],
            key_idx0_asc: vec![0],
            key_idx1: vec![0],
            key_idx1_asc: vec![0],
            key_buf: Vec::new(),
        }
    }
}

impl KeyFirst {
    pub fn from_parts(
        delimiter: u8,
        terminator: u8,
        key_idx0: Vec<usize>,
        key_idx1: Vec<usize>,
    ) -> Self {
        let mut key_idx0_asc = key_idx0.clone();
        let mut key_idx1_asc = key_idx1.clone();
        key_idx0_asc.sort(); 
        key_idx1_asc.sort(); 

        KeyFirst {
            delimiter ,
            terminator ,
            key_idx0 ,
            key_idx0_asc ,
            key_idx1 ,
            key_idx1_asc ,
            key_buf: Vec::new(),
        }
    }
}

impl<W:io::Write> Print<W> for KeyFirst {
    #[inline]
    fn print_left(
        &mut self,
        w: &mut W,
        buf: &[u8],
        fields: &[Range<usize>],
        records: &[usize],
        print: Range<usize>
    ) -> Result<(),Box<Error>> {
        print_single(
            w,
            buf,
            fields,
            records,
            print,
            self.delimiter,
            self.terminator,
            &self.key_idx0,
            &self.key_idx0_asc,
        )
    }
        
    #[inline]
    fn print_right(
        &mut self,
        w: &mut W,
        buf: &[u8],
        fields: &[Range<usize>],
        records: &[usize],
        print: Range<usize>
    ) -> Result<(),Box<Error>> {
        print_single(
            w,
            buf,
            fields,
            records,
            print,
            self.delimiter,
            self.terminator,
            &self.key_idx1,
            &self.key_idx1_asc,
        )
    }
        
    #[inline]
    fn print_both(
        &mut self,
        w: &mut W,
        buf0: &[u8],
        buf1: &[u8],
        fields0: &[Range<usize>],
        fields1: &[Range<usize>],
        records0: &[usize],
        records1: &[usize],
        print0: Range<usize>,
        print1: Range<usize>
    ) -> Result<(),Box<Error>> {
        let mut is_first = true;
        let start0 = match print0.start.checked_sub(1).and_then(|i| records0.get(i)) {
            Some(&start) => start,
            None => 0,
        };
        let start1 = match print1.start.checked_sub(1).and_then(|i| records1.get(i)) {
            Some(&start) => start,
            None => 0,
        };
        let mut r0 = start0..start0;
        let mut r1 = start1..start1;
        self.key_buf.clear();
        for r0e in &records0[print0] {
            r0.end = *r0e;
            let r0f = &fields0[r0.clone()];

            for r1e in &records1[print1.clone()] {
                r1.end = *r1e;
                let r1f = &fields1[r1.clone()];
                // write key fields first
                if self.key_buf.is_empty() {
                    for k in &self.key_idx0 {
                        if !is_first {
                            self.key_buf.write_all(&[self.delimiter])?;
                        } else {
                            is_first = false;
                        }
                        self.key_buf.write_all(&buf0[r0f[*k].clone()])?;
                    }
                }
                w.write_all(&self.key_buf)?;

                // write non-key fields that lie between key fields
                let mut start = 0;
                for k in &self.key_idx0_asc {
                    for f in &r0f[start..*k] {
                        w.write_all(&[self.delimiter])?;
                        w.write_all(&buf0[f.clone()])?;
                    }
                    start = *k + 1;
                }
                // write remaining non-key fields
                for f in &r0f[start..] {
                    w.write_all(&[self.delimiter])?;
                    w.write_all(&buf0[f.clone()])?;
                }

                start = 0;
                // write non-key fields that lie in between key fields
                for k in &self.key_idx1_asc {
                    for f in &r1f[start..*k] {
                        w.write_all(&[self.delimiter])?;
                        w.write_all(&buf1[f.clone()])?;
                    }
                    start = *k + 1;
                }
                // write remaining non-key fields
                for f in &r1f[start..] {
                    w.write_all(&[self.delimiter])?;
                    w.write_all(&buf1[f.clone()])?;
                }
                w.write_all(&[self.terminator])?;
                is_first = true;
                r1.start = r1.end;
            }
            r0.start = r0.end;
            r1.start = start1;
        }
        Ok(())
    }
}
        
#[inline]
fn print_single<W:io::Write>(
    w: &mut W,
    buf: &[u8],
    fields: &[Range<usize>],
    records: &[usize],
    print: Range<usize>,
    delimiter: u8,
    terminator: u8,
    key_idx: &[usize],
    key_idx_asc: &[usize],
) -> Result<(), Box<Error>> {
    let mut is_first = true;
    let mut start = match print.start.checked_sub(1).and_then(|i| records.get(i)) {
        Some(&start) => start,
        None => 0,
    };
    let mut r = start..start;
    for re in &records[print] {
        r.end = *re;
        let rf = &fields[r.clone()];
        // write key fields first
        for k in key_idx {
            if !is_first {
                w.write_all(&[delimiter])?;
            } else {
                is_first = false;
            }
            w.write_all(&buf[rf[*k].clone()])?;
        }
        // write non-key fields that lie in between key fields
        start = 0;
        for k in key_idx_asc {
            for f in &rf[start..*k] {
                w.write_all(&[delimiter])?;
                w.write_all(&buf[f.clone()])?;
            }
            start = *k + 1;
        }
        // write remaining non-key fields
        for f in &rf[start..] {
            w.write_all(&[delimiter])?;
            w.write_all(&buf[f.clone()])?;
        }
        w.write_all(&[terminator])?;
        is_first = true;
        r.start = r.end;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print() {
        
        struct TestCase {
            buf: String,
            fields: Vec<Range<usize>>,
            records: Vec<usize>,
            print: Range<usize>,
            key_idx: Vec<usize>,
            delimiter: u8,
            terminator: u8,
            want: (String, String),
        }

        let test_cases = vec![
            TestCase {
                buf: "a,0,b,0\nc,1,d,1".to_owned(),
                fields: vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15],
                records: vec![4, 8],
                print: 0..1,
                key_idx: vec![0],
                delimiter: b';',
                terminator: b'|',
                want: ("a;0;b;0|".to_owned(), "a;0;b;0;0;b;0|".to_owned()),
            },
            TestCase {
                buf: "a,0,b,0\nc,1,d,1".to_owned(),
                fields: vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15],
                records: vec![4, 8],
                print: 0..1,
                key_idx: vec![2],
                delimiter: b',',
                terminator: b'\n',
                want: ("b,a,0,0\n".to_owned(), "b,a,0,0,a,0,0\n".to_owned()),
            },
            TestCase {
                buf: "a,0,b,0\nc,1,d,1".to_owned(),
                fields: vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15],
                records: vec![4, 8],
                print: 0..1,
                key_idx: vec![2, 0],
                delimiter: b',',
                terminator: b'\n',
                want: ("b,a,0,0\n".to_owned(), "b,a,0,0,0,0\n".to_owned()),
            },
            TestCase {
                buf: "a,0,b,0\nc,1,d,1".to_owned(),
                fields: vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15],
                records: vec![4, 8],
                print: 1..2,
                key_idx: vec![2, 0],
                delimiter: b',',
                terminator: b'\n',
                want: ("d,c,1,1\n".to_owned(), "d,c,1,1,1,1\n".to_owned()),
            },
            TestCase {
                buf: "a,0,b,0\na,1,b,1".to_owned(),
                fields: vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15],
                records: vec![4, 8],
                print: 0..2,
                key_idx: vec![2, 0],
                delimiter: b',',
                terminator: b'\n',
                want: (
                    "b,a,0,0\nb,a,1,1\n".to_owned(),
                    "b,a,0,0,0,0\nb,a,0,0,1,1\nb,a,1,1,0,0\nb,a,1,1,1,1\n".to_owned()),
            },
        ];

        for t in test_cases {
            let TestCase { buf, fields, records, print, key_idx, delimiter, terminator, want } = t;
            let mut p = KeyFirst::from_parts(delimiter, terminator, key_idx.clone(), key_idx);
            let mut left: Vec<u8> = Vec::new();
            let mut right: Vec<u8> = Vec::new();
            let mut both: Vec<u8> = Vec::new();
            p.print_left(&mut left, buf.as_bytes(), &fields, &records, print.clone()).unwrap();
            p.print_right(&mut right, buf.as_bytes(), &fields, &records, print.clone()).unwrap();
            p.print_both(
                &mut both,
                buf.as_bytes(), 
                buf.as_bytes(),
                &fields,
                &fields,
                &records,
                &records,
                print.clone(),
                print.clone(),
            ).unwrap();
            
            assert_eq!(
                (left.as_slice(), right.as_slice(), both.as_slice()), 
                (want.0.as_bytes(), want.0.as_bytes(), want.1.as_bytes())
            );
        }
    }
}

                


