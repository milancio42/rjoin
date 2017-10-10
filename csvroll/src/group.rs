use super::parser::{Parser, Index,};
use std::cmp::Ordering;
use std::ops::Range;
use std::error::Error;
use std::io;

pub struct Group<R> {
    parser: Parser<R>,
    key_idx: Vec<usize>,
    first_rec_idx: usize,
    first_rec: Range<usize>,
    last_rec_idx: usize,
    last_rec_end: usize,
    is_buf_full: bool,
}

impl<R: io::Read> Group<R> {
    pub fn init(mut parser: Parser<R>, key_idx: Vec<usize>) -> Result<Self, Box<Error>> {
        let is_buf_full = parser.parse()?;
        let first_rec: Range<usize>;
        let last_rec_end: usize;
        let last_rec_idx: usize;

        {
            let (_, struct_idx) = parser.output();

            match struct_idx.records().first() {
                Some(&re) => {
                    first_rec = 0..re;
                    last_rec_idx = 1;
                    last_rec_end = re;
                }
                None => {
                    first_rec = 0..0;
                    last_rec_idx = 0;
                    last_rec_end = 0;
                }
            }
        }

        Ok(Self {
            parser ,
            key_idx ,
            first_rec_idx: 0,
            first_rec ,
            last_rec_idx ,
            last_rec_end ,
            is_buf_full ,
        })
    }
    
    #[inline]
    pub fn next_group(&mut self) -> Result<Range<usize>, Box<Error>> {
        loop {
            {
                let (buf, struct_idx) = self.parser.output();
                let fields = struct_idx.fields();

                for &re in &struct_idx.records()[self.last_rec_idx..] {
                    match cmp_records(
                        buf,
                        &fields[self.last_rec_end..re],
                        &fields[self.first_rec.clone()],
                        &self.key_idx)? {

                        Ordering::Less => {
                            return Err("The records are not sorted in ascending order".into());
                        }
                        Ordering::Greater => {
                            let g = self.first_rec_idx..self.last_rec_idx;
                            self.first_rec_idx = self.last_rec_idx;
                            self.first_rec = self.last_rec_end..re;
                            self.last_rec_idx += 1;
                            self.last_rec_end = re;
                            return Ok(g.clone());
                        }
                        Ordering::Equal => {
                            self.last_rec_idx += 1;
                            self.last_rec_end = re;
                        }
                    }
                }
            }

            if self.is_buf_full {
                let consumed = self.first_rec.start;
                self.last_rec_idx -= self.first_rec_idx;
                self.first_rec_idx = 0;
                self.first_rec = 0..(self.first_rec.end - consumed);
                self.last_rec_end -= consumed;
                self.parser.consume(consumed);
                self.is_buf_full = self.parser.parse()?;
            } else {
                let g_start = self.first_rec_idx;
                self.first_rec_idx = self.last_rec_idx;
                return Ok(g_start..self.last_rec_idx);
                
            }
        }
    }

    #[inline]
    pub fn buf_index(&self) -> (&[u8], &Index) {
        self.parser.output()
    }
}

#[inline]
fn cmp_records(
    buf: &[u8],
    rec_0: &[Range<usize>],
    rec_1: &[Range<usize>],
    key_idx: &[usize],
) -> Result<Ordering, Box<Error>> {

    for &k in key_idx {
        let f0 = match rec_0.get(k) {
            Some(f) => f.clone(),
            None => return Err("The first record is shorter than key fields".into()),
        };
        let f1 = match rec_1.get(k) {
            Some(f) => f.clone(),
            None => return Err("The second record is shorter than key fields".into()),
        };
        
        match buf[f0].cmp(&buf[f1]) {
            Ordering::Less => return Ok(Ordering::Less),
            Ordering::Greater => return Ok(Ordering::Greater),
            Ordering::Equal => continue,
        }
    }

    Ok(Ordering::Equal)
}

    

#[cfg(test)]
mod tests {
    use super::*;
    use rollbuf::RollBuf;
    use ::index_builder::IndexBuilder;

    #[test]
    fn test_group() {
        struct TestCase {
            input: String,
            buf_len: usize,
            key_idx: Vec<usize>,
            want: Vec<(String, Index, Range<usize>)>,
        }

        let test_cases = vec![
            TestCase {
                input: "a,0\na,1\nb,0\nc,0".to_owned(),
                buf_len: 24,
                key_idx: vec![0],
                want: vec![
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    0..2,),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    2..3,),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    3..4,),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    4..4,),
                ],
            },
            TestCase {
                input: "a,0\na,1\nb,0\nc,0".to_owned(),
                buf_len: 11,
                key_idx: vec![0],
                want: vec![
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    0..2,),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    2..3,),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    3..4,),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    4..4,),
                ],
            },
            TestCase {
                input: "a,0\na,1\nb,0\nb,0".to_owned(),
                buf_len: 12,
                key_idx: vec![0],
                want: vec![
                   ("a,0\na,1\nb,0\n".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11], vec![2, 4, 6]),
                    0..2,),
                   ("b,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 4]),
                    0..1,),
                   ("b,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 4]),
                    1..2,),
                   ("b,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 4]),
                    2..2,),
                ],
            },
        ];

        for t in test_cases {
            let TestCase { input, buf_len, key_idx, want } = t;
            let buf = RollBuf::with_capacity(buf_len, input.as_bytes());
            let idx_builder = IndexBuilder::new(b',', b'\n');
            let parser = Parser::from_parts(buf, idx_builder);
            let mut group = Group::init(parser, key_idx).unwrap();

            for w in want {
                let recs = group.next_group().unwrap();
                let (buf, idx) = group.buf_index();
                assert_eq!((buf, idx, recs), (w.0.as_bytes(), &w.1, w.2));
            }
        }
    }
}

                

            



                        
                
        
