use csvroll::parser::{Parser, Index,};
use std::cmp::Ordering;
use std::ops::Range;
use std::error::Error;
use std::io;

pub struct Group<R> {
    parser: Parser<R>,
    key_idx: Vec<usize>,
    first_rec: Range<usize>,
    rec: Range<usize>,
    group: Range<usize>,
    rec_count: usize,
    is_buf_full: bool,
}

impl<R> Group<R> {
    #[inline]
    pub fn key_idx(&self) -> &[usize] {
        &self.key_idx
    }
}

impl<R: io::Read> Group<R> {
    pub fn init(mut parser: Parser<R>, key_idx: Vec<usize>) -> Result<Self, Box<Error>> {
        let is_buf_full = parser.parse()?;
        let first_rec: Range<usize>;
        let rec: Range<usize>;
        let group: Range<usize>;
        let rec_count: usize;

        {
            let (_, struct_idx) = parser.output();

            match struct_idx.records().first() {
                Some(&re) => {
                    first_rec = 0..re;
                    rec = first_rec.clone();
                    group = 0..1;
                    rec_count = 1;
                }
                None => {
                    first_rec = 0..0;
                    rec = first_rec.clone();
                    group = 0..0;
                    rec_count = 0;
                }
            }
        }

        Ok(Self {
            parser ,
            key_idx ,
            first_rec ,
            rec ,
            group ,
            rec_count ,
            is_buf_full ,
        })
    }
    
    #[inline]
    pub fn next_group(&mut self) -> Result<Option<Range<usize>>, Box<Error>> {
        loop {
            let mut rec_count = self.rec_count;
            {
                let (buf, struct_idx) = self.parser.output();
                let fields = struct_idx.fields();

                for &re in &struct_idx.records()[self.group.end..] {
                    let rec = self.rec.end..re;
                    rec_count += 1;
                    match cmp_records(
                        buf,
                        buf,
                        &fields[self.rec.end..re],
                        &fields[self.first_rec.clone()],
                        &self.key_idx,
                        &self.key_idx,
                        ) {

                        Ok(ord) => match ord {
                            Ordering::Less => {
                                return Err(format!(
                                    "the record number {} has the key with lower value than the \
                                    preceding record", rec_count).into());
                            }
                            Ordering::Greater => {
                                let g = self.group.clone();
                                self.first_rec = rec.clone();
                                self.rec = rec.clone();
                                self.group = self.group.end..(self.group.end + 1);
                                self.rec_count = rec_count;
                                return Ok(Some(g));
                            }
                            Ordering::Equal => {
                                self.rec = rec.clone();
                                self.group.end += 1;
                            }
                        }
                        Err(e) => {
                            let c = if e == 0 {
                                self.rec_count
                            } else {
                                rec_count
                            };
                            return Err(format!(
                                "the record number {} has less fields than the key", c).into());
                        }
                    }
                }
            }

            self.rec_count = rec_count;
            if self.is_buf_full {
                let field_offset = self.first_rec.start;
                let rec_offset = self.group.start;
                self.first_rec = (self.first_rec.start - field_offset)..(self.first_rec.end - field_offset);
                self.rec = (self.rec.start - field_offset)..(self.rec.end - field_offset);
                self.group = (self.group.start - rec_offset)..(self.group.end - rec_offset);
                self.parser.consume(rec_offset);
                self.is_buf_full = self.parser.parse()?;
            } else {
                let g = self.group.clone();
                if g.start != g.end {
                    self.group = self.group.end..self.group.end;
                    return Ok(Some(g));
                } else {
                    return Ok(None);
                }
            }
        }
    }

    #[inline]
    pub fn buf_index(&self) -> (&[u8], &Index) {
        self.parser.output()
    }
}

#[inline]
pub fn cmp_records(
    buf0: &[u8],
    buf1: &[u8],
    rec_0: &[Range<usize>],
    rec_1: &[Range<usize>],
    key_idx0: &[usize],
    key_idx1: &[usize],
) -> Result<Ordering, usize> {

    for (&k0, &k1) in key_idx0.iter().zip(key_idx1) {
        let f0 = match rec_0.get(k0) {
            Some(f) => f.clone(),
            None => return Err(0),
        };
        let f1 = match rec_1.get(k1) {
            Some(f) => f.clone(),
            None => return Err(1),
        };
        
        match buf0[f0].cmp(&buf1[f1]) {
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
    use csvroll::index_builder::IndexBuilder;

    #[test]
    fn test_group() {
        struct TestCase {
            input: String,
            buf_len: usize,
            key_idx: Vec<usize>,
            want: Vec<(String, Index, Option<Range<usize>>)>,
        }

        let test_cases = vec![
            TestCase {
                input: "a,0\na,1\nb,0\nc,0".to_owned(),
                buf_len: 24,
                key_idx: vec![0],
                want: vec![
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    Some(0..2),),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    Some(2..3),),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    Some(3..4),),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    None,),
                ],
            },
            TestCase {
                input: "a,0\na,1\nb,0\nc,0".to_owned(),
                buf_len: 11,
                key_idx: vec![0],
                want: vec![
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    Some(0..2),),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    Some(2..3),),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    Some(3..4),),
                   ("a,0\na,1\nb,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11, 12..13, 14..15], vec![2, 4, 6, 8]),
                    None,),
                ],
            },
            TestCase {
                input: "a,0\na,1\nb,0\nc,0".to_owned(),
                buf_len: 12,
                key_idx: vec![0],
                want: vec![
                   ("a,0\na,1\nb,0\n".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11], vec![2, 4, 6]),
                    Some(0..2),),
                   ("b,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 4]),
                    Some(0..1),),
                   ("b,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 4]),
                    Some(1..2),),
                   ("b,0\nc,0".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 4]),
                    None,),
                ],
            },
            TestCase {
                input: "a,0\na,1\nb,0\nb,1".to_owned(),
                buf_len: 12,
                key_idx: vec![0],
                want: vec![
                   ("a,0\na,1\nb,0\n".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7, 8..9, 10..11], vec![2, 4, 6]),
                    Some(0..2),),
                   ("b,0\nb,1".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 4]),
                    Some(0..2),),
                   ("b,0\nb,1".to_owned(),
                    Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 4]),
                    None,),
                ],
            },
        ];

        for (i, t) in test_cases.into_iter().enumerate() {
            println!("test case: {}", i);
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

                

            



                        
                
        
