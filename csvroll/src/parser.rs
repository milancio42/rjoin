use rollbuf::RollBuf;
use super::index_builder::IndexBuilder;

use std::error::Error;
use std::cmp;
use std::ops::Range;
use std::io;

#[derive(Debug, Eq, PartialEq)]
pub struct Index {
    fields: Vec<Range<usize>>,
    records: Vec<usize>,
}

impl Index {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            fields: Vec::with_capacity(cap),
            records: Vec::with_capacity(cap),
        }
    }

    pub fn from_parts(fields: Vec<Range<usize>>, records: Vec<usize>) -> Self {
        Self {
            fields ,
            records ,
        }
    }

    #[inline]
    pub fn fields(&self) -> &[Range<usize>] {
        &self.fields[..]
    }

    #[inline]
    pub fn records(&self) -> &[usize] {
        &self.records[..]
    }

    #[inline]
    pub fn push_field(&mut self, f: Range<usize>) {
        self.fields.push(f);
    }

    #[inline]
    pub fn push_record(&mut self, r: usize) {
        self.records.push(r);
    }

    #[inline]
    pub fn get_record(&self, n: usize) -> Option<&[Range<usize>]> {
        if n >= self.records.len() {
            return None;
        }
        let end = match self.records.get(n) {
            Some(&end) => end,
            None => return None,
        };
        let start = match n.checked_sub(1).and_then(|i| self.records.get(i)) {
            Some(&start) => start,
            None => 0,
        };
        Some(&self.fields[start..end])
    }

}


pub struct Parser<R> {
    buf: RollBuf<R>,
    idx_builder: IndexBuilder,
    idx: Index,
    // the number of consumed records
    consumed: usize,
    parsed: usize,
    aux: Index,
}

impl<R> Parser<R> {
    pub fn from_parts(buf: RollBuf<R>, idx_builder: IndexBuilder) -> Self {
        Self {
            buf ,
            idx_builder ,
            idx: Index::new(),
            consumed: 0,
            parsed: 0,
            aux: Index::new(),
        }
    }
}

impl<R: io::Read> Parser<R> {
    pub fn parse(&mut self) -> Result<(&[u8], &Index), Box<Error>> {
        if self.consumed > 0 {
            let record_offset = cmp::min(self.consumed, self.idx.records.len());
            let field_offset = *self.idx.records.get(self.consumed - 1)
                                                .unwrap_or(&self.idx.fields.len());
            let buf_offset = match self.idx.fields.get(field_offset) {
                Some(f) => f.start,
                None => self.parsed,
            };
            self.buf.consume(buf_offset);
            self.buf.roll();
            roll_index(
                &mut self.idx,
                &mut self.aux,
                buf_offset,
                field_offset,
                record_offset,
            );
            self.parsed -= buf_offset;
        }
        let (s, is_buf_full) = self.buf.fill_buf()?;
        self.idx_builder.build(&s[self.parsed..], self.parsed, &mut self.idx);
        if is_buf_full {
            // we don't know yet if we reached EOF, so we drop the last incomplete field and record
            self.parsed = self.idx.fields.pop().unwrap_or(0..0).start;
            let _ = self.idx.records.pop();
        } else {
            // EOF
            self.parsed = self.idx.fields.last().unwrap_or(&(0..0)).end;
        }

        Ok((s, &self.idx))
    }

    #[inline]
    pub fn consume(&mut self, n: usize) {
        self.consumed = n;
    }
}
        
#[inline]
fn roll_index(
    idx: &mut Index,
    aux: &mut Index,
    buf_offset: usize,
    field_offset: usize,
    record_offset: usize,
)  {
    aux.fields.clear();
    aux.records.clear();
    aux.fields.extend_from_slice(&idx.fields[field_offset..]);
    aux.records.extend_from_slice(&idx.records[record_offset..]);

    idx.fields.clear();
    idx.records.clear();
    idx.fields.extend_from_slice(aux.fields.as_slice());
    idx.records.extend_from_slice(aux.records.as_slice());
    
    // reindex
    for f in idx.fields.iter_mut() {
        f.start -= buf_offset;
        f.end -= buf_offset;
    }
    for r in idx.records.iter_mut() {
        *r -= field_offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_get_record() {
        
        struct TestCase {
            idx: Index,
            n: usize,
            want: Option<Vec<Range<usize>>>
        }

        let test_cases = vec![
            TestCase {
                idx: Index::from_parts(vec![], vec![]),
                n: 0,
                want: None,
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5], vec![2, 3]),
                n: 0,
                want: Some(vec![0..1, 2..3]),
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5], vec![2, 3]),
                n: 1,
                want: Some(vec![4..5]),
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5], vec![2, 3]),
                n: 2,
                want: None,
            },
        ];
        for t in test_cases {
            assert_eq!(t.idx.get_record(t.n), t.want.as_ref().map(|f| f.as_slice()));
        }
    }

    #[test]
    fn test_roll_index() {
        struct TestCase {
            idx: Index,
            b_o: usize,
            f_o: usize,
            r_o: usize,
            want: Index,
        }

        let mut aux = Index::new();

        let test_cases = vec![
            TestCase {
                idx: Index::from_parts(vec![0..1], vec![1]),
                b_o: 0,
                f_o: 0,
                r_o: 0,
                want: Index::from_parts(vec![0..1], vec![1]),
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5], vec![2, 3]),
                b_o: 4,
                f_o: 2,
                r_o: 1,
                want: Index::from_parts(vec![0..1], vec![1]),
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5], vec![2, 3]),
                b_o: 6,
                f_o: 3,
                r_o: 2,
                want: Index::from_parts(vec![], vec![]),
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![2, 3]),
                b_o: 6,
                f_o: 3,
                r_o: 2,
                want: Index::from_parts(vec![0..1], vec![]),
            },
        ];
        for t in test_cases {
            let TestCase { mut idx, b_o, f_o, r_o, want } = t;
            roll_index(&mut idx, &mut aux, b_o, f_o, r_o);
            assert_eq!(idx, want);
        }
    }

    #[test]
    fn test_parser() {
        use rollbuf::RollBuf;

        struct TestCase {
            input: String,
            consume: Vec<usize>,
            want: Vec<(String, Index)>,
        }

        let test_cases = vec![
            TestCase {
                input: "a\nb\nc,d,e".to_owned(),
                consume: vec![1, 1, 1, 1],
                want: vec![
                    ("a\nb\nc,d".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5 ], vec![1, 2])),
                    ("b\nc,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5], vec![1])),
                    ("c,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5], vec![3])),
                    ("".to_owned(), Index::from_parts(vec![], vec![])),
                ],
            },
            TestCase {
                input: "a\nb\nc,d,e".to_owned(),
                consume: vec![2, 1, 1],
                want: vec![
                    ("a\nb\nc,d".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5 ], vec![1, 2])),
                    ("c,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5], vec![3])),
                    ("".to_owned(), Index::from_parts(vec![], vec![])),
                ],
            },
        ];

        for t in test_cases {
            let TestCase { input, consume, want } = t;
            let buf = RollBuf::with_capacity(7, input.as_bytes());
            let idx_builder = IndexBuilder::new(b',', b'\n');
            let mut parser = Parser::from_parts(buf, idx_builder);

            for (c, w) in consume.iter().zip(&want) {
                assert_eq!(parser.parse().unwrap(), (w.0.as_bytes(), &w.1));
                parser.consume(*c);
            }
        }
    }
}
            
