use rollbuf::RollBuf;
use super::index_builder::IndexBuilder;

use std::error::Error;
use std::cmp;
use std::ops::Range;

#[derive(Debug)]
pub struct Index {
    fields: Vec<Range<usize>>,
    records: Vec<usize>,
    aux_fields: Vec<Range<usize>>,
    aux_records: Vec<usize>,
}

impl Index {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            fields: Vec::with_capacity(cap),
            records: Vec::with_capacity(cap),
            aux_fields: Vec::with_capacity(cap),
            aux_records: Vec::with_capacity(cap),
        }
    }

    pub fn from_parts(fields: Vec<Range<usize>>, records: Vec<usize>) -> Self {
        Self {
            fields ,
            records ,
            aux_fields: Vec::new(),
            aux_records: Vec::new(),
        }
    }

    pub fn fields(&self) -> &[Range<usize>] {
        &self.fields[..]
    }

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

    #[inline]
    pub fn roll(&mut self, consumed: usize) -> usize {
        if consumed == 0 {
            return 0;
        }
        let buf_offset = match self.get_record(consumed) {
            Some(f) => f[0].start,
            None => match self.fields.last() {
                Some(ref rng) => rng.end,
                None => return 0,
            }
        };
        let f_offset = *self.records.get(consumed - 1).unwrap_or(&self.fields.len());
        let r_offset = cmp::min(consumed, self.records.len());

        // roll to the beginning
        self.aux_fields.clear();
        self.aux_records.clear();
        self.aux_fields.extend_from_slice(&self.fields[f_offset..]);
        self.aux_records.extend_from_slice(&self.records[r_offset..]);
        self.fields.clear();
        self.records.clear();
        self.fields.extend_from_slice(self.aux_fields.as_slice());
        self.records.extend_from_slice(self.aux_records.as_slice());
        
        // reindex
        for f in self.fields.iter_mut() {
            f.start -= buf_offset;
            f.end -= buf_offset;
        }
        for r in self.records.iter_mut() {
            *r -= f_offset;
        }
        buf_offset
    }
}

impl PartialEq for Index {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields && self.records == other.records
    }
}

impl Eq for Index {}

        
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
    fn test_index_roll() {
        struct TestCase {
            idx: Index,
            n: usize,
            want: (usize, Index),
        }

        let test_cases = vec![
            TestCase {
                idx: Index::from_parts(vec![0..1], vec![1]),
                n: 0,
                want: (0, Index::from_parts(vec![0..1], vec![1])),
            },
            TestCase {
                idx: Index::from_parts(vec![], vec![]),
                n: 1,
                want: (0, Index::from_parts(vec![], vec![])),
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5], vec![2, 3]),
                n: 1,
                want: (4, Index::from_parts(vec![0..1], vec![1])),
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5], vec![2, 3]),
                n: 2,
                want: (5, Index::from_parts(vec![], vec![])),
            },
            TestCase {
                idx: Index::from_parts(vec![0..1, 2..3, 4..5], vec![2, 3]),
                n: 3,
                want: (5, Index::from_parts(vec![], vec![])),
            },
        ];
        for t in test_cases {
            let TestCase { mut idx, n, want } = t;
            let offset = idx.roll(n);
            assert_eq!((offset, idx), (want.0, want.1));
        }
    }
}
            
