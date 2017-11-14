use std::io;
use std::error::Error;
use std::cmp;

const DAFAULT_BUF_SIZE: usize = 8 * 1024;

#[derive(Debug)]
pub struct RollBuf<R>  {
    inner: R,
    buf: Vec<u8>,
    pos: usize,
    end: usize,
    aux: Vec<u8>,
    is_rolled: bool,
    max_cap: usize,
}

impl<R: io::Read> RollBuf<R> {
    pub fn new(inner: R) -> RollBuf<R> {
        Self::with_capacity(DAFAULT_BUF_SIZE, inner)
    }

    pub fn with_capacity(cap: usize, inner: R) -> RollBuf<R> {
        RollBuf {
            inner ,
            buf: vec![0; cap],
            pos: 0,
            end: 0,
            aux: vec![0; cap],
            is_rolled: false,
            max_cap: 1 << 24,
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn end_position(&self) -> usize {
        self.end
    }

    pub fn fill_buf(&mut self) -> Result<bool, Box<Error>> {
        if self.pos >= self.end {
            debug_assert!(self.pos == self.end);
            self.end = self.inner.read(&mut self.buf)?;
            self.pos = 0;
        }

        if self.is_rolled {
            self.end += self.inner.read(&mut self.buf[self.end..])?;
            self.is_rolled = false;
        }

        let is_full = self.buf.len() == self.end;
            
        Ok(is_full)
    }

    pub fn consume(&mut self, n: usize) {
        self.pos = cmp::min(self.pos + n, self.end);
    }

    pub fn roll(&mut self) {
        if self.pos > 0 {
            self.aux.clear();
            self.aux.extend_from_slice(&self.buf[self.pos..self.end]);
            let new_end = self.aux.len();
            self.buf[0..new_end].copy_from_slice(&self.aux);
            self.pos = 0;
            self.end = new_end;
        } else {
            let old_len = self.buf.len();
            let reserved = cmp::min(old_len, self.max_cap - old_len);
            self.buf.reserve(reserved);
            self.buf.extend((0..reserved).map(|_| 0));
        }
        self.is_rolled = true;
    }

    pub fn contents(&self) -> &[u8] {
        &self.buf[self.pos..self.end]
    }

    pub fn is_full(&self) -> bool {
        self.buf.len() == self.end
    }
}
        

#[cfg(test)]
mod tests {
    use super::{RollBuf};

    #[test]
    fn test_rollbuf_no_extend() {
        let inner: &[u8] = &[1, 2, 3, 4, 5, 6, 7];
        let mut b = RollBuf::with_capacity(3, inner);

        struct TestCase {
            consume: usize,
            roll: bool,
            want: (Vec<u8>, bool),
        }

        let test_cases = vec![
            TestCase { consume: 2, roll: false, want: (vec![1, 2, 3], true)  },
            TestCase { consume: 2, roll: false, want: (vec![3],       true)  },
            TestCase { consume: 1, roll: false, want: (vec![4, 5, 6], true)  },
            TestCase { consume: 2, roll: true,  want: (vec![6, 7],    false) },
            TestCase { consume: 2, roll: false, want: (vec![],        false) },
        ];

        for t in test_cases {
            b.consume(t.consume);
            if t.roll {
                b.roll();
            }
            let is_full = b.fill_buf().unwrap();
            let contents = b.contents();
            assert_eq!((contents, is_full), (t.want.0.as_slice(), t.want.1));
        }
    }

    #[test]
    fn test_rollbuf_extend() {
        let inner: &[u8] = &[1, 2, 3, 4, 5, 6, 7];
        let mut b = RollBuf::with_capacity(3, inner);

        struct TestCase {
            consume: usize,
            roll: bool,
            want: (Vec<u8>, bool),
        }

        let test_cases = vec![
            TestCase { consume: 2, roll: false, want: (vec![1, 2, 3],           true)  },
            TestCase { consume: 0, roll: true,  want: (vec![1, 2, 3, 4, 5, 6],  true)  },
            TestCase { consume: 4, roll: false, want: (vec![5, 6],              true)  },
            TestCase { consume: 2, roll: true,  want: (vec![7],                 false) },
            TestCase { consume: 2, roll: false, want: (vec![],                  false) },
        ];

        for t in test_cases {
            b.consume(t.consume);
            if t.roll {
                b.roll();
            }
            let is_full = b.fill_buf().unwrap();
            let contents = b.contents();
            assert_eq!((contents, is_full), (t.want.0.as_slice(), t.want.1));
        }
    }
}


