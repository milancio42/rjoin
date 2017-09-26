use std::io;
use std::error::Error;
use std::cmp;

const DAFAULT_BUF_SIZE: usize = 8 * 1024;

pub trait RollBuf {
    fn fill_buf(&mut self) -> Result<&[u8], Box<Error>>;
    fn consume(&mut self, n: usize);
    fn roll(&mut self);
}

#[derive(Debug)]
pub struct Buffer<R>  {
    inner: R,
    buf: Vec<u8>,
    pos: usize,
    end: usize,
    aux: Vec<u8>,
    rolled: bool,
}

impl<R: io::Read> Buffer<R> {
    pub fn new(inner: R) -> Buffer<R> {
        Self::with_capacity(DAFAULT_BUF_SIZE, inner)
    }

    pub fn with_capacity(cap: usize, inner: R) -> Buffer<R> {
        Buffer {
            inner ,
            buf: vec![0; cap],
            pos: 0,
            end: 0,
            aux: vec![0; cap],
            rolled: false,
        }
    }
}

impl<R: io::Read> RollBuf for Buffer<R> {
    fn fill_buf(&mut self) -> Result<&[u8], Box<Error>> {
        if self.pos >= self.end {
            debug_assert!(self.pos == self.end);
            self.end = self.inner.read(&mut self.buf)?;
            self.pos = 0;
        }

        if self.rolled {
            self.end += self.inner.read(&mut self.buf[self.end..])?;
            self.rolled = false;
        }
            
        Ok(&self.buf[self.pos..self.end])
    }

    fn consume(&mut self, n: usize) {
        self.pos = cmp::min(self.pos + n, self.end);
    }

    fn roll(&mut self) {
        self.aux.clear();
        self.aux.extend_from_slice(&self.buf[self.pos..self.end]);
        let new_end = self.aux.len();
        self.buf[0..new_end].copy_from_slice(&self.aux);
        self.pos = 0;
        self.end = new_end;
        self.rolled = true;
    }
}
        

#[cfg(test)]
mod tests {
    use super::{RollBuf, Buffer};

    #[test]
    fn buffer() {
        let inner: &[u8] = &[1, 2, 3, 4, 5, 6, 7];
        let mut b = Buffer::with_capacity(3, inner);

        // this should have no effect
        b.consume(2);

        {
            let out = b.fill_buf().unwrap();
            let want = &[1, 2, 3];
            assert_eq!(out, want);
        }

        b.consume(2);

        {
            let out = b.fill_buf().unwrap();
            let want = &[3];
            assert_eq!(out, want);
        }

        b.consume(1);

        {
            let out = b.fill_buf().unwrap();
            let want = &[4, 5, 6];
            assert_eq!(out, want);
        }
        
        b.consume(2);
        b.roll();
    
        {
            let out = b.fill_buf().unwrap();
            let want = &[6, 7];
            assert_eq!(out, want);
        }
        
        b.consume(2);
        {
            let out = b.fill_buf().unwrap();
            let want = &[];
            assert_eq!(out, want);
        }
    }
}


