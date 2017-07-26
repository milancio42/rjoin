use record::{Group, RecIter};
use std::io;
use std::error::Error;

pub trait Print<W:io::Write> {
    fn print_left(&mut self, w: &mut W, g: &Group) -> Result<(), Box<Error>>;
    fn print_right(&mut self, w: &mut W, g: &Group) -> Result<(), Box<Error>>;
    fn print_both(&mut self, w: &mut W, g0: &Group, g1: &Group) -> Result<(), Box<Error>>;
}

pub struct KeyFirst {
    delimiter: u8,
    terminator: u8,
}

impl Default for KeyFirst {
    fn default() -> Self {
        KeyFirst {
            delimiter: b',',
            terminator: b'\n',
        }
    }
}

impl KeyFirst {
    pub fn new(delimiter: u8, terminator: u8) -> Self {
        KeyFirst {
            delimiter: delimiter,
            terminator: terminator,
        }
    }
}

impl<W:io::Write> Print<W> for KeyFirst {
    #[inline]
    fn print_left(&mut self, w: &mut W, g: &Group) -> Result<(), Box<Error>> {
        print_single(w, g, self.delimiter, self.terminator)
    }
        
    #[inline]
    fn print_right(&mut self, w: &mut W, g: &Group) -> Result<(), Box<Error>> {
        print_single(w, g, self.delimiter, self.terminator)
    }
        
    #[inline]
    fn print_both(&mut self, w: &mut W, g0: &Group, g1: &Group) -> Result<(), Box<Error>> {
        let mut is_first: bool;
        for (rf0, rfe0) in g0.non_key_iter() {
            for (rf1, rfe1) in g1.non_key_iter() {
                is_first = true;
                
                for f in g0.first_key_iter() {
                    if !is_first {
                        w.write_all(&[self.delimiter])?;
                    } else {
                        is_first = false;
                    }
                    w.write_all(f)?;
                }
                for f in RecIter::from_fields(rf0, rfe0) {
                    w.write_all(&[self.delimiter])?;
                    w.write_all(f)?;
                }
                for f in RecIter::from_fields(rf1, rfe1) {
                    w.write_all(&[self.delimiter])?;
                    w.write_all(f)?;
                }
            }
            w.write_all(&[self.terminator])?;
        }
        Ok(())
    }
}
        
    
fn print_single<W:io::Write>(
    w: &mut W,
    g: &Group,
    delimiter: u8,
    terminator: u8,
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
