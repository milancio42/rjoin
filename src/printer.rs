use record::{Group, Record, RecIter};
use std::io;
use std::error::Error;

/// A trait for printing `Group`s in a desired format.
pub trait PrintGroup<W:io::Write> {
    /// Print the left group into `w`.
    fn print_left(&mut self, w: &mut W, g: &Group) -> Result<(), Box<Error>>;
    /// Print the right group into `w`.
    fn print_right(&mut self, w: &mut W, g: &Group) -> Result<(), Box<Error>>;
    /// Print both groups into `w`.
    fn print_both(&mut self, w: &mut W, g0: &Group, g1: &Group) -> Result<(), Box<Error>>;
}

/// A trait for printing `Record` in a desired format.
pub trait PrintRecord<W:io::Write> {
    /// Print the left record into `w`.
    fn print_left(&mut self, w: &mut W, r: &Record) -> Result<(), Box<Error>>;
    /// Print the right record into `w`.
    fn print_right(&mut self, w: &mut W, r: &Record) -> Result<(), Box<Error>>;
    /// Print both records into `w`.
    fn print_both(&mut self, w: &mut W, r0: &Record, r1: &Record) -> Result<(), Box<Error>>;
}

/// Print the records/groups in the following format: first the key fields followed by non-key
/// fields.
#[derive(Clone, Copy)]
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

impl<W:io::Write> PrintGroup<W> for KeyFirst {
    #[inline]
    fn print_left(&mut self, w: &mut W, g: &Group) -> Result<(), Box<Error>> {
        print_single_group(w, g, self.delimiter, self.terminator)
    }
        
    #[inline]
    fn print_right(&mut self, w: &mut W, g: &Group) -> Result<(), Box<Error>> {
        print_single_group(w, g, self.delimiter, self.terminator)
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
                w.write_all(&[self.terminator])?;
            }
        }
        Ok(())
    }
}
        
impl<W:io::Write> PrintRecord<W> for KeyFirst {
    #[inline]
    fn print_left(&mut self, w: &mut W, r: &Record) -> Result<(), Box<Error>> {
        print_single_rec(w, r, self.delimiter, self.terminator)
    }
        
    #[inline]
    fn print_right(&mut self, w: &mut W, r: &Record) -> Result<(), Box<Error>> {
        print_single_rec(w, r, self.delimiter, self.terminator)
    }
        
    #[inline]
    fn print_both(&mut self, w: &mut W, r0: &Record, r1: &Record) -> Result<(), Box<Error>> {
        let mut is_first = true;
        
        for f in r0.key_iter() {
            if !is_first {
                w.write_all(&[self.delimiter])?;
            } else {
                is_first = false;
            }
            w.write_all(f)?;
        }
        for f in r0.non_key_iter() {
            w.write_all(&[self.delimiter])?;
            w.write_all(f)?;
        }
        for f in r1.non_key_iter() {
            w.write_all(&[self.delimiter])?;
            w.write_all(f)?;
        }
        w.write_all(&[self.terminator])?;
        Ok(())
    }
}
    
#[inline]
fn print_single_group<W:io::Write>(
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

#[inline]
fn print_single_rec<W:io::Write>(
    w: &mut W,
    r: &Record,
    delimiter: u8,
    terminator: u8,
) -> Result<(), Box<Error>> {
    let mut is_first = true;
        
    for f in r.key_iter() {
        if !is_first {
            w.write_all(&[delimiter])?;
        } else {
            is_first = false;
        }
        w.write_all(f)?;
    }
    for f in r.non_key_iter() {
        w.write_all(&[delimiter])?;
        w.write_all(f)?;
    }
    w.write_all(&[terminator])?;
    Ok(())
}
