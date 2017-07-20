use super::record::Record;
use super::csv_core;

use std::io::{self, BufRead};
use std::error::Error;

pub struct ReaderBuilder {
    buffer_cap: usize,
    core_builder: Box<csv_core::ReaderBuilder>,
}

impl Default for ReaderBuilder {
    fn default() -> Self {
        ReaderBuilder {
            buffer_cap: 8 * (1<<10),
            core_builder: Box::new(csv_core::ReaderBuilder::default()),
        }
    }
}

impl ReaderBuilder {
    pub fn delimiter(mut self, del: u8) -> Self {
        self.core_builder.delimiter(del);
        self
    }
    
    pub fn terminator(mut self, term: csv_core::Terminator) -> Self {
        self.core_builder.terminator(term);
        self
    }

    pub fn buffer_capacity(mut self, cap: usize) -> Self {
        self.buffer_cap = cap;
        self
    }

    pub fn from_reader<R: io::Read>(&self, rdr: R) -> Reader<R> {
        Reader {
            core: Box::new(self.core_builder.build()),
            rdr: io::BufReader::with_capacity(self.buffer_cap, rdr),
            state: ReaderState { eof: false },
        }
    }
}

pub struct Reader<R> {
    core: Box<csv_core::Reader>,
    rdr: io::BufReader<R>,
    state: ReaderState,
}

struct ReaderState {
    // reached EOF of the underlying reader 
    eof: bool,
}

impl<R: io::Read> Reader<R> {
    #[inline]
    pub fn read_record(
        &mut self, 
        record: &mut Record,
    ) -> Result<bool, Box<Error>> {
        use csv_core::ReadRecordResult::*;

        record.clear();
        if self.state.eof {
            return Ok(false);
        }
        let (mut outlen, mut endlen) = (0, 0);
        loop {
            let (res, nin, nout, nend) = {
               let input = self.rdr.fill_buf()?;
               let (mut fields, mut ends) = record.fields_mut();
               self.core.read_record(input, &mut fields[outlen..], &mut ends[endlen..])
            };
            self.rdr.consume(nin);
            outlen += nout;
            endlen += nend;
            match res {
                InputEmpty => continue,
                OutputFull => {
                    record.expand_fields();
                    continue;
                }
                OutputEndsFull => {
                    record.expand_bounds();
                    continue;
                }
                Record => {
                    record.set_len(endlen);
                    record.set_key_fields()?;
                    record.set_non_key_fields()?;
                    return Ok(true);
                }
                End => {
                    self.state.eof = true;
                    return Ok(false);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ReaderBuilder;
    use record::RecordBuilder;
    use csv_core::Terminator;
    
    #[test]
    fn read_1() {
        let data = "1,Aragorn,The Lord of the Rings\n2,Jon Snow,The Song of Ice and Fire";
        let mut rdr = ReaderBuilder::default().from_reader(data.as_bytes());
        let mut rec = RecordBuilder::default().build().unwrap();

        let _ = rdr.read_record(&mut rec).unwrap();

        assert_eq!(rec.get_field(0), Some(&b"1"[..]));
        assert_eq!(rec.get_field(1), Some(&b"Aragorn"[..]));
        assert_eq!(rec.get_field(2), Some(&b"The Lord of the Rings"[..]));
        assert_eq!(rec.get_field(3), None);
        assert_eq!(rec.get_field(4), None);

        assert_eq!(rec.get_key_field(0), Some(&b"1"[..]));
        assert_eq!(rec.get_key_field(1), None);
        assert_eq!(rec.get_key_field(2), None);

        assert_eq!(rec.get_non_key_field(0), Some(&b"Aragorn"[..]));
        assert_eq!(rec.get_non_key_field(1), Some(&b"The Lord of the Rings"[..]));
        assert_eq!(rec.get_non_key_field(2), None);
        assert_eq!(rec.get_non_key_field(3), None);

        let _ = rdr.read_record(&mut rec).unwrap();

        assert_eq!(rec.get_field(0), Some(&b"2"[..]));
        assert_eq!(rec.get_field(1), Some(&b"Jon Snow"[..]));
        assert_eq!(rec.get_field(2), Some(&b"The Song of Ice and Fire"[..]));
        assert_eq!(rec.get_field(3), None);
        assert_eq!(rec.get_field(4), None);

        assert_eq!(rec.get_key_field(0), Some(&b"2"[..]));
        assert_eq!(rec.get_key_field(1), None);
        assert_eq!(rec.get_key_field(2), None);

        assert_eq!(rec.get_non_key_field(0), Some(&b"Jon Snow"[..]));
        assert_eq!(rec.get_non_key_field(1), Some(&b"The Song of Ice and Fire"[..]));
        assert_eq!(rec.get_non_key_field(2), None);
        assert_eq!(rec.get_non_key_field(3), None);
    }

    #[test]
    fn read_2() {
        let data = "1;Aragorn;The Lord of the Rings$2;Jon Snow;The Song of Ice and Fire";
        let mut rdr = ReaderBuilder::default().delimiter(b';')
                                              .terminator(Terminator::Any(b'$'))
                                              .from_reader(data.as_bytes());
        let mut rec = RecordBuilder::default().build().unwrap();

        let _ = rdr.read_record(&mut rec).unwrap();

        assert_eq!(rec.get_field(0), Some(&b"1"[..]));
        assert_eq!(rec.get_field(1), Some(&b"Aragorn"[..]));
        assert_eq!(rec.get_field(2), Some(&b"The Lord of the Rings"[..]));
        assert_eq!(rec.get_field(3), None);
        assert_eq!(rec.get_field(4), None);

        assert_eq!(rec.get_key_field(0), Some(&b"1"[..]));
        assert_eq!(rec.get_key_field(1), None);
        assert_eq!(rec.get_key_field(2), None);

        assert_eq!(rec.get_non_key_field(0), Some(&b"Aragorn"[..]));
        assert_eq!(rec.get_non_key_field(1), Some(&b"The Lord of the Rings"[..]));
        assert_eq!(rec.get_non_key_field(2), None);
        assert_eq!(rec.get_non_key_field(3), None);

        let _ = rdr.read_record(&mut rec).unwrap();

        assert_eq!(rec.get_field(0), Some(&b"2"[..]));
        assert_eq!(rec.get_field(1), Some(&b"Jon Snow"[..]));
        assert_eq!(rec.get_field(2), Some(&b"The Song of Ice and Fire"[..]));
        assert_eq!(rec.get_field(3), None);
        assert_eq!(rec.get_field(4), None);

        assert_eq!(rec.get_key_field(0), Some(&b"2"[..]));
        assert_eq!(rec.get_key_field(1), None);
        assert_eq!(rec.get_key_field(2), None);

        assert_eq!(rec.get_non_key_field(0), Some(&b"Jon Snow"[..]));
        assert_eq!(rec.get_non_key_field(1), Some(&b"The Song of Ice and Fire"[..]));
        assert_eq!(rec.get_non_key_field(2), None);
        assert_eq!(rec.get_non_key_field(3), None);
    }
}
