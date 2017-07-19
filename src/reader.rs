use super::record::Record;
use super::csv_core;

use std::io::{self, BufRead};
use std::error::Error;

pub struct Reader<R> {
    core: Box<csv_core::Reader>,
    rdr: io::BufReader<R>,
    state: ReaderState,
}

struct ReaderState {
    // reached EOF of the underlying reader 
    eof: bool,
}

impl<R> Reader<R> where R: io::Read {
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
