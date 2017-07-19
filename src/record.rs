use std::cmp;
use std::error::Error;
use std::ops::Range;

#[derive(Debug, Eq, PartialEq)]
pub struct Record {
    fields: Vec<u8>,
    fields_bounds: Bounds,
    key_fields: Vec<u8>,
    key_fields_bounds: Bounds,
    non_key_fields: Vec<u8>,
    non_key_fields_bounds: Bounds,
    // field numbers composing the key in the original order
    key_idx: Vec<usize>,
    // field numbers composing the key sorted in ascending order
    key_idx_asc: Vec<usize>,
}

impl Record {
    #[inline]
    pub fn load(&mut self, fields: &[u8], ends: &[usize]) -> Result<(), Box<Error>> {
        self.clear();
        self.fields.extend_from_slice(fields);
        self.fields_bounds.ends.extend_from_slice(ends);
        self.set_key_fields()?;
        self.set_non_key_fields()?;
        Ok(())
    }

    #[inline]
    pub fn fields_mut(&mut self) -> (&mut [u8], &mut [usize]) {
        (&mut self.fields, &mut self.fields_bounds.ends)
    }

    #[inline]
    pub fn expand_fields(&mut self) {
        let new_len = self.fields.len().checked_mul(2).unwrap();
        self.fields.resize(cmp::max(4, new_len), 0);
    }

    #[inline]
    pub fn expand_bounds(&mut self) {
        self.fields_bounds.expand();
    }

    #[inline]
    pub fn set_len(&mut self, len: usize) {
        self.fields_bounds.ends.resize(len, 0);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.fields.clear();
        self.fields_bounds.clear();
        self.key_fields.clear();
        self.key_fields_bounds.clear();
        self.non_key_fields.clear();
        self.non_key_fields_bounds.clear();
    }

    #[inline]
    pub fn get_field(&self, i: usize) -> Option<&[u8]> {
        self.fields_bounds.get(i).map(|r| &self.fields[r])
    }
        
    #[inline]
    pub fn get_key_field(&self, i: usize) -> Option<&[u8]> {
        self.key_fields_bounds.get(i).map(|r| &self.key_fields[r])
    }
        
    #[inline]
    pub fn get_non_key_field(&self, i: usize) -> Option<&[u8]> {
        self.non_key_fields_bounds.get(i).map(|r| &self.non_key_fields[r])
    }

    #[inline]
    pub fn set_key_fields(&mut self) -> Result<(), Box<Error>> {
        let mut end_last = 0;
        for &nf in &self.key_idx {
            match self.fields_bounds.get(nf) {
                Some(fr) => {
                    let f = &self.fields[fr];
                    self.key_fields.extend_from_slice(f);
                    let end = end_last + f.len();
                    self.key_fields_bounds.ends.push(end);
                    end_last = end;
                }
                None => return Err(format!("The key field <{}> not found in data", nf).into()),
            }
        }
        Ok(())
    }

    #[inline]
    pub fn set_non_key_fields(&mut self) -> Result<(), Box<Error>> {
        let mut fe_last = 0;
        let mut nkfe_last = 0;
        let mut nf = 0;
        let mut kit = self.key_idx_asc.iter();
        let mut ko = kit.next();
        for &fe in &self.fields_bounds.ends {
            let advance_key = match ko {
                Some(&k) => {
                    if k == nf {
                        true
                    } else {
                        false
                    }
                }
                None => false,
            };

            if advance_key {
                ko = kit.next();
                fe_last = fe;
            } else {
                self.non_key_fields.extend_from_slice(&self.fields[fe_last..fe]);
                let nkfe = nkfe_last + (fe - fe_last);
                self.non_key_fields_bounds.ends.push(nkfe);
                nkfe_last = nkfe;
                fe_last = fe;
            }
            nf += 1;
        }
        Ok(())
    }
}


#[derive(Debug, Eq, PartialEq)]
struct Bounds {
    ends: Vec<usize>,
}

impl Bounds {
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Bounds {
            ends: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn expand(&mut self) {
        let new_len = self.ends.len().checked_mul(2).unwrap();
        self.ends.resize(cmp::max(4, new_len), 0);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.ends.clear();
    }

    #[inline]
    fn get(&self, i: usize) -> Option<Range<usize>> {
        if i >= self.ends.len() {
            return None;
        }
        let end = match self.ends.get(i) {
            Some(&end) => end,
            None => return None,
        };
        let start = match i.checked_sub(1).and_then(|i| self.ends.get(i)) {
            Some(&start) => start,
            None => 0,
        };
        Some(start..end)
    }
}

pub struct RecordBuilder {
    fields_cap: usize,
    fields_bounds_cap: usize,
    key_fields_cap: usize,
    key_fields_bounds_cap: usize,
    non_key_fields_bounds_cap: usize,
    non_key_fields_cap: usize,
    key_idx: Result<Vec<usize>, Box<Error>>,
    key_idx_asc: Result<Vec<usize>, Box<Error>>,
}

impl Default for RecordBuilder {
    fn default() -> Self {
        RecordBuilder {
            fields_cap: 0,
            fields_bounds_cap: 0,
            key_fields_cap: 0,
            key_fields_bounds_cap: 0,
            non_key_fields_bounds_cap: 0,
            non_key_fields_cap: 0,
            key_idx: Ok(vec![0]),
            key_idx_asc: Ok(vec![0]),
        }
    }
}

impl RecordBuilder {
    pub fn fields_cap(mut self, cap: usize) -> Self {
        self.fields_cap = cap;
        self
    }
    pub fn fields_bounds_cap(mut self, cap: usize) -> Self {
        self.fields_bounds_cap = cap;
        self
    }

    pub fn key_fields_cap(mut self, cap: usize) -> Self {
        self.key_fields_cap = cap;
        self
    }
    pub fn key_fields_bounds_cap(mut self, cap: usize) -> Self {
        self.key_fields_bounds_cap = cap;
        self
    }

    pub fn non_key_fields_cap(mut self, cap: usize) -> Self {
        self.non_key_fields_cap = cap;
        self
    }

    pub fn non_key_fields_bounds_cap(mut self, cap: usize) -> Self {
        self.non_key_fields_bounds_cap = cap;
        self
    }

    pub fn keys(mut self, k: &[usize]) -> Self {
        let key_idx: Vec<usize> = k.into();
        let mut key_idx_asc: Vec<usize> = key_idx.clone();
        key_idx_asc.sort();

        self.key_idx_asc = Ok(key_idx_asc)
                            .and_then(|v| {
                                for w in v.windows(2) {
                                    if w[0] == w[1] {
                                        return Err("the key fields must be unique".into());
                                    }
                                }
                                Ok(v)
                            });
        // at this point we applied all the checks so we can safely write the key_idx
        self.key_idx = Ok(key_idx);
        self
    }

    pub fn build(self) -> Result<Record, Box<Error>> {
        let key_idx = self.key_idx?; 
        let key_idx_asc = self.key_idx_asc?; 
        let r = Record {
            fields: vec![0; self.fields_cap],
            fields_bounds: Bounds::with_capacity(self.fields_bounds_cap),
            key_fields: Vec::with_capacity(self.key_fields_cap),
            key_fields_bounds: Bounds::with_capacity(self.key_fields_bounds_cap),
            non_key_fields: Vec::with_capacity(self.non_key_fields_cap),
            non_key_fields_bounds: Bounds::with_capacity(self.non_key_fields_bounds_cap),
            // field numbers composing the key in the original order
            key_idx: key_idx,
            // field numbers composing the key sorted in ascending order
            key_idx_asc: key_idx_asc,
        };
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::RecordBuilder;

    #[test]
    fn record_1() {
        let mut rec = RecordBuilder::default().build().unwrap();
        
        rec.load(b"foobarquux", &[3,6,10]).unwrap();
        
        assert_eq!(rec.get_field(0), Some(&b"foo"[..]));
        assert_eq!(rec.get_field(1), Some(&b"bar"[..]));
        assert_eq!(rec.get_field(2), Some(&b"quux"[..]));
        assert_eq!(rec.get_field(3), None);
        assert_eq!(rec.get_field(4), None);

        // by default, the first field is the key
        assert_eq!(rec.get_key_field(0), Some(&b"foo"[..]));
        assert_eq!(rec.get_key_field(1), None);
        assert_eq!(rec.get_key_field(2), None);

        // by default, the first field is the key
        assert_eq!(rec.get_non_key_field(0), Some(&b"bar"[..]));
        assert_eq!(rec.get_non_key_field(1), Some(&b"quux"[..]));
        assert_eq!(rec.get_non_key_field(2), None);
        assert_eq!(rec.get_non_key_field(3), None);
    }

    #[test]
    fn record_2() {
        let mut rec = RecordBuilder::default()
                                    .keys(&[1])
                                    .build().unwrap();
        
        rec.load(b"foobarquux", &[3,6,10]).unwrap();
        
        assert_eq!(rec.get_field(0), Some(&b"foo"[..]));
        assert_eq!(rec.get_field(1), Some(&b"bar"[..]));
        assert_eq!(rec.get_field(2), Some(&b"quux"[..]));
        assert_eq!(rec.get_field(3), None);
        assert_eq!(rec.get_field(4), None);

        assert_eq!(rec.get_key_field(0), Some(&b"bar"[..]));
        assert_eq!(rec.get_key_field(1), None);
        assert_eq!(rec.get_key_field(2), None);

        assert_eq!(rec.get_non_key_field(0), Some(&b"foo"[..]));
        assert_eq!(rec.get_non_key_field(1), Some(&b"quux"[..]));
        assert_eq!(rec.get_non_key_field(2), None);
        assert_eq!(rec.get_non_key_field(3), None);
    }
}


