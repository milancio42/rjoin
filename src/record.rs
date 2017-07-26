use std::cmp;
use std::error::Error;
use std::ops::Range;

pub struct RecordBuilder {
    fields_cap: usize,
    key_fields_cap: usize,
    non_key_fields_cap: usize,
    key_idx: Result<Vec<usize>, Box<Error>>,
    key_idx_asc: Result<Vec<usize>, Box<Error>>,
}

impl Default for RecordBuilder {
    fn default() -> Self {
        RecordBuilder {
            fields_cap: 0,
            key_fields_cap: 0,
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

    pub fn key_fields_cap(mut self, cap: usize) -> Self {
        self.key_fields_cap = cap;
        self
    }

    pub fn non_key_fields_cap(mut self, cap: usize) -> Self {
        self.non_key_fields_cap = cap;
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
            fields: Vec::with_capacity(self.fields_cap),
            fields_bounds: Bounds::with_capacity(self.fields_cap),
            key_fields: Vec::with_capacity(self.key_fields_cap),
            key_fields_bounds: Bounds::with_capacity(self.key_fields_cap),
            non_key_fields: Vec::with_capacity(self.non_key_fields_cap),
            non_key_fields_bounds: Bounds::with_capacity(self.non_key_fields_cap),
            // field numbers composing the key in the original order
            key_idx: key_idx,
            // field numbers composing the key sorted in ascending order
            key_idx_asc: key_idx_asc,
        };
        Ok(r)
    }
}

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

    #[inline]
    pub fn iter<'r>(&'r self) -> RecIter<'r> {
        RecIter {
           f: &self.fields,
           fe: &self.fields_bounds.ends,
           end_last: 0,
           i: 0,
        }
    }

    #[inline]
    pub fn key_iter<'r>(&'r self) -> RecIter<'r> {
        RecIter {
           f: &self.key_fields,
           fe: &self.key_fields_bounds.ends,
           end_last: 0,
           i: 0,
        }
    }

    #[inline]
    pub fn non_key_iter<'r>(&'r self) -> RecIter<'r> {
        RecIter {
           f: &self.non_key_fields,
           fe: &self.non_key_fields_bounds.ends,
           end_last: 0,
           i: 0,
        }
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
        get_bound(&self.ends, i)
    }

    #[inline]
    fn end(&self) -> usize {
        end(&self.ends)
    }
}

pub struct GroupBuilder {
    fields_cap: usize,
    first_key_fields_cap: usize,
    non_key_fields_cap: usize,
}

impl Default for GroupBuilder {
    fn default() -> Self {
        GroupBuilder {
            fields_cap: 0,
            first_key_fields_cap: 0,
            non_key_fields_cap: 0,
        }
    }
}

impl GroupBuilder {
    pub fn fields_cap(mut self, cap: usize) -> Self {
        self.fields_cap = cap;
        self
    }

    pub fn first_key_fields_cap(mut self, cap: usize) -> Self {
        self.first_key_fields_cap = cap;
        self
    }

    pub fn non_key_fields_cap(mut self, cap: usize) -> Self {
        self.non_key_fields_cap = cap;
        self
    }

    pub fn from_record(self, rec: Record) -> Group {
        Group {
            look_ahead: rec,
            fields: Vec::with_capacity(self.fields_cap),
            fields_bounds: Bounds::with_capacity(self.fields_cap),
            recs: Bounds::with_capacity(self.fields_cap),
            first_key_fields: Vec::with_capacity(self.first_key_fields_cap),
            first_key_fields_bounds: Bounds::with_capacity(self.first_key_fields_cap),
            non_key_fields: Vec::with_capacity(self.non_key_fields_cap),
            non_key_fields_bounds: Bounds::with_capacity(self.non_key_fields_cap),
            non_key_recs: Bounds::with_capacity(self.non_key_fields_cap),
            is_fused: false,
            is_first: true,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Group {
    look_ahead: Record,
    fields: Vec<u8>,
    fields_bounds: Bounds,
    recs: Bounds,
    first_key_fields: Vec<u8>,
    first_key_fields_bounds: Bounds,
    non_key_fields: Vec<u8>,
    non_key_fields_bounds: Bounds,
    non_key_recs: Bounds,
    is_fused: bool,
    is_first: bool,
}

impl Group {
    #[inline]
    pub fn clear(&mut self) {
        self.fields.clear();
        self.fields_bounds.clear();
        self.recs.clear();
        self.first_key_fields.clear();
        self.first_key_fields_bounds.clear();
        self.non_key_fields.clear();
        self.non_key_fields_bounds.clear();
        self.non_key_recs.clear();
    }
        
    #[inline]
    pub fn is_fused(&self) -> bool {
        self.is_fused
    }

    #[inline]
    pub fn fused(&mut self) {
        self.is_fused = true;
    }

    #[inline]
    pub fn is_first(&self) -> bool {
        self.is_first
    }

    #[inline]
    pub fn not_first(&mut self) {
        self.is_first = false;
    }

    #[inline]
    pub fn push_rec(&mut self) {
        self.fields_bounds.ends.push(self.fields.len());
        let e = self.look_ahead.fields_bounds.end();
        self.fields.extend_from_slice(&self.look_ahead.fields[..e]);
        self.fields_bounds.ends.extend_from_slice(&self.look_ahead.fields_bounds.ends);
        self.recs.ends.push(self.fields_bounds.ends.len());

        if self.first_key_fields.is_empty() {
            self.first_key_fields.extend_from_slice(&self.look_ahead.key_fields);
            self.first_key_fields_bounds.ends.extend_from_slice(&self.look_ahead
                                                                     .key_fields_bounds.ends);
        }

        self.non_key_fields_bounds.ends.push(self.non_key_fields.len());
        self.non_key_fields.extend_from_slice(&self.look_ahead.non_key_fields);
        self.non_key_fields_bounds.ends.extend_from_slice(&self.look_ahead
                                                               .non_key_fields_bounds.ends);
        self.non_key_recs.ends.push(self.non_key_fields_bounds.ends.len());
    }

    #[inline]
    pub fn look_ahead_mut(&mut self) -> &mut Record {
        &mut self.look_ahead
    }

    #[inline]
    pub fn is_group(&self) -> Result<bool, Box<Error>> {
        match cmp_keys(&self.look_ahead.key_fields,
                       &self.look_ahead.key_fields_bounds.ends,
                       &self.first_key_fields,
                       &self.first_key_fields_bounds.ends) {
            cmp::Ordering::Less => return Err("The records are not sorted in ascending order"
                                                 .into()),
            cmp::Ordering:: Greater=> Ok(false),
            cmp::Ordering::Equal => Ok(true),
        }
    }

    #[inline]
    pub fn cmp_keys(&self, other: &Group) -> cmp::Ordering {
        cmp_keys(&self.first_key_fields,
                 &self.first_key_fields_bounds.ends,
                 &other.first_key_fields,
                 &other.first_key_fields_bounds.ends)
    }

    #[inline]
    pub fn get_field(&self, rec_i: usize, field_i: usize) -> Option<&[u8]> {
        self.recs.get(rec_i).and_then(|r| get_bound_offset(&self.fields_bounds.ends[r], field_i))
                            .map(|(o, r)| &self.fields[o..][r])
    }

    #[inline]
    pub fn get_first_key_field(&self, i: usize) -> Option<&[u8]> {
        self.first_key_fields_bounds.get(i).map(|r| &self.first_key_fields[r])
    }

    #[inline]
    pub fn get_non_key_field(&self, rec_i: usize, field_i: usize) -> Option<&[u8]> {
        self.non_key_recs.get(rec_i).and_then(|r| get_bound_offset(&self.non_key_fields_bounds
                                                                        .ends[r], field_i))
                            .map(|(o, r)| &self.non_key_fields[o..][r])
    }

    #[inline]
    pub fn iter<'g>(&'g self) -> GroupIter<'g> {
        GroupIter {
           f: &self.fields,
           fe: &self.fields_bounds.ends,
           r: &self.recs.ends,
           r_end_last: 0,
           i: 0,
        }
    }

    #[inline]
    pub fn first_key_iter<'r>(&'r self) -> RecIter<'r> {
        RecIter {
           f: &self.first_key_fields,
           fe: &self.first_key_fields_bounds.ends,
           end_last: 0,
           i: 0,
        }
    }

    #[inline]
    pub fn non_key_iter<'g>(&'g self) -> GroupIter<'g> {
        GroupIter {
           f: &self.non_key_fields,
           fe: &self.non_key_fields_bounds.ends,
           r: &self.non_key_recs.ends,
           r_end_last: 0,
           i: 0,
        }
    }

}

#[inline]
fn get_bound(ends: &[usize], i: usize) -> Option<Range<usize>> {
    if i >= ends.len() {
        return None;
    }
    let end = match ends.get(i) {
        Some(&end) => end,
        None => return None,
    };
    let start = match i.checked_sub(1).and_then(|i| ends.get(i)) {
        Some(&start) => start,
        None => 0,
    };
    Some(start..end)
}

#[inline]
fn get_bound_offset(ends: &[usize], i: usize) -> Option<(usize, Range<usize>)> {
    let offset = ends[0];
    let ends = &ends[1..];
    if i >= ends.len() {
        return None;
    }
    let end = match ends.get(i) {
        Some(&end) => end,
        None => return None,
    };
    let start = match i.checked_sub(1).and_then(|i| ends.get(i)) {
        Some(&start) => start,
        None => 0,
    };
    Some((offset, start..end))
}

#[inline]
fn end(ends: &[usize]) -> usize {
    ends.last().map_or(0, |i| *i)
}
    

#[inline]
fn cmp_keys(
    fields: &[u8],
    bounds: &[usize],
    other_fields: &[u8],
    other_bounds: &[usize],
) -> cmp::Ordering {
    use self::cmp::Ordering::*;

    let mut e0_last = 0;
    let mut e1_last = 0;

    // we assume the fields are of equal length
    for (&e0, &e1) in bounds.iter().zip(other_bounds) {
        match fields[e0_last..e0].cmp(&other_fields[e1_last..e1]) {
            Less => return Less,
            Greater => return Greater,
            Equal => {
                e0_last = e0;
                e1_last = e1;
            }
        }
    }
    Equal
}

pub struct RecIter<'r> {
    f: &'r [u8],
    fe: &'r [usize],
    end_last: usize,
    i: usize,
}

impl<'r> RecIter<'r> {
    pub fn from_fields(fields: &'r [u8], ends: &'r [usize]) -> Self {
        RecIter {
            f: fields,
            fe: ends,
            end_last: 0,
            i: 0,
        }
    }
}

impl<'r> Iterator for RecIter<'r> {
    type Item = &'r [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.fe.len() {
            None
        } else {
            let start = self.end_last;
            let end = self.fe[self.i];
            self.i += 1;
            self.end_last = end;
            Some(&self.f[start..end])
        }
    }
}

pub struct GroupIter<'g> {
    f: &'g [u8],
    fe: &'g [usize],
    r: &'g [usize],
    r_end_last: usize,
    i: usize,
}

impl<'g> Iterator for GroupIter<'g> {
    type Item = (&'g [u8], &'g [usize]);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.r.len() {
            None
        } else {
            let r_start = self.r_end_last;
            let r_end = self.r[self.i];
            let fe = &self.fe[r_start..r_end];
            let offset = fe[0];
            let f_end = end(fe);
            self.i += 1;
            self.r_end_last = r_end;
            Some((&self.f[offset..][..f_end], &fe[1..]))
        }
    }
}

    
#[cfg(test)]
mod tests {
    use super::{RecordBuilder, GroupBuilder, RecIter};
    use std::cmp::Ordering;

    #[test]
    fn record_0() {
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
    fn record_1() {
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

    #[test]
    fn record_iter_0() {
        let mut rec = RecordBuilder::default().build().unwrap();
        
        rec.load(b"foobarquux", &[3,6,10]).unwrap();

        let mut r_it = rec.iter();

        assert_eq!(r_it.next().unwrap(), &b"foo"[..]);
        assert_eq!(r_it.next().unwrap(), &b"bar"[..]);
        assert_eq!(r_it.next().unwrap(), &b"quux"[..]);
        assert_eq!(r_it.next(), None);
        assert_eq!(r_it.next(), None);
        
        let mut rk_it = rec.key_iter();

        assert_eq!(rk_it.next().unwrap(), &b"foo"[..]);
        assert_eq!(rk_it.next(), None);
        assert_eq!(rk_it.next(), None);

        let mut rnk_it = rec.non_key_iter();

        assert_eq!(rnk_it.next().unwrap(), &b"bar"[..]);
        assert_eq!(rnk_it.next().unwrap(), &b"quux"[..]);
        assert_eq!(rnk_it.next(), None);
        assert_eq!(rnk_it.next(), None);
    }

    #[test]
    fn record_iter_1() {
        let fields = b"foobarquux";
        let fields_ends = [3,6,10];
        let mut r_it = RecIter::from_fields(fields, &fields_ends[..]);

        assert_eq!(r_it.next().unwrap(), &b"foo"[..]);
        assert_eq!(r_it.next().unwrap(), &b"bar"[..]);
        assert_eq!(r_it.next().unwrap(), &b"quux"[..]);
        assert_eq!(r_it.next(), None);
        assert_eq!(r_it.next(), None);
    }

    #[test]
    fn group_0() {
        let rec = RecordBuilder::default().build().unwrap();
        let mut g = GroupBuilder::default().from_record(rec);
        
        g.look_ahead_mut().load(b"foobarquux", &[3,6,10]).unwrap();
        g.push_rec();
        g.look_ahead_mut().load(b"foofortytwo", &[3,8,11]).unwrap();
        g.push_rec();
        
        assert_eq!(g.get_field(0, 0), Some(&b"foo"[..]));
        assert_eq!(g.get_field(0, 1), Some(&b"bar"[..]));
        assert_eq!(g.get_field(0, 2), Some(&b"quux"[..]));
        assert_eq!(g.get_field(0, 3), None);
        assert_eq!(g.get_field(0, 4), None);

        assert_eq!(g.get_field(1, 0), Some(&b"foo"[..]));
        assert_eq!(g.get_field(1, 1), Some(&b"forty"[..]));
        assert_eq!(g.get_field(1, 2), Some(&b"two"[..]));
        assert_eq!(g.get_field(1, 3), None);
        assert_eq!(g.get_field(1, 4), None);

        // by default, the first field is the key
        assert_eq!(g.get_first_key_field(0), Some(&b"foo"[..]));
        assert_eq!(g.get_first_key_field(1), None);
        assert_eq!(g.get_first_key_field(2), None);

        // by default, the first field is the key
        assert_eq!(g.get_non_key_field(0, 0), Some(&b"bar"[..]));
        assert_eq!(g.get_non_key_field(0, 1), Some(&b"quux"[..]));
        assert_eq!(g.get_non_key_field(0, 2), None);
        assert_eq!(g.get_non_key_field(0, 3), None);

        assert_eq!(g.get_non_key_field(1, 0), Some(&b"forty"[..]));
        assert_eq!(g.get_non_key_field(1, 1), Some(&b"two"[..]));
        assert_eq!(g.get_non_key_field(1, 2), None);
        assert_eq!(g.get_non_key_field(1, 3), None);
    }

    #[test]
    fn group_iter() {
        let rec = RecordBuilder::default().build().unwrap();
        let mut g = GroupBuilder::default().from_record(rec);
        
        g.look_ahead_mut().load(b"foobarquux", &[3,6,10]).unwrap();
        g.push_rec();
        g.look_ahead_mut().load(b"foofortytwo", &[3,8,11]).unwrap();
        g.push_rec();

        let mut g_it = g.iter();
        assert_eq!(g_it.next().unwrap(), (&b"foobarquux"[..], &[3,6,10][..]));
        assert_eq!(g_it.next().unwrap(), (&b"foofortytwo"[..], &[3,8,11][..]));
        assert_eq!(g_it.next(), None);
        assert_eq!(g_it.next(), None);
        
        let mut gk_it = g.first_key_iter();
        assert_eq!(gk_it.next().unwrap(), &b"foo"[..]);
        assert_eq!(gk_it.next(), None);

        let mut gnk_it = g.non_key_iter();
        assert_eq!(gnk_it.next().unwrap(), (&b"barquux"[..], &[3,7][..]));
        assert_eq!(gnk_it.next().unwrap(), (&b"fortytwo"[..], &[5,8][..]));
        assert_eq!(gnk_it.next(), None);
        assert_eq!(gnk_it.next(), None);
    }

    #[test]
    fn group_cmp_keys_equal() {
        let r0 = RecordBuilder::default().build().unwrap();
        let mut g0 = GroupBuilder::default().from_record(r0);

        let r1 = RecordBuilder::default().build().unwrap();
        let mut g1 = GroupBuilder::default().from_record(r1);

        g0.look_ahead_mut().load(b"colorblue", &[5,9]).unwrap();
        g0.push_rec();
        g0.look_ahead_mut().load(b"colorgreen", &[5,10]).unwrap();
        g0.push_rec();

        g1.look_ahead_mut().load(b"colorred", &[5,8]).unwrap();
        g1.push_rec();

        assert_eq!(g0.cmp_keys(&g1), Ordering::Equal);
    }

    #[test]
    fn group_cmp_keys_less() {
        let r0 = RecordBuilder::default().build().unwrap();
        let mut g0 = GroupBuilder::default().from_record(r0);

        let r1 = RecordBuilder::default().build().unwrap();
        let mut g1 = GroupBuilder::default().from_record(r1);

        g0.look_ahead_mut().load(b"colorblue", &[5,9]).unwrap();
        g0.push_rec();
        g0.look_ahead_mut().load(b"colorgreen", &[5,10]).unwrap();
        g0.push_rec();

        g1.look_ahead_mut().load(b"shapecircle", &[5,11]).unwrap();
        g1.push_rec();

        assert_eq!(g0.cmp_keys(&g1), Ordering::Less);
    }

    #[test]
    fn group_cmp_keys_greater() {
        let r0 = RecordBuilder::default().build().unwrap();
        let mut g0 = GroupBuilder::default().from_record(r0);

        let r1 = RecordBuilder::default().build().unwrap();
        let mut g1 = GroupBuilder::default().from_record(r1);


        g0.look_ahead_mut().load(b"shapecircle", &[5,11]).unwrap();
        g0.push_rec();

        g1.look_ahead_mut().load(b"colorblue", &[5,9]).unwrap();
        g1.push_rec();
        g1.look_ahead_mut().load(b"colorgreen", &[5,10]).unwrap();
        g1.push_rec();

        assert_eq!(g0.cmp_keys(&g1), Ordering::Greater);
    }
}


