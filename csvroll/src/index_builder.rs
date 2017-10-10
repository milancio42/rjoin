use super::avx;
use super::bit;
use super::parser::Index;
use x86intrin::{m256i, mm256_cmpeq_epi8, mm256_movemask_epi8};

#[derive(Debug)]
pub struct IndexBuilder {
    // field separator
    m_fs: m256i,
    // record terminator
    m_rt: m256i,
    b_fs: Vec<u64>,
    b_rt: Vec<u64>,
}

impl IndexBuilder {
    pub fn new(field_separator: u8, record_terminator: u8) -> Self {
        Self {
            m_fs: avx::mm256i(field_separator as i8),
            m_rt: avx::mm256i(record_terminator as i8),
            b_fs: Vec::new(),
            b_rt: Vec::new(),
        }
    }

    #[inline(always)]
    pub fn build(
        &mut self,
        buf: &[u8],
        buf_offset: usize,
        idx: &mut Index,
    ) {
        let b_len = (buf.len() + 63) / 64;
        if b_len == 0 {
            return;
        }
        let appendix = 64 - buf.len() % 64;

        self.b_fs.clear();
        self.b_rt.clear();

        if b_len > self.b_fs.capacity() {
            self.b_fs.reserve_exact(b_len);
            self.b_rt.reserve_exact(b_len);
        }

        build_structural_character_bitmap(
            buf,
            &mut self.b_fs,
            &mut self.b_rt,
            &self.m_fs,
            &self.m_rt
        );
        build_main_index(&self.b_fs, &self.b_rt, buf_offset, appendix, idx);
    }
}

#[inline]
fn build_structural_character_bitmap(
    buf: &[u8],
    b_fs: &mut Vec<u64>,
    b_rt: &mut Vec<u64>,
    m_fs: &m256i,
    m_rt: &m256i,
) {
    let b_len = buf.len();
    let mut i = 0;

    while i + 63 < b_len {
        let m1 = unsafe { avx::u8_to_m256i(buf, i) };
        let m2 = unsafe { avx::u8_to_m256i(buf, i + 32) };

        b_fs.push(mbitmap(&m1, &m2, m_fs));
        b_rt.push(mbitmap(&m1, &m2, m_rt));

        i += 64;
    }
    
    if i + 32 < b_len {
        let m1 = unsafe { avx::u8_to_m256i(buf, i) };
        let m2 = unsafe { avx::u8_to_m256i_rest(buf, i + 32) };

        b_fs.push(mbitmap(&m1, &m2, m_fs));
        b_rt.push(mbitmap(&m1, &m2, m_rt));
    } else if i + 32 == b_len {
        let m1 = unsafe { avx::u8_to_m256i(buf, i) };
        
        b_fs.push(mbitmap_partial(&m1, m_fs));
        b_rt.push(mbitmap_partial(&m1, m_rt));
    } else if i < b_len {
        let m1 = unsafe { avx::u8_to_m256i_rest(buf, i) };

        b_fs.push(mbitmap_partial(&m1, m_fs));
        b_rt.push(mbitmap_partial(&m1, m_rt));
    }
}

#[inline]
fn mbitmap(x1: &m256i, x2: &m256i, y: &m256i) -> u64 {
    let i1 = mm256_movemask_epi8(mm256_cmpeq_epi8(*x1, *y));
    let i2 = mm256_movemask_epi8(mm256_cmpeq_epi8(*x2, *y));
    u64::from(i1 as u32) | (u64::from(i2 as u32) << 32)
}

#[inline]
fn mbitmap_partial(x: &m256i, y: &m256i) -> u64 {
    u64::from(mm256_movemask_epi8(mm256_cmpeq_epi8(*x, *y)) as u32)
}       
        
#[inline]
fn build_main_index(
    b_fs: &[u64],
    b_rt: &[u64],
    buf_offset: usize,
    appendix: usize,
    idx: &mut Index
) {

    let mut f_start = buf_offset;
    let mut last_f_count = idx.fields().len();
    let mut i = 0usize;
    for (f, r) in b_fs.iter().zip(b_rt) {
        // the record terminator works also as the field separator.
        let mut m_field_rec = *f | *r;
        let mut m_rec = *r;
        let mut m_field_rec_len = m_field_rec.trailing_zeros();
        let mut m_rec_len = m_rec.trailing_zeros();
        while m_field_rec != 0 {
            let f_end = buf_offset + i * 64 + (m_field_rec_len as usize);
            idx.push_field(f_start..f_end);
            f_start = f_end + 1;
            last_f_count += 1;
            // test if the rec_field separator is a record terminator
            if m_field_rec_len == m_rec_len {
                idx.push_record(last_f_count);
                m_rec = bit::r(m_rec);
                m_rec_len = m_rec.trailing_zeros();
            }
            m_field_rec = bit::r(m_field_rec);
            m_field_rec_len = m_field_rec.trailing_zeros();
        }

        i += 1;
    }

    // remainder
    let f_end = buf_offset + i * 64 - appendix;
    idx.push_field(f_start..f_end);
    idx.push_record(last_f_count + 1);
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_build_structural_character_bitmap() {
        let c = b',';
        let m = avx::mm256i(c as i8);

        macro_rules! s {
            ($( [ $c:expr ; $n:expr ] ),*) => {{
                let mut v = Vec::new();
                $( v.extend_from_slice(&[$c; $n]); )*
                v
            }}
        }

        struct TestCase {
            s: Vec<u8>,
            d: Vec<u64>,
        }
        let test_cases = vec![
            TestCase {
                s: vec![],
                d: vec![],
            },
            TestCase {
                s: vec![0xff; 32],
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([c; 1], [0xff; 31]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000001,
                ],
            },
            TestCase {
                s: s!([0xff; 1], [c; 1], [0xff; 30]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000010,
                ],
            },
            TestCase {
                s: s!([0xff; 30], [c; 1], [0xff; 1]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_01000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 31], [c; 1]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_10000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: vec![c; 32],
                d: vec![
                    0b00000000_00000000_00000000_00000000_11111111_11111111_11111111_11111111,
                ],
            },
            TestCase {
                s: vec![0xff; 64],
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([c; 1], [0xff; 63]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000001,
                ],
            },
            TestCase {
                s: s!([0xff; 8], [c; 1], [0xff; 55]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000001_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 16], [c; 1], [0xff; 47]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000001_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 24], [c; 1], [0xff; 39]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000001_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 32], [c; 1], [0xff; 31]),
                d: vec![
                    0b00000000_00000000_00000000_00000001_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 40], [c; 1], [0xff; 23]),
                d: vec![
                    0b00000000_00000000_00000001_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 48], [c; 1], [0xff; 15]),
                d: vec![
                    0b00000000_00000001_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 56], [c; 1], [0xff; 7]),
                d: vec![
                    0b00000001_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 63], [c; 1]),
                d: vec![
                    0b10000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([c; 32], [0xff; 32]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_11111111_11111111_11111111_11111111,
                ],
            },
            TestCase {
                s: s!([0xff; 32], [c; 32]),
                d: vec![
                    0b11111111_11111111_11111111_11111111_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: vec![c; 64],
                d: vec![
                    0b11111111_11111111_11111111_11111111_11111111_11111111_11111111_11111111,
                ],
            },
            TestCase {
                s: vec![0xff; 96],
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([c; 1], [0xff; 95]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000001,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 17], [c; 1], [0xff; 78]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000010_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 31], [c; 1], [0xff; 64]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_10000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 32], [c; 1], [0xff; 63]),
                d: vec![
                    0b00000000_00000000_00000000_00000001_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 45], [c; 1], [0xff; 50]),
                d: vec![
                    0b00000000_00000000_00100000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 63], [c; 1], [0xff; 32]),
                d: vec![
                    0b10000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 64], [c; 1], [0xff; 31]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000001,
                ],
            },
            TestCase {
                s: s!([0xff; 73], [c; 1], [0xff; 22]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000010_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 83], [c; 1], [0xff; 12]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00001000_00000000_00000000,
                ],
            },
            TestCase {
                s: s!([0xff; 95], [c; 1]),
                d: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_10000000_00000000_00000000_00000000,
                ],
            },
        ];
        for t in test_cases {
            let mut d = Vec::with_capacity(t.s.len() / 64 + 1);
            build_structural_character_bitmap(
                &t.s,
                &mut d,
                &mut vec![],
                &m,
                &m,
            );
            assert_eq!(t.d, d);
        }
    }

    #[test]
    fn test_build_main_index() {
        struct TestCase {
            b_fs: Vec<u64>,
            b_rt: Vec<u64>,
            buf_offset: usize,
            appendix: usize,
            idx: Index,
            want: Index,
        }
        let test_cases = vec![
            TestCase {
                b_fs: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
                buf_offset: 0,
                appendix: 0,
                idx: Index::from_parts(vec![], vec![]),
                want: Index::from_parts(vec![0..64], vec![1]),
            },
            TestCase {
                b_fs: vec![
                    0b00000000_10000000_00000000_00000000_10000000_00000000_00000000_10000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
                buf_offset: 0,
                appendix: 0,
                idx: Index::from_parts(vec![], vec![]),
                want: Index::from_parts(vec![0..7, 8..31, 32..55, 56..64], vec![4]),
            },
            TestCase {
                b_fs: vec![
                    0b00000000_10000000_00000000_00000000_10000000_00000000_00000000_10000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_10000000_00000000_00000000_00000000_00000000,
                ],
                buf_offset: 0,
                appendix: 0,
                idx: Index::from_parts(vec![], vec![]),
                want: Index::from_parts(vec![0..7, 8..31, 32..39, 40..55, 56..64], vec![3, 5]),
            },
            TestCase {
                b_fs: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
                b_rt: vec![
                    0b10000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
                buf_offset: 0,
                appendix: 0,
                idx: Index::from_parts(vec![], vec![]),
                want: Index::from_parts(vec![0..63, 64..64], vec![1, 2]),
            },
            TestCase {
                b_fs: vec![
                    0b10000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                ],
                buf_offset: 0,
                appendix: 0,
                idx: Index::from_parts(vec![], vec![]),
                want: Index::from_parts(vec![0..63, 64..64], vec![1]),
            },
            TestCase {
                b_fs: vec![
                    0b00000000_10000000_00000000_00000000_10000000_00000000_00000000_10000000,
                    0b00000000_00000000_00000000_00000000_00000000_10000000_00000000_00000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_10000000_00000000,
                ],
                buf_offset: 0,
                appendix: 0,
                idx: Index::from_parts(vec![], vec![]),
                want: Index::from_parts(vec![0..7, 8..31, 32..55, 56..79, 80..87, 88..128], vec![4, 6]),
            },
            TestCase {
                b_fs: vec![
                    0b00000000_10000000_00000000_00000000_10000000_00000000_00000000_10000000,
                    0b00000000_00000000_00000000_00000000_00000000_10000000_00000000_00000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_10000000_00000000,
                ],
                buf_offset: 0,
                appendix: 32,
                idx: Index::from_parts(vec![], vec![]),
                want: Index::from_parts(vec![0..7, 8..31, 32..55, 56..79, 80..87, 88..96], vec![4, 6]),
            },
            TestCase {
                b_fs: vec![
                    0b00000000_10000000_00000000_00000000_10000000_00000000_00000000_10000000,
                    0b00000000_00000000_00000000_00000000_00000000_10000000_00000000_00000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_10000000_00000000,
                ],
                buf_offset: 24,
                appendix: 32,
                idx: Index::from_parts(vec![0..15, 16..23], vec![2]),
                want: Index::from_parts(
                    vec![0..15, 16..23, 24..31, 32..55, 56..79, 80..103, 104..111, 112..120],
                    vec![2, 6, 8]),
            },
            TestCase {
                b_fs: vec![
                    0b00000000_10000000_00000000_00000000_10000000_00000000_00000000_10000000,
                    0b00000000_00000000_00000000_00000000_00000000_10000000_00000000_00000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_10000000_00000000,
                ],
                buf_offset: 24,
                appendix: 32,
                idx: Index::from_parts(vec![0..15, 16..23], vec![1]), // the last field belongs to
                                                                      // an incomplete record
                want: Index::from_parts(
                    vec![0..15, 16..23, 24..31, 32..55, 56..79, 80..103, 104..111, 112..120],
                    vec![1, 6, 8]),
            },
            TestCase {
                b_fs: vec![
                    0b00000000_10000000_00000000_00000000_10000000_00000000_00000000_11000000,
                    0b00000000_00000000_00000000_00000000_00000000_10000000_00000000_00000000,
                ],
                b_rt: vec![
                    0b00000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000,
                    0b00000000_00000000_00000000_00000000_00000000_00000000_11000000_00000000,
                ],
                buf_offset: 24,
                appendix: 32,
                idx: Index::from_parts(vec![0..15, 16..23], vec![2]),
                want: Index::from_parts(
                    vec![0..15, 16..23, 24..30, 31..31, 32..55, 56..79, 80..102, 103..103, 104..111, 112..120],
                    vec![2, 7, 8, 10]),
            },
        ];
        for t in test_cases {
            let TestCase { b_fs, b_rt, buf_offset, appendix, mut idx, want } = t;
            build_main_index(&b_fs, &b_rt, buf_offset, appendix, &mut idx);
            assert_eq!(idx, want);
        }
    }
}



