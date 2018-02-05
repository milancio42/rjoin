# csvroll

A low-level, non-FSM, zero-copy CSV parser.

Dual-licensed under MIT or [unlicense](unlicense.org)

### Documentation

https://docs.rs/csvroll


### Usage

Add this to your `Cargo.toml`:
```toml
[dependencies]
csvroll = "0.1"
```

and this to your crate root:
```rust
extern crate csvroll;
```

The parser uses SIMD which is not yet stabilized, so you need to use the nightly channel.

### Example

```rust
extern crate rollbuf;

use rollbuf::RollBuf;
use csvroll::parser::{Parser, Index,};
use csvroll::index_builder::IndexBuilder;


struct TestCase {
    input: String,
    buf_len: usize,
    consume: Vec<usize>,
    want: Vec<(String, Index)>,
}

let test_cases = vec![
    TestCase {
        input: "a\nb\nc,d,e".to_owned(),
        buf_len: 7,
        consume: vec![1, 1, 1, 1],
        want: vec![
            ("a\nb\nc,d".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5 ], vec![1, 2])),
            ("b\nc,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5], vec![1])),
            ("c,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5], vec![3])),
            ("".to_owned(), Index::from_parts(vec![], vec![])),
        ],
    },
    TestCase {
        input: "a\nb\nc,d,e".to_owned(),
        buf_len: 7,
        consume: vec![2, 1, 1],
        want: vec![
            ("a\nb\nc,d".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5 ], vec![1, 2])),
            ("c,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5], vec![3])),
            ("".to_owned(), Index::from_parts(vec![], vec![])),
        ],
    },
    TestCase {
        input: "a\nb\nc,d,e".to_owned(),
        buf_len: 7,
        consume: vec![1, 0, 1, 1, 1],
        want: vec![
            ("a\nb\nc,d".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5 ], vec![1, 2])),
            ("b\nc,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5], vec![1])),
            ("b\nc,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5, 6..7], vec![1, 4])),
            ("c,d,e".to_owned(), Index::from_parts(vec![0..1, 2..3, 4..5], vec![3])),
            ("".to_owned(), Index::from_parts(vec![], vec![])),
        ],
    },
    TestCase {
        input: "a\nb\nc\n".to_owned(),
        buf_len: 4,
        consume: vec![2, 1, 1],
        want: vec![
            ("a\nb\n".to_owned(), Index::from_parts(vec![0..1, 2..3], vec![1, 2])),
            ("c\n".to_owned(), Index::from_parts(vec![0..1], vec![1])),
            ("".to_owned(), Index::from_parts(vec![], vec![])),
        ],
    },
    TestCase {
        input: "a\nb\nc,".to_owned(),
        buf_len: 4,
        consume: vec![2, 1, 1],
        want: vec![
            ("a\nb\n".to_owned(), Index::from_parts(vec![0..1, 2..3], vec![1, 2])),
            ("c,".to_owned(), Index::from_parts(vec![0..1, 2..2], vec![2])),
            ("".to_owned(), Index::from_parts(vec![], vec![])),
        ],
    },
];

for (i, t) in test_cases.into_iter().enumerate() {
    println!("test case: {}", i);
    let TestCase { input, buf_len, consume, want } = t;
    let buf = RollBuf::with_capacity(buf_len, input.as_bytes());
    let idx_builder = IndexBuilder::new(b',', b'\n');
    let mut parser = Parser::from_parts(buf, idx_builder);

    for (i, (c, w)) in consume.iter().zip(&want).enumerate() {
        println!("parse: {}", i);
        parser.parse().unwrap();
        assert_eq!(parser.output(), (w.0.as_bytes(), &w.1));
        parser.consume(*c);
    }
}

```


