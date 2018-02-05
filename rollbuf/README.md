# rollbuf

An extendable buffer which rolls the last incomplete part to the beginning so it can be completed and consumed.

Dual-licensed under MIT or [unlicense](unlicense.org)

### Documentation

https://docs.rs/rollbuf

### Usage

Add this to your `Cargo.toml`:
```toml
[dependencies]
rollbuf = "0.1"
```

and this to your crate root:
```rust
extern crate rollbuf;
```

### Example

```rust
use rollbuf::RollBuf;

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
```

