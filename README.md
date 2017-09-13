# Rjoin

A tool for joining CSV data on command line.

Dual-licensed under MIT or [unlicense](unlicense.org)

### Documentation

[https://docs.rs/rjoin](https://docs.rs/rjoin)

### Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
rjoin = "0.1.0"
```

add this to your crate root:

```rust
extern crate rjoin;
```
### Installation

The binary name for `rjoin` is `rj`.

```bash
$ cargo install rjoin
```

(don't forget to add `$HOME/.cargo/bin` to your path).

### Why should you use `rjoin`?

*   it can perform the join on multiple fields
*   it has higher flexibilty on specifying the field separators and record terminators compared to GNU join
*   it has a very flexible CSV parser which can recognize quotes, escape characters and even comments (currently based on BurntSushi's excellent [CSV](https://github.com/BurntSushi/rust-csv) library)
*   it is likely faster than GNU join when checking the correct order of records
*   it has (subjectively) cleaner CLI. 

### Why shouldn't you use `rjoin`?

*   you need a specific output format. GNU join is more flexible on this, but it can be mitigated by piping the output to `cut` or `awk`.
*   you need a case insensitive join. This can be mitigated by preprocessing data with `tr` utility.
*   you need to perform the join as fast as possible by not checking the correct order of the input.

### Example

Let's suppose we have the following data:

```bash
$ cat left
color,blue
color,green
color,red
shape,circle
shape,square
```

```bash
$ cat right
altitude,low                                        
altitude,high                                       
color,orange                                          
color,purple                                          
```
To get the lines with the common key:

```bash
$ rj left right
color,blue,orange
color,blue,purple
color,green,orange
color,green,purple
color,red,orange
color,red,purple
```

Some comments:

*   by default, the first field is the key. If you wish to use another field, you can specify it using `--key/-k` option (even per file).
    `rjoin` supports **multiple fields** as the key, but the number of key fields in both files must be equal.
*   by default, only the lines with the common key are printed. If you wish to print also unmached lines from the left or right file, use 
    any combination of these: `--show-left/-l`, `--show-right/-r` or `--show-both/-b`. Note however, if you use any of these options, the default behavior 
    of showing matches of both files is reset and must be made explicit if desired by adding `-b`. 
*   there are multiple lines with the same key in both files, resulting in [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product).

To get the lines with the unmatched key in both files:

```bash
$ rj -lr left right
altitude,low                                        
altitude,high                                       
shape,circle
shape,square
```

### Contributing

Any kind of contribution (e.g. comment, suggestion, question, bug report and pull request) is welcome.

### Acknowledgments

A big thanks to BurntSushi for his excellent work.
