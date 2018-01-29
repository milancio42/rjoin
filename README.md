# Rjoin

rjoin is a new command line utility for joining records of two files on common fields.

Dual-licensed under MIT or [unlicense](unlicense.org)

### Documentation

[https://docs.rs/rjoin](https://docs.rs/rjoin)

### Installation

The binary name for rjoin is `rj`.

```bash
$ cargo --version
cargo 0.25.0-nightly (a88fbace4 2017-12-29) # requires nightly channel
$ RUSTFLAGS="-C target-cpu=native" cargo install rjoin
```

(don't forget to add `$HOME/.cargo/bin` to your path).

### Why should you use `rjoin`?

*   it can perform the join on multiple fields
*   it has higher flexibilty on specifying the field separators and record terminators compared to GNU join
*   it has (subjectively) cleaner CLI.

### Why you should not use `rjoin`?

*   you need a specific output format. GNU join is more flexible on this, but it can be mitigated by piping the output to `awk`.
*   you need a case insensitive join. This can be mitigated by preprocessing data with `tr` utility.
*   you don't have an AVX2 capable CPU.

### Quick Example

Let's suppose we have the following data:

```bash
$ cat left
color,blue
color,green
color,red
shape,circle
shape,square

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
    `rj` supports **multiple fields** as the key, but the number of key fields in both files must be equal.
*   by default, only the lines with the common key are printed. If you wish to print also unmached lines from the left or right file, use
    any combination of these: `--show-left/-l`, `--show-right/-r` or `--show-both/-b`. Note however, if you use any of these options, the default behavior
    is reset (e.g. if you want to see the unmatched lines in the left file along with the matched lines, use `-lb`. With `-l` you will not see the matched lines.)
*   there are multiple lines with the same key in both files, resulting in [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product).

To get the lines with the unmatched key in both files:

```bash
$ rj -lr left right
altitude,low                                        
altitude,high                                       
shape,circle
shape,square
```

Check the [tutorial](examples/tutorial.md) for the detailed walkthrough.

### Contributing

Any kind of contribution (e.g. comment, suggestion, question, bug report and pull request) is welcome.

### Why Rust?

Because C eats a bloody lot of mental resources only to avoid shooting my leg, or worse.

### Acknowledgments

The CSV parser used in Rjoin is based on the work of [Y. Li, N. R. Katsipoulakis, B. Chandramouli, J. Goldstein, and D. Kossmann. Mison: a fast JSON parser for data analytics. In *VLDB*, 2017](http://www.vldb.org/pvldb/vol10/p1118-li.pdf).

The SIMD part was shamelessly copied from [`pikkr`](https://github.com/pikkr/pikkr)

And finally a big thanks to BurntSushi for his excellent work.
