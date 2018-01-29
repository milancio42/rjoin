## How to join 2 files using `rj`

### Defaults

By default, the first field is the join field, called the key, which
must be sorted in ascending order. The fields are expected to be
delimited by commas and the records by line feeds.

Consider the following 2 files:
```bash
$ cat countries
1,Italy
2,France
3,Spain

$ cat cities
1,Rome
3,Madrid
4,Berlin
```

The two files *countries* and *cities* have the first field as the key, so they can be joined:

```bash
$ rj countries cities
1,Italy,Rome
3,Spain,Madrid
```

By default, the result contains only the matched records.

### Specify the key fields

For both files:
```bash
$ rj -k=1 countries cities
1,Italy,Rome
3,Spain,Madrid
```

Or for each file separately:
```bash
$ rj --left-key=1 --right-key=1 countries cities
1,Italy,Rome
3,Spain,Madrid
```

### Specify a field delimiter

Equal input and output field delimiter:
```bash
$ rj -d=',' countries cities
1,Italy,Rome
3,Spain,Madrid
```

Output field delimiter. Input still uses the default delimiter:
```bash
$ rj --out-delimiter=';' countries cities
1;Italy;Rome
3;Spain;Madrid
```


Different input and output field delimiter. If you specify the input field delimiter, you must specify also the output field delimiter:
```bash
$ rj --in-delimiter=',' --out-delimiter=';' countries cities
1;Italy;Rome
3;Spain;Madrid
```

You can specify the input field delimiter for each file (again with output field delimiter):
```bash
$ rj --in-left-delimiter=',' --in-right-delimiter=',' --out-delimiter=',' countries cities
1,Italy,Rome
3,Spain,Madrid
```

### Specify a record terminator

Equal input and output record terminator:
```bash
$ rj -t=$'\n' countries cities
1,Italy,Rome
3,Spain,Madrid
```

Output record terminator. Input still uses the default terminator:
```bash
$ rj --out-terminator='#' countries cities
1,Italy,Rome#3,Spain,Madrid#
```

Different input and output record terminator. If you specify the input record terminator, you must specify also the output record terminator:
```bash
$ rj --in-terminator --out-terminator='#' countries cities
1,Italy,Rome#3,Spain,Madrid#
```

You can specify the input record terminator for each file (again with output record terminator):
```bash
$ rj --in-left-terminator=$'\n' --in-right-terminator=$'\n' --out-terminator=$'\n' countries cities
1,Italy,Rome
3,Spain,Madrid
```


### Specify which records to display

Matched records:
```bash
$ rj -b countries cities
1,Italy,Rome
3,Spain,Madrid
```

Unmatched from the left file:
```bash
$ rj -l countries cities
2,France
```

Unmatched from the right file:
```bash
$ rj -r countries cities
4,Berlin
```

These can be combined arbitrarily:
```bash
$ rj -lbr countries cities
1,Italy,Rome
2,France
3,Spain,Madrid
4,Berlin

$ rj -lr countries cities
2,France
4,Berlin
```

### How to join on multiple fields

Consider the following files:
```bash
$ cat shop1
Author,Book
celebrated author,common book name
celebrated author,uncommon book name
forgotten author,another common book name
forgotten author,common book name

$ cat shop2
celebrated author,common book name
celebrated author,yet another common book name
forgotten author,common book name
```

Find common books in both shops:
```bash
$ rj --header -k=1,2 shop1 shop2
Author,Book
celebrated author,common book name
forgotten author,common book name
```
The flag `--header` causes the first row in each input file to be treated as a header.

Find unique books in shop1:
```bash
$ rj --header -k=1,2 -l shop1 shop2
Author,Book
celebrated author,uncommon book name
forgotten author,another common book name
```

Find unique books in shop2:
```bash
$ rj --header -k=1,2 -r shop1 shop2
Author,Book
celebrated author,yet another common book name
```

## How to reorder the output columns

Currently `rj` output is composed of the key fields followed by the non-key fields from left and right file, if any.
If you wish a different order, you can use `awk`.

Do one thing and do it well. Output fields reordering is not the part of *the one thing*.

```bash
$ rj countries cities | awk -vFS=',' -vOFS=',' '{print $1, $3, $2}'
1,Rome,Italy
3,Madrid,Spain
```
