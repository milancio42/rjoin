## generate data
```bash
$ cat /dev/urandom | tr -dc 'a-z' | fold -w 5 | head -n 1000000 | sort | awk -vOFS=',' '{print $1, "L"}' > left-li
$ cat /dev/urandom | tr -dc 'a-z' | fold -w 5 | head -n 1000000 | sort | awk -vOFS=',' '{print $1, "R"}' > right-li
$ cat /dev/urandom | tr -dc 'a-z' | fold -w 4 | head -n 1000000 | sort | awk -vOFS=',' '{print $1, "L"}' > left-hi
$ cat /dev/urandom | tr -dc 'a-z' | fold -w 4 | head -n 1000000 | sort | awk -vOFS=',' '{print $1, "R"}' > right-hi
```

Notes: we generate two sets of data. The first one having the intersection between the left and right file of less than 10% (left-li and right-li). The second one with the intersetion of about 90% (left-hi and right-hi).


## inner join

The inner join prints only the matched records.

##### low intersection performance

| Tool | Command | Line Count | Time |
| ---- | ------- | ---------- | ---- |
| rjoin | `rj left-li right-li` | 84107 | **0.18s** |
| GNU join | `LC_COLLATE=C join -t ','  left-li right-li` | 84107 | 0.19s |

##### high intersection performance

| Tool | Command | Line Count | Time |
| ---- | ------- | ---------- | ---- |
| rjoin | `rj left-hi right-hi` | 2187458 | 0.36s |
| GNU join | `LC_COLLATE=C join -t ','  left-hi right-hi` | 2187458 | 0.36s |

## left outer join

The left outer join prints the matched records as well as unmatched records from the left file.

##### low intersection performance

| Tool | Command | Line Count | Time |
| ---- | ------- | ---------- | ---- |
| rjoin | `rj -lb left-li right-li` | 1003428 | 0.25s |
| GNU join | `LC_COLLATE=C join -t ',' -a 1  left-li right-li` | 1003428 | 0.25s |

##### high intersection performance

| Tool | Command | Line Count | Time |
| ---- | ------- | ---------- | ---- |
| rjoin | `rj -lb left-hi right-hi` | 2299563 | 0.41s |
| GNU join | `LC_COLLATE=C join -t ',' -a 1  left-hi right-hi` | 2299563 | **0.39s** |

## full outer join

The full outer join prints the matched records as well as unmatched records from both files.

##### low intersection performance

| Tool | Command | Line Count | Time |
| ---- | ------- | ---------- | ---- |
| rjoin | `rj -lbr left-li right-li` | 1922744 | 0.34s |
| GNU join | `LC_COLLATE=C join -t ',' -a 1 -a 2  left-li right-li` | 1922744 | **0.30s** |

##### high intersection performance

| Tool | Command | Line Count | Time |
| ---- | ------- | ---------- | ---- |
| rjoin | `rj -lbr left-hi right-hi` | 2411874 | 0.42s |
| GNU join | `LC_COLLATE=C join -t ',' -a 1 -a 2  left-hi right-hi` | 2411874 | **0.40s** |

## observations

`rj` is slightly faster when the output is small but loses with medium-large outputs. There are two factors in play here:
-  `rj` uses a faster parser, based on AVX2 instructions rather than
a simple `memchr` based one found in GNU join.
-  `rj` pays a penalty for multiple fields join by introducing
another indirection for them. This seems to have a non-trivial cost when
printing the output. However it can be mitigated by specializing for the
single field joins.


## final notes

GNU join uses `LC_COLLATE=C` environmental variable to force the byte ordering when comparing the keys. The same ordering is used in `rjoin`.
Both tools check whether the input is ordered correctly.
