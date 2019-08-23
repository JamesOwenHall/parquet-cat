# parquet-cat
Fast command line access to data and metadata in Apache Parquet files.

## Usage

### Print Contents as JSON
To print out the contents of Parquet files, pass their file names as arguments.

```sh
$ parquet-cat *.parquet
```

### Show Metadata
Use the `-m`/`--metadata` flag to get file metadata without scanning the whole file.

```sh
$ parquet-cat -m *.parquet
```

## Performance

A lot of the functionality that parquet-cat provides is also covered by parquet-tools. However, one area in which they differ significantly is in performance. The following benchmark uses a 25 MiB parquet file with 690,159 rows. The file contains numbers, strings and timestamps.

### Scanning Data

```sh
$ time parquet-cat data.parquet | wc -l
$ time parquet-tools cat -j data.parquet | wc -l
```

program       | time
--------------|-----
parquet-cat   | 6 sec
parquet-tools | 1 min 16 sec

parquet-cat is 10x faster at converting the data to JSON.

### Retrieving Metadata

```sh
$ time parquet-cat -m data.parquet
$ time parquet-tools rowcount data.parquet
```

program       | time
--------------|-----
parquet-cat   | 0.016 sec
parquet-tools | 0.803 sec

Scanning metadata is also faster, which can make a difference when reading from a large number of files.
