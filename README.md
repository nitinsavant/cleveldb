# ClevelDB

ClevelDB is my attempt to build a clone of LevelDB in order to explore many of its key ideas, including:

- A skip list (i.e. memtable) to support fast retrieval of the most recent db writes
- A write-ahead-log (i.e. journal) to add basic persistence and support recovery in the event of a crash
- SSTables to write (i.e. flush) older data to disk that won't fit in memory
- On-disk indexes to improve performance when searching SSTables
- Bloom Filter to improve performance when searching SSTables

## TODO
- Use a larger variable-length integer for key/value sizes. The key/value sizes are currently uint16, so their max size is limited to 65,536.
- Integrate the bloom filter into ClevelDB. I may need to modify the multi-table RangeScan implementation (because it currently returns the nearest key and that doesn't appear to work with a basic BloomFilter guard clause)
- Add comprehensive tests to verify merged ClevelDBIterator behaves as expected
- Add background compaction to remove duplicate/deleted keys and potentially reduce the number of SSTables and their sizes
- Fix and run benchmarks for current ClevelDB implementation (SkipList + SSTables)

## KNOWN ISSUES
- If keys are updated while the memtable is being flushed, those update operations will be missing from the journal (i.e. write-ahead-log) because the journal is truncated after flushing.
- Searching for a key that is in a memtable that's being actively flushed will not be found.

## Benchmarks

For the following two implementations:
- NaiveDB (in-memory HashMap implementation)
- ClevelDB (in-memory SkipList implementation)

### Benchmarked on 2023-01-20:

Benchmark_ClevelDBFillSeq
Benchmark_ClevelDBFillSeq-8     	 2382663	       510.1 ns/op

Benchmark_ClevelDBFillRand
Benchmark_ClevelDBFillRand-8    	 1000000	      2383 ns/op

Benchmark_ClevelDBDeleteSeq
Benchmark_ClevelDBDeleteSeq-8   	 3382195	       364.6 ns/op

Benchmark_ClevelDBReadSeq
Benchmark_ClevelDBReadSeq-8     	 3188511	       515.0 ns/op

Benchmark_ClevelDBRangeScan
Benchmark_ClevelDBRangeScan-8   	 3497749	       397.2 ns/op

Benchmark_NaiveDBFillSeq
Benchmark_NaiveDBFillSeq-8      	 2934643	       370.3 ns/op

Benchmark_NaiveDBFillRand
Benchmark_NaiveDBFillRand-8     	 2748723	       448.7 ns/op

Benchmark_NaiveDBDeleteSeq
Benchmark_NaiveDBDeleteSeq-8    	 5081919	       264.7 ns/op

Benchmark_NaiveDBReadSeq
Benchmark_NaiveDBReadSeq-8      	 5130001	       262.3 ns/op

Benchmark_NaiveDBRangeScan
Benchmark_NaiveDBRangeScan-8    	   10000	   2417228 ns/op
