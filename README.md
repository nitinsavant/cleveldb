# ClevelDB

A clone of LevelDB written in Go.

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