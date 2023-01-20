# ClevelDB

A clone of LevelDB written in Go.

### Benchmarks

Given a seeded database with 1M keys. 

- NaiveDB (basic in-memory HashMap implementation)
  - Get ~200 nsec/op
  - Put ~8000 nsec/op
  - Delete ~120 nsec/op
  - RangeScan ~3 sec/op