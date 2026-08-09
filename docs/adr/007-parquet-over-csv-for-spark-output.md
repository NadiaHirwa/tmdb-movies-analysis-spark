# 007: Use Parquet Instead of CSV for Processed Data Output

## Status
Accepted

## Context
The pandas version of this project (ADR-005) chose CSV for processed
data output, reasoning that CSV is simple, human-readable, and
sufficient for a small, fixed dataset. This Spark version revisits
that decision, since the trade-offs differ meaningfully in a Spark
context.

## Decision
Save cleaned data as Parquet instead of CSV.

## Reasoning
- **Type preservation**: this pipeline invests real effort casting
  `release_date` to a proper date type and `budget`/`revenue` to
  numeric types (see `convert_dtypes()`). CSV is a text-only format
  and would discard all of that type information on write, requiring
  it to be re-parsed on every future read. Parquet preserves the
  schema exactly as written.
- **Consistency with the module's broader content**: this course
  module's Section 3 covered Apache Iceberg, Delta Lake, and Hudi in
  depth - all three are metadata layers built on top of Parquet.
  Writing this pipeline's own output as Parquet keeps the file format
  story coherent with everything else studied in this apprenticeship.
- **Spark-native**: Parquet is Spark's optimized, native columnar
  format. Spark's own internal optimizations (predicate pushdown,
  columnar reads) are designed around Parquet, not CSV. Choosing CSV
  in a Spark pipeline works against the tool's natural direction.

## Trade-off Acknowledged
Parquet is a binary format - it can't be opened directly in a text
editor or quickly eyeballed with `cat`, unlike CSV. This is a real,
if minor, loss of quick inspectability during debugging. In practice,
`spark.read.parquet(path).show()` is a fast enough substitute that
this cost is acceptable.

## Consequence
`data/processed/movies_clean.parquet` is a directory (not a single
file), since Spark writes output in parallel across partitions. Code
reading this data elsewhere in the pipeline (`kpis.py`,
`visualize.py` inputs) must use `spark.read.parquet(path)`, which
handles this transparently.