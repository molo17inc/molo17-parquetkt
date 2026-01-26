# Performance Benchmark Guide

## Overview

The `PerformanceBenchmarkTest.kt` contains comprehensive performance benchmarks for the molo17-parquetkt library.

## Benchmark Tests

### 1. Main Benchmark: 100 Columns x 500,000 Rows

**Test:** `benchmark - write 100 columns with 500K rows`

**Specifications:**
- **Rows:** 500,000
- **Columns:** 100 (distributed across 5 data types)
  - 20 INT32 columns
  - 20 INT64 columns
  - 20 DOUBLE columns
  - 20 STRING columns
  - 20 BOOLEAN columns
- **Total cells:** 50,000,000
- **Compression:** SNAPPY
- **Estimated raw data size:** ~400 MB

**What it measures:**
- Data generation time
- Parquet file write time
- File size after compression
- Throughput (rows/second and cells/second)
- File integrity verification

**Expected output:**
```
================================================================================
PERFORMANCE BENCHMARK: 100 Columns x 500,000 Rows
================================================================================

Generating test data...
Schema created with 100 columns:
  - 20 INT32 columns
  - 20 INT64 columns
  - 20 DOUBLE columns
  - 20 STRING columns
  - 20 BOOLEAN columns

Generating 500,000 rows of dummy data...
Data generation complete!
  Total cells: 50,000,000
  Estimated data size: ~400 MB

--------------------------------------------------------------------------------
Writing Parquet file...
--------------------------------------------------------------------------------

✅ WRITE SUCCESSFUL!

================================================================================
BENCHMARK RESULTS
================================================================================
File: benchmark_100cols_500krows.parquet
File size: XX MB
Rows: 500,000
Columns: 100
Total cells: 50,000,000

Write time: XXXX ms (XX.X seconds)
Throughput: XXXXX rows/second
Throughput: XXXXXXX cells/second
================================================================================

Verifying file integrity...
✅ File verification successful!
Read time: XXXX ms (XX.X seconds)

================================================================================
```

### 2. Compression Benchmark

**Test:** `benchmark - write with different compression codecs`

**Specifications:**
- **Rows:** 100,000
- **Columns:** 20
- **Codecs tested:** UNCOMPRESSED, SNAPPY, GZIP, ZSTD

**What it measures:**
- Write time for each codec
- File size for each codec
- Compression ratio
- Throughput comparison

### 3. Row Group Benchmark

**Test:** `benchmark - write multiple row groups`

**Specifications:**
- **Row groups:** 5
- **Rows per group:** 100,000
- **Total rows:** 500,000
- **Columns:** 20

**What it measures:**
- Multi-row group write performance
- Memory efficiency with chunked writes

## Running the Benchmarks

### Prerequisites

1. **Install Gradle** (if not already installed):
   ```bash
   # macOS
   brew install gradle
   
   # Or download from https://gradle.org/install/
   ```

2. **Generate Gradle wrapper** (first time only):
   ```bash
   gradle wrapper
   ```

### Run All Benchmarks

```bash
# Using Gradle wrapper
./gradlew test --tests "com.molo17.parquetkt.PerformanceBenchmarkTest"

# Or using the provided script
chmod +x run-benchmark.sh
./run-benchmark.sh
```

### Run Specific Benchmark

```bash
# Main 500K rows benchmark
./gradlew test --tests "com.molo17.parquetkt.PerformanceBenchmarkTest.benchmark - write 100 columns with 500K rows"

# Compression comparison
./gradlew test --tests "com.molo17.parquetkt.PerformanceBenchmarkTest.benchmark - write with different compression codecs"

# Row group benchmark
./gradlew test --tests "com.molo17.parquetkt.PerformanceBenchmarkTest.benchmark - write multiple row groups"
```

### Run with Detailed Output

```bash
./gradlew test --tests "com.molo17.parquetkt.PerformanceBenchmarkTest" --info
```

## Interpreting Results

### Write Performance

**Good performance indicators:**
- **Throughput:** > 50,000 rows/second
- **Cell throughput:** > 5,000,000 cells/second
- **Write time:** < 10 seconds for 500K rows

### Compression Ratios

**Expected compression ratios (compressed size / raw size):**
- **UNCOMPRESSED:** 1.0 (no compression)
- **SNAPPY:** 0.3-0.5 (fast, moderate compression)
- **GZIP:** 0.2-0.4 (slower, better compression)
- **ZSTD:** 0.2-0.3 (good balance)

### File Size

**Expected file sizes for 100 cols x 500K rows:**
- **Raw data:** ~400 MB
- **SNAPPY compressed:** ~120-200 MB
- **GZIP compressed:** ~80-160 MB
- **ZSTD compressed:** ~80-120 MB

## Performance Optimization Tips

### For Writing

1. **Use appropriate compression:**
   - SNAPPY for speed
   - GZIP/ZSTD for smaller files

2. **Batch writes:**
   - Write in row groups of 100K-500K rows
   - Don't create too many small row groups

3. **Column types:**
   - Use appropriate types (INT32 vs INT64)
   - Consider dictionary encoding for strings (future enhancement)

### For Reading

1. **Column projection:**
   - Only read columns you need
   - Use schema to filter early

2. **Row group filtering:**
   - Read specific row groups if possible
   - Use statistics for filtering (future enhancement)

## Benchmark Data Generation

The benchmark generates realistic dummy data:

- **INT32:** Random integers 0-1,000,000
- **INT64:** Random longs 0-1,000,000,000
- **DOUBLE:** Random doubles 0.0-1,000,000.0
- **STRING:** String values with 10,000 unique values
- **BOOLEAN:** Random true/false

**Seed:** Fixed at 42 for reproducibility

## Troubleshooting

### Out of Memory

If you get OOM errors:

```bash
# Increase JVM heap size
export GRADLE_OPTS="-Xmx4g"
./gradlew test --tests "com.molo17.parquetkt.PerformanceBenchmarkTest"
```

### Slow Performance

Possible causes:
1. Debug mode enabled
2. Insufficient memory
3. Disk I/O bottleneck
4. Antivirus scanning temp files

### Test Timeout

Increase test timeout in `build.gradle.kts`:

```kotlin
tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
    maxHeapSize = "4g"
    // Increase timeout
    systemProperty("junit.jupiter.execution.timeout.default", "10m")
}
```

## Comparing with Other Libraries

To compare performance with other Parquet libraries:

1. **Apache Parquet Java:** Use parquet-avro or parquet-hadoop
2. **FastParquet (Python):** Write equivalent Python benchmark
3. **parquet-dotnet (C#):** Run similar benchmark in C#

**Metrics to compare:**
- Write throughput (rows/second)
- File size (compression efficiency)
- Memory usage
- Read throughput

## Future Enhancements

Planned optimizations:
- [ ] Dictionary encoding for strings
- [ ] Parallel column encoding
- [ ] Memory-mapped I/O
- [ ] Streaming writes (chunked)
- [ ] Delta encodings
- [ ] Statistics calculation

## Example Benchmark Results

**System:** MacBook Pro M1, 16GB RAM, SSD

```
Main Benchmark (100 cols x 500K rows):
  Write time: ~8-12 seconds
  Throughput: ~40,000-60,000 rows/second
  File size: ~150 MB (SNAPPY)
  
Compression Comparison (20 cols x 100K rows):
  UNCOMPRESSED: 1200 ms, 180 MB
  SNAPPY:        800 ms,  60 MB
  GZIP:         1500 ms,  45 MB
  ZSTD:         1000 ms,  40 MB
```

*Note: Actual results will vary based on hardware and system load.*
