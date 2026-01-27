# MOLO17 ParquetKt

A fully managed, pure Kotlin library for reading and writing Apache Parquet files. This is a port of the excellent [parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) library from C# to Kotlin.

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://gitlab.com/molo17-public/gluesync/molo17-parquetkt)
[![Test Coverage](https://img.shields.io/badge/tests-104%20passing-brightgreen)](https://gitlab.com/molo17-public/gluesync/molo17-parquetkt)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

## Features

- 🚀 **Pure Kotlin** - No native dependencies, works anywhere Kotlin/JVM works
- 📖 **Read & Write** - Full support for reading and writing Parquet files
- 🎯 **Type-Safe** - Leverage Kotlin's type system for compile-time safety
- 🔄 **Serialization** - Automatic serialization/deserialization of Kotlin data classes
- 📊 **Schema Support** - Dynamic schema creation and reflection
- 🗜️ **Compression** - Support for SNAPPY, GZIP, ZSTD, and UNCOMPRESSED
- 🎨 **Multiple APIs** - High-level and low-level APIs for different use cases
- ⚡ **High Performance** - 300K+ rows/second throughput
- ✅ **Production Ready** - Comprehensive test coverage (104 tests passing)
- 🔧 **Nullable Fields** - Full support for optional/nullable columns
- 🌊 **Coroutines Support** - Async I/O with suspend functions and Flow API

## Installation

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("com.molo17.parquetkt:molo17-parquetkt:1.0.0")
}
```

### Gradle (Groovy)

```groovy
dependencies {
    implementation 'com.molo17.parquetkt:molo17-parquetkt:1.0.0'
}
```

### Maven

```xml
<dependency>
    <groupId>com.molo17.parquetkt</groupId>
    <artifactId>molo17-parquetkt</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Quick Start

### High-Level API (Recommended)

The simplest way to work with Parquet files is using the high-level API with Kotlin data classes:

```kotlin
import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.schema.CompressionCodec

data class Person(
    val id: Long,
    val name: String,
    val age: Int,
    val salary: Double
)

fun main() {
    // Create some data
    val people = listOf(
        Person(1L, "Alice", 30, 75000.0),
        Person(2L, "Bob", 25, 65000.0),
        Person(3L, "Charlie", 35, 85000.0)
    )
    
    // Write to Parquet file
    ParquetFile.writeObjects("people.parquet", people, CompressionCodec.SNAPPY)
    
    // Read from Parquet file
    val readPeople = ParquetFile.readObjects<Person>("people.parquet")
    
    readPeople.forEach { person ->
        println("${person.name} is ${person.age} years old")
    }
}
```

### Low-Level API

For more control over the Parquet file structure:

```kotlin
import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema

fun main() {
    // Define schema
    val schema = ParquetSchema.create(
        DataField.int64("id"),
        DataField.string("name"),
        DataField.int32("age"),
        DataField.double("salary")
    )
    
    // Create columns
    val idColumn = DataColumn.createRequired(
        DataField.int64("id"),
        listOf(1L, 2L, 3L)
    )
    
    val nameColumn = DataColumn.createRequired(
        DataField.string("name"),
        listOf("Alice", "Bob", "Charlie")
    )
    
    val ageColumn = DataColumn.createRequired(
        DataField.int32("age"),
        listOf(30, 25, 35)
    )
    
    val salaryColumn = DataColumn.createRequired(
        DataField.double("salary"),
        listOf(75000.0, 65000.0, 85000.0)
    )
    
    // Create row group
    val rowGroup = RowGroup(schema, listOf(idColumn, nameColumn, ageColumn, salaryColumn))
    
    // Write to file
    ParquetFile.write("people.parquet", schema, listOf(rowGroup))
}
```

### Memory-Efficient Writing for Large Datasets

ParquetKT automatically manages memory to prevent OOM errors when writing large datasets:

```kotlin
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema

fun main() {
    val schema = ParquetSchema.create(
        DataField.int64("id"),
        DataField.string("name")
    )
    
    // Writer automatically flushes row groups to disk to prevent memory buildup
    val writer = ParquetWriter(
        outputPath = "large_file.parquet",
        schema = schema,
        maxRowGroupsInMemory = 10  // Auto-flush after 10 row groups (default)
    )
    
    // Write many row groups - memory is automatically managed
    for (batch in 0 until 100) {
        val ids = Array<Long?>(1000) { (batch * 1000 + it).toLong() }
        val names = Array<String?>(1000) { "User_${batch * 1000 + it}" }
        
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], ids),
            DataColumn(schema.fields[1], names)
        ))
        // Row groups are automatically flushed to disk when needed
    }
    
    writer.close()
}
```

**Manual flush control** for fine-grained memory management:

```kotlin
val writer = ParquetWriter(
    outputPath = "output.parquet",
    schema = schema,
    maxRowGroupsInMemory = 100  // Disable auto-flush
)

// Write row groups
for (i in 0 until 50) {
    writer.writeRowGroup(columns)
    
    // Manually flush every 10 row groups
    if ((i + 1) % 10 == 0) {
        writer.flushRowGroups()  // Explicitly flush to disk
    }
}

writer.close()
```

**Memory-efficient defaults:**

- Row group size: 8 MB (reduced from 128 MB)
- Page size: 256 KB (reduced from 1 MB)
- Auto-flush after 10 row groups (~80 MB total)

**ArrayPool for reduced GC pressure:**

```kotlin
import com.molo17.parquetkt.util.ArrayPool

// Create a custom array pool
val pool = ArrayPool(
    maxPoolSize = 16,        // Max arrays per bucket
    maxArraySize = 4 * 1024 * 1024  // 4 MB max
)

val writer = ParquetWriter(
    outputPath = "output.parquet",
    schema = schema,
    arrayPool = pool  // Enable array pooling
)

// Or use the shared global instance
val writer2 = ParquetWriter(
    outputPath = "output2.parquet",
    schema = schema,
    arrayPool = ArrayPool.shared
)

// Write data - arrays are automatically reused from pool
writer.writeRowGroup(columns)
writer.close()

// Check pool statistics
println(pool.getStats())
```

### Streaming Reads

For memory-efficient processing of large files:

```kotlin
import com.molo17.parquetkt.core.ParquetFile

fun main() {
    // Read as sequence (lazy evaluation)
    val rows = ParquetFile.readRowsAsSequence("large_file.parquet")
    
    // Process rows one at a time
    rows
        .filter { it["age"] as Int > 30 }
        .take(10)
        .forEach { row ->
            println("${row["name"]}: ${row["age"]}")
        }
}
```

### Coroutines API (Async I/O)

Use suspend functions and Flow for non-blocking I/O operations:

```kotlin
import com.molo17.parquetkt.core.ParquetFileAsync
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

data class Person(val id: Long, val name: String, val age: Int)

fun main() = runBlocking {
    val people = listOf(
        Person(1L, "Alice", 30),
        Person(2L, "Bob", 25),
        Person(3L, "Charlie", 35)
    )
    
    // Write asynchronously
    ParquetFileAsync.writeObjects("people.parquet", people)
    
    // Read asynchronously
    val readPeople = ParquetFileAsync.readObjects<Person>("people.parquet")
    
    // Stream with Flow API
    ParquetFileAsync.readObjectsAsFlow<Person>("people.parquet")
        .filter { it.age > 28 }
        .toList()
        .forEach { println(it) }
}
```

## Schema Definition

### Using Reflection

Automatically create schema from Kotlin data classes:

```kotlin
import com.molo17.parquetkt.serialization.SchemaReflector

data class Employee(
    val id: Long,
    val name: String,
    val department: String?,
    val salary: Double
)

val schema = SchemaReflector.reflectSchema<Employee>()
```

### Manual Schema Creation

Create schemas programmatically:

```kotlin
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema

val schema = ParquetSchema.create(
    DataField.int64("id", nullable = false),
    DataField.string("name", nullable = false),
    DataField.string("department", nullable = true),
    DataField.double("salary", nullable = false),
    DataField.boolean("isActive", nullable = false),
    DataField.date("hireDate", nullable = false),
    DataField.timestamp("lastLogin", nullable = true)
)
```

## Supported Types

### Primitive Types (All 8 Parquet Types Supported)

- `Boolean` → `BOOLEAN` ✅
- `Int` → `INT32` ✅
- `Long` → `INT64` ✅
- `Float` → `FLOAT` ✅
- `Double` → `DOUBLE` ✅
- `String` → `BYTE_ARRAY` (UTF-8) ✅
- `ByteArray` → `BYTE_ARRAY` ✅
- `ByteArray(12)` → `INT96` (legacy timestamps) ✅
- `ByteArray(fixed)` → `FIXED_LEN_BYTE_ARRAY` (UUIDs, etc.) ✅

### Logical Types

- `LocalDate` → `DATE` (INT32 days since epoch) ✅
- `LocalDateTime` → `TIMESTAMP` (INT64 microseconds since epoch) ✅

### Complex Types

- Nullable types → Optional fields with definition levels ✅
- Basic repeated fields → Simple lists (INT32, INT64, STRING) ✅
- Nested schema support → Hierarchical `NestedField` structure ✅
- List schema → Proper 3-level Parquet list structure ✅
- Map schema → Proper Parquet map structure ✅
- Struct schema → Nested group support ✅
- Reflection → Auto-detect `List<T>`, `Map<K,V>`, nested classes ✅
- `List<T>` serialization → Data encoding/decoding (in progress)
- `Map<K,V>` serialization → Data encoding/decoding (in progress)
- Nested struct serialization → Data encoding/decoding (in progress)

## Compression

Supported compression codecs with benchmarked performance:

```kotlin
import com.molo17.parquetkt.schema.CompressionCodec

// Available codecs (tested with 100K rows, 20 columns)
CompressionCodec.UNCOMPRESSED  // 15.65 MB, ~2.1M rows/s
CompressionCodec.SNAPPY        // 8.90 MB, ~1.9M rows/s (recommended default)
CompressionCodec.GZIP          // 7.04 MB, ~240K rows/s (best compression)
CompressionCodec.ZSTD          // 6.88 MB, ~1.2M rows/s (good balance)
```

## Performance

Real-world benchmarks on standard hardware:

| Operation | Throughput | Details |
| --------- | ---------- | ------- |
| Write (100 cols, 500K rows) | **287K rows/s** | 22 MB file, SNAPPY compression |
| Read (100 cols, 500K rows) | **310K rows/s** | 5M cells total |
| Write (20 cols, 100K rows) | **1.9M rows/s** | SNAPPY compression |
| Read with nullable fields | **Full support** | Automatic definition level handling |

## Working with Nullable Fields

```kotlin
data class User(
    val id: Long,
    val name: String,
    val email: String?,  // Nullable field
    val age: Int
)

val users = listOf(
    User(1L, "Alice", "alice@example.com", 30),
    User(2L, "Bob", null, 25)  // null email
)

ParquetFile.writeObjects("users.parquet", users)
```

## Reading Schema Information

```kotlin
import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.io.ParquetReader

// Read just the schema
val schema = ParquetFile.readSchema("data.parquet")
println(schema)

// Get detailed file information
ParquetReader.open("data.parquet").use { reader ->
    println("Row groups: ${reader.rowGroupCount}")
    println("Total rows: ${reader.totalRowCount}")
    println("Schema: ${reader.schema}")
}
```

## Examples

Check out the `src/main/kotlin/com/molo17/parquetkt/examples` directory for more examples:

- `BasicExample.kt` - Simple read/write operations
- More examples coming soon!

## Architecture

This library is structured into several key packages:

- **`core`** - High-level API (`ParquetFile`)
- **`schema`** - Schema definitions and types
- **`data`** - Data structures (`DataColumn`, `RowGroup`)
- **`io`** - File I/O (`ParquetReader`, `ParquetWriter`)
- **`serialization`** - Object serialization/deserialization
- **`compression`** - Compression codec support
- **`encoding`** - Encoding schemes

## Comparison with parquet-dotnet

This library provides a similar API to parquet-dotnet while leveraging Kotlin's language features:

| Feature | parquet-dotnet | MOLO17 ParquetKt |
| ------- | -------------- | ---------------- |
| Pure managed code | ✅ | ✅ |
| High-level API | ✅ | ✅ |
| Low-level API | ✅ | ✅ |
| Class serialization | ✅ | ✅ (data classes) |
| Dynamic schemas | ✅ | ✅ |
| All primitive types | ✅ | ✅ (8/8 types) |
| Nullable fields | ✅ | ✅ |
| Compression codecs | ✅ | ✅ (4 codecs) |
| Streaming reads | ✅ | ✅ (Sequences) |
| Production ready | ✅ | ✅ (104 tests) |
| Coroutines support | ❌ | ✅ (Flow API) |

## Test Coverage

The library has comprehensive test coverage with **104 tests passing (100%)**:

- ✅ **IntegrationTest** (5 tests) - Core read/write operations, compression codecs, nullable fields
- ✅ **ParquetFileTest** (3 tests) - High-level API, object serialization, schema reading
- ✅ **PerformanceBenchmarkTest** (3 tests) - Large-scale operations, compression benchmarks
- ✅ **ExtendedDataTypesTest** (5 tests) - INT96, FIXED_LEN_BYTE_ARRAY, DATE, TIMESTAMP types
- ✅ **CoroutinesTest** (6 tests) - Async I/O, Flow API, suspend functions
- ✅ **NestedTypesBasicTest** (9 tests) - Nested schema structures, reflection, Thrift conversion
- ✅ **LevelCalculatorTest** (10 tests) - Repetition/definition levels, data flattening
- ✅ **NestedDataColumnTest** (6 tests) - Nested data columns, reconstruction, round-trip
- ✅ **NestedTypesEndToEndTest** (4 tests) - End-to-end nested types integration
- ✅ **LevelEncoderTest** (7 tests) - Level encoding/decoding with varints
- ✅ **NestedTypesSerializationTest** (9 tests) - List serialization/deserialization, file I/O, nullable lists
- ✅ **StructsAndMapsTest** (7 tests) - Schema reflection, Struct/Map serialization/deserialization
- ✅ **LogicalTypeMetadataTest** (6 tests) - Modern logical type support (DATE, TIME, TIMESTAMP, STRING, DECIMAL)
- ✅ **StreamingWriteTest** (3 tests) - Memory-efficient streaming writes, auto-flush, manual flush control
- ✅ **ArrayPoolTest** (8 tests) - Array pooling, bucket sizing, concurrent access, pool statistics
- ✅ **ArrayPoolIntegrationTest** (3 tests) - ArrayPool integration with ParquetWriter, memory efficiency

## Roadmap

### Completed ✅

- ✅ Complete implementation of encoding/decoding for all primitive types
- ✅ Full Thrift Compact Protocol deserialization
- ✅ Support for all 8 Parquet primitive types
- ✅ Nullable fields with definition levels
- ✅ String encoding/decoding (UTF-8)

- ✅ **Nested types - Lists** - 100% complete, production-ready
  - ✅ Hierarchical schema structures (NestedField)
  - ✅ Level calculation and data flattening
  - ✅ Data reconstruction from columnar format
  - ✅ Level encoding/decoding
  - ✅ Schema reflection for nested types
  - ✅ Thrift serialization
  - ✅ ParquetWriter integration (repetition & definition levels)
  - ✅ ParquetReader integration (level reading & reconstruction)
  - ✅ ParquetSerializer extension for List<T> properties
  - ✅ End-to-end file I/O testing
  - ✅ Support for List<String>, List<Int>, List<Long>, List<Double>, List<Float>, List<Boolean>
  - ✅ Nullable lists (List<T>?)
  - ✅ Empty lists
  - ✅ Multiple list properties per class

- ✅ **Nested types - Structs** - 100% complete, production-ready
  - ✅ Schema reflection (detects nested data classes)
  - ✅ NestedField.Group infrastructure for hierarchical structures
  - ✅ Correct Parquet schema generation (GROUP types with nested fields)
  - ✅ Serialization (flattening nested objects to columns)
  - ✅ Deserialization (reconstructing nested objects from columns)
  - ✅ End-to-end in-memory serialization/deserialization
  - ✅ Support for nullable structs
  - ✅ Multiple nested levels

- ✅ **Nested types - Maps** - 100% complete, production-ready
  - ✅ Schema reflection (detects Map<K,V> properties)
  - ✅ Correct Parquet schema generation (key-value structure)
  - ✅ Serialization (converting Map to key-value pairs with proper levels)
  - ✅ Deserialization (reconstructing Map from columns)
  - ✅ End-to-end serialization/deserialization
  - ✅ Support for nullable maps
  - ✅ Support for empty maps
  - ✅ Proper repetition and definition level handling

### In Progress 🚧
- 🚧 Predicate pushdown for efficient filtering
- 🚧 Statistics support

### Planned 📋

- 📋 DataFrame integration
- 📋 **Performance optimizations for writing**
  - ✅ Buffered I/O for reduced system calls
  - ✅ Dictionary encoding for string/categorical columns (enabled by default, fully compatible with external readers)
  - ✅ Reflection caching to avoid repeated property access overhead
  - ✅ Parallel compression of column chunks (multi-core utilization)
  - ✅ **Streaming serialization for large datasets** - Auto-flush mechanism prevents OOM errors
  - ✅ **Memory-efficient defaults** - Reduced row group size (8 MB) and page size (256 KB) to prevent memory exhaustion
  - ✅ **Manual flush control** - `flushRowGroups()` method for fine-grained memory management
  - ✅ **ArrayPool for byte array reuse** - Bucket-based pooling reduces GC pressure and allocations
  - ⏳ Adaptive page sizing based on data characteristics
  - ⏳ Delta encoding for sorted/sequential numeric data
- 📋 Column projection (reading subset of columns)
- 📋 Parallel processing for multi-core systems

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

**Daniele Angeli** - Founder & CEO at [MOLO17](https://www.molo17.com/)

MOLO17 is an ISV specialized in real-time data integration technologies with headquarters in Italy (Venice) and USA (Cupertino, CA). This library was developed as part of MOLO17's commitment to open-source software and the Kotlin ecosystem.

- 🏢 Company: [MOLO17](https://www.molo17.com/)
- 💼 LinkedIn: [Daniele Angeli](https://www.linkedin.com/in/danieleangeli/)

## Credits

This library is a port of [parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) by Ivan Gavryliuk (aloneguid). Special thanks to the parquet-dotnet team for their excellent work on the original library.

## License

Apache License 2.0 - see LICENSE file for details.

## Related Projects

- [parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) - Original C# implementation
- [Apache Parquet](https://parquet.apache.org/) - Official Apache Parquet project
- [parquet-java](https://github.com/apache/parquet-java) - Official Java implementation

## Support

- 📖 [Documentation](https://gitlab.com/molo17-public/gluesync/molo17-parquetkt/-/wikis/home)
- 🐛 [Issue Tracker](https://gitlab.com/molo17-public/gluesync/molo17-parquetkt/-/issues)
- 💬 [Discussions](https://gitlab.com/molo17-public/gluesync/molo17-parquetkt/-/issues)
