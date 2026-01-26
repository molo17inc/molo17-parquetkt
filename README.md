# MOLO17 ParquetKt

A fully managed, pure Kotlin library for reading and writing Apache Parquet files. This is a port of the excellent [parquet-dotnet](https://github.com/aloneguid/parquet-dotnet) library from C# to Kotlin.

## Features

- 🚀 **Pure Kotlin** - No native dependencies, works anywhere Kotlin/JVM works
- 📖 **Read & Write** - Full support for reading and writing Parquet files
- 🎯 **Type-Safe** - Leverage Kotlin's type system for compile-time safety
- 🔄 **Serialization** - Automatic serialization/deserialization of Kotlin data classes
- 📊 **Schema Support** - Dynamic schema creation and reflection
- 🗜️ **Compression** - Support for SNAPPY, GZIP, LZ4, ZSTD, and more
- 🎨 **Multiple APIs** - High-level and low-level APIs for different use cases
- ⚡ **Streaming** - Memory-efficient streaming reads

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

### Primitive Types

- `Boolean` → `BOOLEAN`
- `Int` → `INT32`
- `Long` → `INT64`
- `Float` → `FLOAT`
- `Double` → `DOUBLE`
- `String` → `BYTE_ARRAY` (UTF-8)
- `ByteArray` → `BYTE_ARRAY`

### Logical Types

- `LocalDate` → `DATE`
- `LocalDateTime` → `TIMESTAMP`
- `BigDecimal` → `DECIMAL`
- `UUID` → `UUID`

### Complex Types

- `List<T>` → Repeated fields
- Nullable types → Optional fields

## Compression

Supported compression codecs:

```kotlin
import com.molo17.parquetkt.schema.CompressionCodec

// Available codecs
CompressionCodec.UNCOMPRESSED
CompressionCodec.SNAPPY    // Default, good balance
CompressionCodec.GZIP      // Better compression, slower
CompressionCodec.LZ4       // Fast compression
CompressionCodec.ZSTD      // Best compression
CompressionCodec.BROTLI
CompressionCodec.LZO
```

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

This library aims to provide a similar API to parquet-dotnet while leveraging Kotlin's language features:

| Feature | parquet-dotnet | MOLO17 ParquetKt |
|---------|----------------|------------------|
| Pure managed code | ✅ | ✅ |
| High-level API | ✅ | ✅ |
| Low-level API | ✅ | ✅ |
| Class serialization | ✅ | ✅ (data classes) |
| Dynamic schemas | ✅ | ✅ |
| All compressions | ✅ | ✅ |
| Streaming reads | ✅ | ✅ (Sequences) |
| Coroutines support | ❌ | 🚧 (planned) |

## Roadmap

- [ ] Complete implementation of encoding/decoding
- [ ] Full schema conversion (Parquet ↔ ParquetSchema)
- [ ] Support for nested types (structs, lists, maps)
- [ ] Coroutines support for async I/O
- [ ] Predicate pushdown for efficient filtering
- [ ] Statistics support
- [ ] Metadata access
- [ ] DataFrame integration
- [ ] Performance optimizations

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

**Daniele Angeli** - Founder & CEO at [MOLO17](https://www.molo17.com/)

MOLO17 is an Italian software development company specializing in innovative digital solutions, mobile applications, and enterprise software. This library was developed as part of MOLO17's commitment to open-source software and the Kotlin ecosystem.

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
