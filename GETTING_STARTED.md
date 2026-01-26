# Getting Started with molo17-parquetkt

This guide will help you get started with the molo17-parquetkt library, whether you're porting code from parquet-dotnet or starting fresh.

## Prerequisites

- JDK 17 or higher
- Gradle 8.0+ (or use the included wrapper)
- Basic knowledge of Kotlin
- Familiarity with Apache Parquet format (helpful but not required)

## Building the Project

```bash
# Clone the repository
git clone <your-repo-url>
cd molo17-parquetkt

# Build the project
./gradlew build

# Run tests
./gradlew test

# Run examples
./gradlew run
```

## Quick Start Examples

### Example 1: Write and Read Simple Data

```kotlin
import com.molo17.parquetkt.core.ParquetFile

data class Product(
    val id: Long,
    val name: String,
    val price: Double,
    val inStock: Boolean
)

fun main() {
    // Create data
    val products = listOf(
        Product(1L, "Laptop", 999.99, true),
        Product(2L, "Mouse", 29.99, true),
        Product(3L, "Keyboard", 79.99, false)
    )
    
    // Write to Parquet file
    ParquetFile.writeObjects("products.parquet", products)
    
    // Read back
    val readProducts = ParquetFile.readObjects<Product>("products.parquet")
    
    readProducts.forEach { println(it) }
}
```

### Example 2: Working with Nullable Fields

```kotlin
data class Employee(
    val id: Long,
    val name: String,
    val email: String?,      // Nullable
    val department: String?, // Nullable
    val salary: Double
)

val employees = listOf(
    Employee(1L, "Alice", "alice@example.com", "Engineering", 75000.0),
    Employee(2L, "Bob", null, "Sales", 65000.0),
    Employee(3L, "Charlie", "charlie@example.com", null, 85000.0)
)

ParquetFile.writeObjects("employees.parquet", employees)
```

### Example 3: Using Different Compression

```kotlin
import com.molo17.parquetkt.schema.CompressionCodec

// SNAPPY (default, good balance)
ParquetFile.writeObjects("data_snappy.parquet", data, CompressionCodec.SNAPPY)

// GZIP (better compression, slower)
ParquetFile.writeObjects("data_gzip.parquet", data, CompressionCodec.GZIP)

// ZSTD (best compression)
ParquetFile.writeObjects("data_zstd.parquet", data, CompressionCodec.ZSTD)

// Uncompressed
ParquetFile.writeObjects("data_raw.parquet", data, CompressionCodec.UNCOMPRESSED)
```

### Example 4: Streaming Large Files

```kotlin
// Memory-efficient reading with sequences
val rows = ParquetFile.readRowsAsSequence("large_file.parquet")

rows
    .filter { it["age"] as Int > 30 }
    .take(100)
    .forEach { row ->
        println("${row["name"]}: ${row["age"]}")
    }
```

### Example 5: Low-Level API

```kotlin
import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema

// Define schema manually
val schema = ParquetSchema.create(
    DataField.int64("id"),
    DataField.string("name"),
    DataField.double("value")
)

// Create columns
val ids = DataColumn.createRequired(
    DataField.int64("id"),
    listOf(1L, 2L, 3L)
)

val names = DataColumn.createRequired(
    DataField.string("name"),
    listOf("Alice", "Bob", "Charlie")
)

val values = DataColumn.createRequired(
    DataField.double("value"),
    listOf(100.0, 200.0, 300.0)
)

// Create row group
val rowGroup = RowGroup(schema, listOf(ids, names, values))

// Write to file
ParquetFile.write("data.parquet", schema, listOf(rowGroup))
```

### Example 6: Schema Reflection

```kotlin
import com.molo17.parquetkt.serialization.SchemaReflector

data class User(
    val id: Long,
    val username: String,
    val email: String?,
    val age: Int,
    val balance: Double,
    val isActive: Boolean
)

// Automatically generate schema from data class
val schema = SchemaReflector.reflectSchema<User>()

println(schema)
// Output:
// ParquetSchema {
//   id: INT64 (REQUIRED)
//   username: BYTE_ARRAY (REQUIRED)
//   email: BYTE_ARRAY (OPTIONAL)
//   age: INT32 (REQUIRED)
//   balance: DOUBLE (REQUIRED)
//   isActive: BOOLEAN (REQUIRED)
// }
```

## Common Use Cases

### Use Case 1: Data Export

Export application data to Parquet for analytics:

```kotlin
fun exportUsersToParquet(users: List<User>, outputPath: String) {
    ParquetFile.writeObjects(outputPath, users, CompressionCodec.SNAPPY)
    println("Exported ${users.size} users to $outputPath")
}
```

### Use Case 2: Data Import

Import Parquet data into your application:

```kotlin
fun importUsersFromParquet(inputPath: String): List<User> {
    return ParquetFile.readObjects<User>(inputPath)
}
```

### Use Case 3: Data Transformation

Read, transform, and write Parquet data:

```kotlin
fun transformData(inputPath: String, outputPath: String) {
    val data = ParquetFile.readObjects<InputType>(inputPath)
    
    val transformed = data.map { input ->
        OutputType(
            id = input.id,
            value = input.value * 2,
            // ... transform fields
        )
    }
    
    ParquetFile.writeObjects(outputPath, transformed)
}
```

### Use Case 4: Batch Processing

Process large files in batches:

```kotlin
fun processBatches(inputPath: String, batchSize: Int) {
    val rows = ParquetFile.readRowsAsSequence(inputPath)
    
    rows.chunked(batchSize).forEach { batch ->
        // Process batch
        processBatch(batch)
    }
}
```

## Supported Data Types

| Kotlin Type | Parquet Type | Notes |
|-------------|--------------|-------|
| `Boolean` | `BOOLEAN` | |
| `Int` | `INT32` | |
| `Long` | `INT64` | |
| `Float` | `FLOAT` | |
| `Double` | `DOUBLE` | |
| `String` | `BYTE_ARRAY` | UTF-8 encoded |
| `ByteArray` | `BYTE_ARRAY` | Raw bytes |
| `LocalDate` | `INT32` | Days since epoch |
| `LocalDateTime` | `INT64` | Milliseconds since epoch |
| `T?` | Optional field | Any nullable type |

## Best Practices

### 1. Use Data Classes

```kotlin
// ✅ Good: Data class with immutable properties
data class Record(
    val id: Long,
    val name: String,
    val value: Double
)

// ❌ Avoid: Mutable classes
class Record {
    var id: Long = 0
    var name: String = ""
    var value: Double = 0.0
}
```

### 2. Choose Appropriate Compression

```kotlin
// For fast writes and good compression
CompressionCodec.SNAPPY

// For maximum compression (slower)
CompressionCodec.ZSTD

// For compatibility
CompressionCodec.GZIP
```

### 3. Use Sequences for Large Files

```kotlin
// ✅ Good: Memory-efficient
val rows = ParquetFile.readRowsAsSequence("large.parquet")
rows.filter { ... }.take(100)

// ❌ Avoid: Loads entire file into memory
val rows = ParquetFile.readObjects<Record>("large.parquet")
```

### 4. Handle Resources Properly

```kotlin
// ✅ Good: Automatic resource cleanup
ParquetReader.open("data.parquet").use { reader ->
    // Use reader
}

// ❌ Avoid: Manual cleanup (error-prone)
val reader = ParquetReader.open("data.parquet")
try {
    // Use reader
} finally {
    reader.close()
}
```

### 5. Validate Data Before Writing

```kotlin
fun writeData(data: List<Record>, path: String) {
    require(data.isNotEmpty()) { "Data cannot be empty" }
    require(data.all { it.id > 0 }) { "Invalid IDs found" }
    
    ParquetFile.writeObjects(path, data)
}
```

## Troubleshooting

### Issue: "NotImplementedError: Schema conversion not yet implemented"

**Cause**: The low-level schema conversion is not yet complete.

**Solution**: Use the high-level API with data classes:
```kotlin
// Instead of low-level API
ParquetFile.write(path, schema, rowGroups)

// Use high-level API
ParquetFile.writeObjects(path, data)
```

### Issue: "Unsupported type" when using reflection

**Cause**: The type is not yet supported by SchemaReflector.

**Solution**: Use supported primitive types or manually define the schema.

### Issue: File not found or permission denied

**Cause**: Invalid file path or insufficient permissions.

**Solution**: Check file path and permissions:
```kotlin
val file = File("data.parquet")
println("Exists: ${file.exists()}")
println("Can read: ${file.canRead()}")
println("Can write: ${file.canWrite()}")
```

## Next Steps

1. **Read the [README.md](README.md)** for comprehensive documentation
2. **Check [PORTING_GUIDE.md](PORTING_GUIDE.md)** if migrating from parquet-dotnet
3. **Review [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md)** to see what's implemented
4. **Explore examples** in `src/main/kotlin/io/github/parquetkt/examples/`
5. **Run tests** to see more usage patterns

## Getting Help

- 📖 Read the documentation
- 🐛 Report issues on GitHub
- 💬 Ask questions in discussions
- 📧 Contact maintainers

## Contributing

Contributions are welcome! The library is in active development and needs help with:

- Implementing encoding/decoding
- Adding nested type support
- Performance optimization
- Documentation improvements
- More examples and tests

See [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for areas that need work.
