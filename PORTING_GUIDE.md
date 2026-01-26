# Porting Guide: C# (parquet-dotnet) to Kotlin

This document provides guidance on porting code from parquet-dotnet (C#) to molo17-parquetkt.

## Table of Contents

- [Language Differences](#language-differences)
- [API Mapping](#api-mapping)
- [Type Conversions](#type-conversions)
- [Common Patterns](#common-patterns)
- [Examples](#examples)

## Language Differences

### Properties vs Fields

**C# (parquet-dotnet):**
```csharp
public class DataField
{
    public string Name { get; set; }
    public ParquetType DataType { get; set; }
    public bool IsNullable { get; set; }
}
```

**Kotlin (molo17-parquetkt):**
```kotlin
data class DataField(
    val name: String,
    val dataType: ParquetType,
    val isNullable: Boolean
)
```

### Nullable Types

**C# (parquet-dotnet):**
```csharp
string? nullableString;
int? nullableInt;
```

**Kotlin (molo17-parquetkt):**
```kotlin
val nullableString: String?
val nullableInt: Int?
```

### Collections

**C# (parquet-dotnet):**
```csharp
List<DataField> fields = new List<DataField>();
Array data = new int[] { 1, 2, 3 };
```

**Kotlin (molo17-parquetkt):**
```kotlin
val fields = mutableListOf<DataField>()
val data = arrayOf(1, 2, 3)
```

### LINQ vs Sequences

**C# (parquet-dotnet):**
```csharp
var filtered = data
    .Where(x => x.Age > 30)
    .Select(x => x.Name)
    .ToList();
```

**Kotlin (molo17-parquetkt):**
```kotlin
val filtered = data
    .filter { it.age > 30 }
    .map { it.name }
    .toList()
```

## API Mapping

### Writing Parquet Files

**C# (parquet-dotnet):**
```csharp
using Parquet;
using Parquet.Data;

var schema = new ParquetSchema(
    new DataField<int>("id"),
    new DataField<string>("name"),
    new DataField<double>("salary")
);

var data = new List<Person> {
    new Person { Id = 1, Name = "Alice", Salary = 75000 }
};

using (Stream fileStream = File.OpenWrite("data.parquet"))
{
    using (var parquetWriter = await ParquetWriter.CreateAsync(schema, fileStream))
    {
        using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
        {
            await groupWriter.WriteColumnAsync(new DataColumn(
                schema.DataFields[0],
                data.Select(x => x.Id).ToArray()
            ));
        }
    }
}
```

**Kotlin (molo17-parquetkt):**
```kotlin
import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema

data class Person(val id: Int, val name: String, val salary: Double)

val data = listOf(
    Person(1, "Alice", 75000.0)
)

// High-level API (recommended)
ParquetFile.writeObjects("data.parquet", data)

// Or low-level API
val schema = ParquetSchema.create(
    DataField.int32("id"),
    DataField.string("name"),
    DataField.double("salary")
)
// ... create columns and row groups
```

### Reading Parquet Files

**C# (parquet-dotnet):**
```csharp
using (Stream fileStream = File.OpenRead("data.parquet"))
{
    using (var parquetReader = await ParquetReader.CreateAsync(fileStream))
    {
        for (int i = 0; i < parquetReader.RowGroupCount; i++)
        {
            using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
            {
                DataColumn[] columns = await groupReader.ReadEntireRowGroupAsync();
                // Process columns
            }
        }
    }
}
```

**Kotlin (molo17-parquetkt):**
```kotlin
// High-level API
val data = ParquetFile.readObjects<Person>("data.parquet")

// Streaming API
val rows = ParquetFile.readRowsAsSequence("data.parquet")
rows.forEach { row ->
    println(row)
}

// Low-level API
ParquetReader.open("data.parquet").use { reader ->
    for (i in 0 until reader.rowGroupCount) {
        val rowGroup = reader.readRowGroup(i)
        // Process row group
    }
}
```

### Schema Definition

**C# (parquet-dotnet):**
```csharp
var schema = new ParquetSchema(
    new DataField<int>("id"),
    new DataField<string>("name") { IsNullable = true },
    new DataField<DateTime>("created")
);
```

**Kotlin (molo17-parquetkt):**
```kotlin
val schema = ParquetSchema.create(
    DataField.int32("id", nullable = false),
    DataField.string("name", nullable = true),
    DataField.timestamp("created", nullable = false)
)
```

## Type Conversions

| C# Type | Kotlin Type | Parquet Type |
|---------|-------------|--------------|
| `bool` | `Boolean` | `BOOLEAN` |
| `int` | `Int` | `INT32` |
| `long` | `Long` | `INT64` |
| `float` | `Float` | `FLOAT` |
| `double` | `Double` | `DOUBLE` |
| `string` | `String` | `BYTE_ARRAY` (UTF-8) |
| `byte[]` | `ByteArray` | `BYTE_ARRAY` |
| `DateTime` | `LocalDateTime` | `TIMESTAMP_MILLIS` |
| `DateTimeOffset` | `Instant` | `TIMESTAMP_MILLIS` |
| `decimal` | `BigDecimal` | `DECIMAL` |
| `Guid` | `UUID` | `UUID` |

## Common Patterns

### Using Statement vs Use Function

**C# (parquet-dotnet):**
```csharp
using (var writer = ParquetWriter.Create(schema, stream))
{
    // Use writer
}
// Automatically disposed
```

**Kotlin (molo17-parquetkt):**
```kotlin
ParquetWriter.create(file, schema).use { writer ->
    // Use writer
}
// Automatically closed
```

### Async/Await vs Coroutines

**C# (parquet-dotnet):**
```csharp
public async Task<List<Person>> ReadDataAsync(string path)
{
    using (var stream = File.OpenRead(path))
    {
        using (var reader = await ParquetReader.CreateAsync(stream))
        {
            // Read data
        }
    }
}
```

**Kotlin (molo17-parquetkt):**
```kotlin
// Currently synchronous, coroutines support planned
fun readData(path: String): List<Person> {
    return ParquetFile.readObjects(path)
}

// Future coroutines support:
suspend fun readDataAsync(path: String): List<Person> {
    return withContext(Dispatchers.IO) {
        ParquetFile.readObjects(path)
    }
}
```

### Extension Methods vs Extension Functions

**C# (parquet-dotnet):**
```csharp
public static class DataFieldExtensions
{
    public static DataField WithNullable(this DataField field, bool nullable)
    {
        field.IsNullable = nullable;
        return field;
    }
}

var field = new DataField<string>("name").WithNullable(true);
```

**Kotlin (molo17-parquetkt):**
```kotlin
fun DataField.withNullable(nullable: Boolean): DataField {
    return copy(repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED)
}

val field = DataField.string("name").withNullable(true)
```

### Class Serialization

**C# (parquet-dotnet):**
```csharp
public class Employee
{
    public int Id { get; set; }
    public string Name { get; set; }
    public double Salary { get; set; }
}

var employees = new List<Employee> { /* ... */ };

using (var stream = File.OpenWrite("employees.parquet"))
{
    await ParquetSerializer.SerializeAsync(employees, stream);
}

using (var stream = File.OpenRead("employees.parquet"))
{
    var employees = await ParquetSerializer.DeserializeAsync<Employee>(stream);
}
```

**Kotlin (molo17-parquetkt):**
```kotlin
data class Employee(
    val id: Int,
    val name: String,
    val salary: Double
)

val employees = listOf(/* ... */)

// Write
ParquetFile.writeObjects("employees.parquet", employees)

// Read
val employees = ParquetFile.readObjects<Employee>("employees.parquet")
```

## Examples

### Example 1: Simple Write and Read

**C# (parquet-dotnet):**
```csharp
public class Record
{
    public int Id { get; set; }
    public string Name { get; set; }
}

var records = new List<Record>
{
    new Record { Id = 1, Name = "Alice" },
    new Record { Id = 2, Name = "Bob" }
};

await ParquetSerializer.SerializeAsync(records, "data.parquet");
var readRecords = await ParquetSerializer.DeserializeAsync<Record>("data.parquet");
```

**Kotlin (molo17-parquetkt):**
```kotlin
data class Record(
    val id: Int,
    val name: String
)

val records = listOf(
    Record(1, "Alice"),
    Record(2, "Bob")
)

ParquetFile.writeObjects("data.parquet", records)
val readRecords = ParquetFile.readObjects<Record>("data.parquet")
```

### Example 2: Working with Nullable Fields

**C# (parquet-dotnet):**
```csharp
public class User
{
    public int Id { get; set; }
    public string? Email { get; set; }  // Nullable
}

var users = new List<User>
{
    new User { Id = 1, Email = "alice@example.com" },
    new User { Id = 2, Email = null }
};
```

**Kotlin (molo17-parquetkt):**
```kotlin
data class User(
    val id: Int,
    val email: String?  // Nullable
)

val users = listOf(
    User(1, "alice@example.com"),
    User(2, null)
)
```

### Example 3: Compression

**C# (parquet-dotnet):**
```csharp
var options = new ParquetSerializerOptions
{
    CompressionMethod = CompressionMethod.Snappy
};

await ParquetSerializer.SerializeAsync(data, stream, options);
```

**Kotlin (molo17-parquetkt):**
```kotlin
ParquetFile.writeObjects(
    "data.parquet",
    data,
    CompressionCodec.SNAPPY
)
```

## Key Differences Summary

1. **Immutability**: Kotlin favors immutable data structures (`val`, `data class`)
2. **Null Safety**: Kotlin has built-in null safety with `?` operator
3. **No Async/Await**: Current implementation is synchronous (coroutines planned)
4. **Data Classes**: Kotlin's data classes are more concise than C# POCOs
5. **Extension Functions**: Similar to C# extension methods but more idiomatic
6. **Sequences**: Kotlin sequences are lazy like C# IEnumerable
7. **Resource Management**: `use` function instead of `using` statement

## Migration Checklist

- [ ] Convert C# classes to Kotlin data classes
- [ ] Replace nullable reference types (`?`) with Kotlin nullable types
- [ ] Convert `async/await` to synchronous calls (or wait for coroutines support)
- [ ] Replace LINQ queries with Kotlin collection operations
- [ ] Update `using` statements to `use` functions
- [ ] Convert properties to constructor parameters in data classes
- [ ] Replace `List<T>` with `List<T>` or `MutableList<T>`
- [ ] Update DateTime to LocalDateTime or Instant
- [ ] Convert extension methods to extension functions

## Need Help?

- Check the [README.md](README.md) for more examples
- Look at the test files in `src/test/kotlin/io/github/parquetkt/`
- Review the example code in `src/main/kotlin/io/github/parquetkt/examples/`
- Open an issue on GitHub for specific questions
