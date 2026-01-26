# Nested Types Implementation Roadmap

## Current Status

ParquetKt currently supports all primitive types and basic repeated fields. Full nested type support (structs, lists, maps) with proper Parquet 3-level structure is a complex feature requiring significant architectural changes.

## What's Implemented ✅

### Basic Repeated Fields
- Simple repeated primitive types (e.g., `List<Int>`, `List<String>`)
- Repetition level = 1 for basic lists
- Factory methods: `DataField.int32List()`, `DataField.stringList()`, etc.

### All Primitive Types
- BOOLEAN, INT32, INT64, FLOAT, DOUBLE
- BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96
- DATE, TIMESTAMP logical types

## What's Planned 🚧

### Phase 1: Enhanced Repeated Fields (Short-term)
- [ ] Proper 3-level list structure (list → element → value)
- [ ] Nested definition/repetition level calculation
- [ ] List encoding/decoding with proper levels
- [ ] Support for `List<T>` in data classes

### Phase 2: Struct/Group Support (Medium-term)
- [ ] Nested schema groups
- [ ] Struct field definitions
- [ ] Nested object serialization
- [ ] Support for nested data classes

### Phase 3: Map Support (Medium-term)
- [ ] Map schema structure (key_value group)
- [ ] Map encoding/decoding
- [ ] Support for `Map<K, V>` in data classes

### Phase 4: Complex Nesting (Long-term)
- [ ] Lists of structs
- [ ] Maps with complex values
- [ ] Deeply nested structures
- [ ] Arbitrary nesting depth

## Technical Challenges

### 1. Schema Representation
Parquet uses a flattened schema with groups. Example:
```
message Document {
  required int64 id;
  optional group addresses (LIST) {
    repeated group list {
      optional group element {
        required binary street (STRING);
        required binary city (STRING);
      }
    }
  }
}
```

This requires:
- Hierarchical schema structure
- Group field support
- Proper parent-child relationships

### 2. Repetition and Definition Levels
Nested structures require complex level calculation:
- Definition levels track null values at each nesting level
- Repetition levels track list boundaries
- Levels must be calculated for the entire path

### 3. Encoding/Decoding
- Values are stored in columnar format
- Levels are stored separately
- Reconstruction requires level interpretation

### 4. Serialization
- Reflection for nested Kotlin data classes
- Flattening nested structures to columns
- Reconstructing nested objects from columns

## Current Workaround

For now, users can:
1. Use basic repeated fields for simple lists
2. Flatten nested structures manually
3. Use separate columns for related data
4. Store JSON/serialized data in BYTE_ARRAY fields

## Example: Basic Repeated Fields

```kotlin
// Define schema with repeated field
val schema = ParquetSchema.create(
    DataField.int64("id"),
    DataField.stringList("tags")  // List of strings
)

// Note: Full nested support coming in future releases
```

## Contributing

If you're interested in helping implement nested types support, please:
1. Review the Parquet format specification
2. Check existing issues/PRs
3. Reach out to discuss implementation approach

## References

- [Parquet Format Specification](https://github.com/apache/parquet-format)
- [Dremel Paper](https://research.google/pubs/pub36632/) - Original nested data encoding
- [parquet-dotnet Nested Types](https://github.com/aloneguid/parquet-dotnet/blob/master/docs/nested-types.md)
