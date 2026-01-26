# Encoder/Decoder Integration Plan for Nested Types

## Current Status

### ✅ Completed Components
1. **NestedField.kt** - Schema structures for lists, maps, structs
2. **LevelCalculator.kt** - Calculate and flatten data with levels
3. **NestedDataColumn.kt** - Column structure with values + levels
4. **NestedDataReconstructor** - Reconstruct nested data from levels
5. **LevelEncoder.kt** - Encode/decode levels (varint format)

### 🚧 Integration Needed

#### Phase 1: Writer Integration
**Goal:** Enable writing nested data to Parquet files

**Components to modify:**
1. **ParquetWriter.kt** - Handle NestedDataColumn with levels
   - Detect when writing nested columns
   - Write definition levels before data
   - Write repetition levels before data
   - Update page header with level byte lengths

2. **DataColumn.kt** - Extend to support levels
   - Add optional `definitionLevels` and `repetitionLevels` properties
   - Factory methods for creating columns with levels

#### Phase 2: Reader Integration
**Goal:** Enable reading nested data from Parquet files

**Components to modify:**
1. **ParquetReader.kt** - Read and reconstruct nested data
   - Read definition levels from pages
   - Read repetition levels from pages
   - Pass levels to decoder
   - Reconstruct nested structures

2. **PlainDecoder.kt** - Decode with level awareness
   - Accept definition/repetition levels
   - Decode values with proper null handling

#### Phase 3: Serializer Integration
**Goal:** Auto-serialize data classes with List<T> properties

**Components to modify:**
1. **ParquetSerializer.kt** - Detect and handle List properties
   - Detect `List<T>` properties via reflection
   - Flatten list data using LevelCalculator
   - Create NestedDataColumn for list fields

2. **ParquetDeserializer.kt** - Reconstruct List properties
   - Detect list fields in schema
   - Use NestedDataReconstructor to rebuild lists
   - Assign to data class properties

## Implementation Steps

### Step 1: Extend DataColumn for Levels
```kotlin
// Add to DataColumn.kt
data class DataColumn<T>(
    val field: DataField,
    val data: Array<T?>,
    val definitionLevels: IntArray? = null,  // NEW
    val repetitionLevels: IntArray? = null   // NEW
) {
    companion object {
        fun <T> createWithLevels(
            field: DataField,
            data: Array<T?>,
            definitionLevels: IntArray,
            repetitionLevels: IntArray
        ): DataColumn<T> = DataColumn(field, data, definitionLevels, repetitionLevels)
    }
}
```

### Step 2: Modify ParquetWriter for Levels
```kotlin
// In ParquetWriter.writeColumnChunk()
private fun writeColumnChunk(column: DataColumn<*>, ...) {
    // Write repetition levels if present
    if (column.repetitionLevels != null) {
        val repLevelData = LevelEncoder.encodeLevels(column.repetitionLevels.toList())
        output.write(repLevelData)
    }
    
    // Write definition levels if present
    if (column.definitionLevels != null) {
        val defLevelData = LevelEncoder.encodeLevels(column.definitionLevels.toList())
        output.write(defLevelData)
    }
    
    // Write values (existing code)
    val encodedData = encoder.encode(column.definedData)
    output.write(encodedData)
}
```

### Step 3: Modify ParquetReader for Levels
```kotlin
// In ParquetReader.readColumn()
private fun readColumn(columnChunk: ColumnChunk): DataColumn<*> {
    val maxRepLevel = field.maxRepetitionLevel
    val maxDefLevel = field.maxDefinitionLevel
    
    // Read repetition levels if needed
    val repLevels = if (maxRepLevel > 0) {
        val repData = readLevelData(...)
        LevelEncoder.decodeLevels(repData, valueCount)
    } else null
    
    // Read definition levels if needed
    val defLevels = if (maxDefLevel > 0) {
        val defData = readLevelData(...)
        LevelEncoder.decodeLevels(defData, valueCount)
    } else null
    
    // Read values (existing code)
    val values = decoder.decode(...)
    
    return DataColumn.createWithLevels(field, values, defLevels, repLevels)
}
```

### Step 4: Extend ParquetSerializer
```kotlin
// In ParquetSerializer.serialize()
fun serialize(objects: List<T>): RowGroup {
    val columns = schema.fields.map { field ->
        if (isListField(field)) {
            // Extract list data from objects
            val listData = objects.map { obj -> 
                getPropertyValue(obj, field.name) as List<*>?
            }
            
            // Flatten using LevelCalculator
            val nestedField = getNestedField(field)
            val column = NestedDataColumn.createForList(nestedField, listData)
            
            // Convert to DataColumn with levels
            DataColumn.createWithLevels(
                field, 
                column.values.toTypedArray(),
                column.definitionLevels.toIntArray(),
                column.repetitionLevels.toIntArray()
            )
        } else {
            // Existing primitive field handling
            createPrimitiveColumn(field, objects)
        }
    }
    
    return RowGroup(columns, objects.size)
}
```

## Testing Strategy

### Unit Tests
1. ✅ LevelEncoder encode/decode
2. ✅ NestedDataColumn creation
3. ✅ NestedDataReconstructor
4. 🚧 DataColumn with levels
5. 🚧 Write/read column with levels

### Integration Tests
1. 🚧 Write List<String> to file
2. 🚧 Read List<String> from file
3. 🚧 Round-trip data class with List property
4. 🚧 Compatibility with parquet-dotnet files

### End-to-End Tests
1. 🚧 Serialize data class with List<String>
2. 🚧 Deserialize data class with List<String>
3. 🚧 Multiple lists in same file
4. 🚧 Nested lists (List<List<T>>)

## Estimated Effort

- Step 1 (DataColumn extension): ~50 lines, 1 hour
- Step 2 (Writer integration): ~100 lines, 2 hours
- Step 3 (Reader integration): ~100 lines, 2 hours
- Step 4 (Serializer integration): ~150 lines, 3 hours
- Testing & debugging: 4 hours

**Total: ~400 lines, ~12 hours**

## Success Criteria

✅ Write data class with `List<String>` property to Parquet file
✅ Read data class with `List<String>` property from Parquet file
✅ Perfect round-trip (write → read → identical data)
✅ Files readable by parquet-dotnet
✅ All existing tests still passing
✅ New tests for nested types passing

## Next Immediate Actions

1. Run test for LevelEncoder
2. Extend DataColumn to support levels
3. Modify ParquetWriter.writeColumnChunk() for levels
4. Create test: write simple list to file
5. Modify ParquetReader for levels
6. Create test: read simple list from file
7. Extend ParquetSerializer for List properties
8. Create end-to-end test with data class
