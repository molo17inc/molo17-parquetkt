/*
 * Copyright 2026 MOLO17
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.molo17.parquetkt.serialization

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.NestedDataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.NestedField
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.ParquetType
import com.molo17.parquetkt.schema.LogicalType
import com.molo17.parquetkt.schema.Repetition
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.full.starProjectedType

class ParquetSerializer<T : Any>(
    private val clazz: KClass<T>
) {
    private val properties: List<KProperty1<T, *>> = clazz.memberProperties.toList()
    
    fun serialize(data: List<T>, schema: ParquetSchema): RowGroup {
        val columns = if (schema.hasNestedStructure && schema.nestedFields != null) {
            // Handle nested structures - flatten to leaf columns
            serializeNestedFields(data, schema.nestedFields)
        } else {
            // Simple flat structure
            schema.fields.map { field ->
                serializeColumn(data, field)
            }
        }
        return RowGroup(schema, columns)
    }
    
    private fun serializeNestedFields(data: List<T>, nestedFields: List<NestedField>): List<DataColumn<*>> {
        val columns = mutableListOf<DataColumn<*>>()
        
        for (field in nestedFields) {
            when (field) {
                is NestedField.Primitive -> {
                    // Simple field - serialize directly
                    columns.add(serializeColumn(data, field.toDataField()))
                }
                is NestedField.Group -> {
                    // Check if this is a struct, list, or map
                    when (field.logicalType) {
                        LogicalType.LIST -> {
                            // Handle list - find the corresponding property and serialize
                            val property = properties.find { it.name == field.name }
                            if (property != null && isListProperty(property)) {
                                // Get the leaf field for the list
                                val leafFields = field.getLeafFields()
                                if (leafFields.isNotEmpty()) {
                                    columns.add(serializeListColumn(data, leafFields.first().toDataField(), property))
                                }
                            }
                        }
                        LogicalType.MAP -> {
                            // Handle map - flatten to key-value columns
                            columns.addAll(serializeMapField(data, field))
                        }
                        else -> {
                            // Regular struct - flatten to leaf columns
                            columns.addAll(serializeStructField(data, field))
                        }
                    }
                }
            }
        }
        
        return columns
    }
    
    private fun serializeColumn(data: List<T>, field: DataField): DataColumn<*> {
        val property = properties.find { it.name == field.name }
            ?: throw IllegalArgumentException("Property ${field.name} not found in class ${clazz.simpleName}")
        
        property.isAccessible = true
        
        // Check if property is a List type
        if (isListProperty(property)) {
            return serializeListColumn(data, field, property)
        }
        
        // Check if property is a Map type
        if (isMapProperty(property)) {
            return serializeMapColumn(data, field, property)
        }
        
        // Check if property is a nested data class (Struct)
        if (isStructProperty(property)) {
            throw UnsupportedOperationException(
                "Struct properties should be handled via serializeNestedFields, not serializeColumn"
            )
        }
        
        val values = data.map { obj ->
            property.get(obj)
        }
        
        return DataColumn.create(field, values)
    }
    
    private fun isListProperty(property: KProperty1<T, *>): Boolean {
        val returnType = property.returnType
        val classifier = returnType.classifier
        return classifier == List::class
    }
    
    private fun isMapProperty(property: KProperty1<T, *>): Boolean {
        val returnType = property.returnType
        val classifier = returnType.classifier
        return classifier == Map::class
    }
    
    private fun isStructProperty(property: KProperty1<T, *>): Boolean {
        val returnType = property.returnType
        val classifier = returnType.classifier as? KClass<*>
        return classifier != null && classifier.isData
    }
    
    private fun serializeListColumn(
        data: List<T>,
        field: DataField,
        property: KProperty1<T, *>
    ): DataColumn<*> {
        // Extract list data from objects
        val listData = data.map { obj ->
            @Suppress("UNCHECKED_CAST")
            property.get(obj) as? List<*>
        }
        
        // Create NestedField for the list
        val nestedField = createNestedFieldForList(field, property)
        
        // Flatten using NestedDataColumn
        @Suppress("UNCHECKED_CAST")
        val column = NestedDataColumn.createForList<Any?>(nestedField, listData as List<List<Any?>?>)
        
        // Get the element type from the nested field to create the correct field for the column
        val elementField = getElementFieldFromNestedField(nestedField)
        
        // Use property nullability, not field nullability, to determine the correct levels
        val isPropertyNullable = property.returnType.isMarkedNullable
        val columnField = DataField(
            name = field.name,
            dataType = elementField.dataType,
            logicalType = elementField.logicalType ?: LogicalType.NONE,
            repetition = if (isPropertyNullable) Repetition.OPTIONAL else Repetition.REQUIRED,
            maxRepetitionLevel = 1,
            maxDefinitionLevel = if (isPropertyNullable) 3 else 1
        )
        
        // Store the flattened values in the data array
        // The levels arrays indicate the structure and boundaries
        @Suppress("UNCHECKED_CAST")
        return DataColumn(
            field = columnField,
            data = column.values.toTypedArray() as Array<Any?>,
            definitionLevels = column.definitionLevels.toIntArray(),
            repetitionLevels = column.repetitionLevels.toIntArray()
        ) as DataColumn<*>
    }
    
    private fun serializeStructField(data: List<T>, structField: NestedField.Group): List<DataColumn<*>> {
        // Find the property for this struct
        val property = properties.find { it.name == structField.name }
            ?: throw IllegalArgumentException("Property ${structField.name} not found in class ${clazz.simpleName}")
        
        property.isAccessible = true
        
        // Get all leaf fields from the struct
        val leafFields = structField.getLeafFields()
        val columns = mutableListOf<DataColumn<*>>()
        
        // For each leaf field, extract values from the nested objects
        for (leafField in leafFields) {
            val values = data.map { obj ->
                val structValue = property.get(obj)
                if (structValue == null) {
                    null
                } else {
                    // Extract the nested field value
                    extractNestedValue(structValue, leafField.name)
                }
            }
            
            columns.add(DataColumn.create(leafField.toDataField(), values))
        }
        
        return columns
    }
    
    private fun extractNestedValue(obj: Any, fieldName: String): Any? {
        val kClass = obj::class
        val property = kClass.memberProperties.find { it.name == fieldName }
            ?: throw IllegalArgumentException("Property $fieldName not found in ${kClass.simpleName}")
        
        property.isAccessible = true
        return (property as KProperty1<Any, *>).get(obj)
    }
    
    private fun serializeMapField(data: List<T>, mapField: NestedField.Group): List<DataColumn<*>> {
        // Find the property for this map
        val property = properties.find { it.name == mapField.name }
            ?: throw IllegalArgumentException("Property ${mapField.name} not found in class ${clazz.simpleName}")
        
        property.isAccessible = true
        
        // Get the key-value struct from the map field
        val keyValueStruct = mapField.children.first() as NestedField.Group
        val keyField = keyValueStruct.children[0] as NestedField.Primitive
        val valueField = keyValueStruct.children[1] as NestedField.Primitive
        
        // Flatten maps to key-value pairs with proper levels
        val keyValues = mutableListOf<Any?>()
        val valueValues = mutableListOf<Any?>()
        val definitionLevels = mutableListOf<Int>()
        val repetitionLevels = mutableListOf<Int>()
        
        for (obj in data) {
            val map = property.get(obj) as? Map<*, *>
            
            if (map == null) {
                // Null map: definition level 0
                definitionLevels.add(0)
                repetitionLevels.add(0)
            } else if (map.isEmpty()) {
                // Empty map: definition level 1
                definitionLevels.add(1)
                repetitionLevels.add(0)
            } else {
                // Non-empty map: add each key-value pair
                var isFirst = true
                for ((key, value) in map.entries) {
                    keyValues.add(key)
                    valueValues.add(value)
                    definitionLevels.add(3) // Key-value pair present
                    repetitionLevels.add(if (isFirst) 0 else 1) // 0 for first entry, 1 for repeated
                    isFirst = false
                }
            }
        }
        
        val keyColumn = DataColumn(
            field = keyField.toDataField(),
            data = keyValues.toTypedArray(),
            definitionLevels = definitionLevels.toIntArray(),
            repetitionLevels = repetitionLevels.toIntArray()
        )
        
        val valueColumn = DataColumn(
            field = valueField.toDataField(),
            data = valueValues.toTypedArray(),
            definitionLevels = definitionLevels.toIntArray(),
            repetitionLevels = repetitionLevels.toIntArray()
        )
        
        return listOf(keyColumn, valueColumn)
    }
    
    private fun serializeMapColumn(
        data: List<T>,
        field: DataField,
        property: KProperty1<T, *>
    ): DataColumn<*> {
        // This is called from the old path - should not be reached with new nested structure
        throw UnsupportedOperationException(
            "Map serialization should use serializeMapField with NestedField.Group"
        )
    }
    
    private fun getElementFieldFromNestedField(nestedField: NestedField.Group): NestedField.Primitive {
        // Navigate through the 3-level list structure to get the element field
        // Structure: list -> list.element (group) -> list.element.element (primitive)
        val listElement = nestedField.children.first() as NestedField.Group
        return listElement.children.first() as NestedField.Primitive
    }
    
    private fun createNestedFieldForList(
        field: DataField,
        property: KProperty1<T, *>
    ): NestedField.Group {
        // Determine element type from property's type arguments
        val elementType = property.returnType.arguments.firstOrNull()?.type
        val elementClassifier = elementType?.classifier as? KClass<*>
        
        // Create element field based on type
        val elementField = when (elementClassifier) {
            String::class -> NestedField.Primitive(
                name = "element",
                dataType = ParquetType.BYTE_ARRAY,
                logicalType = LogicalType.STRING
            )
            Int::class -> NestedField.Primitive(
                name = "element",
                dataType = ParquetType.INT32
            )
            Long::class -> NestedField.Primitive(
                name = "element",
                dataType = ParquetType.INT64
            )
            Double::class -> NestedField.Primitive(
                name = "element",
                dataType = ParquetType.DOUBLE
            )
            Float::class -> NestedField.Primitive(
                name = "element",
                dataType = ParquetType.FLOAT
            )
            Boolean::class -> NestedField.Primitive(
                name = "element",
                dataType = ParquetType.BOOLEAN
            )
            else -> NestedField.Primitive(
                name = "element",
                dataType = ParquetType.BYTE_ARRAY
            )
        }
        
        // Create list field structure
        return NestedField.createList(
            name = field.name,
            elementField = elementField,
            nullable = property.returnType.isMarkedNullable,
            elementNullable = elementType?.isMarkedNullable ?: false
        )
    }
    
    companion object {
        inline fun <reified T : Any> create(): ParquetSerializer<T> {
            return ParquetSerializer(T::class)
        }
    }
}

class ParquetDeserializer<T : Any>(
    private val clazz: KClass<T>
) {
    fun deserialize(rowGroup: RowGroup): List<T> {
        val constructor = clazz.constructors.first()
        val parameters = constructor.parameters
        
        // Check if any parameters are List types and need reconstruction
        val listColumns = mutableMapOf<String, List<List<*>?>>()
        
        // Check if any parameters are nested data classes (Structs) and need reconstruction
        val structColumns = mutableMapOf<String, List<Any?>>()
        
        parameters.forEach { param ->
            if (param.type.classifier == Map::class) {
                // Check if this is a Map type that needs reconstruction
                val mapObjects = reconstructMap(rowGroup, param.name!!, rowGroup.rowCount)
                structColumns[param.name!!] = mapObjects
            } else if (param.type.classifier == List::class) {
                // Find the column for this parameter
                val column = rowGroup.columns.find { it.field.name == param.name }
                if (column != null && column.repetitionLevels != null) {
                    // For list columns, the data array contains flattened values
                    // We need to get non-null values from the data array
                    val defLevels = column.definitionLevels?.toList() ?: emptyList()
                    val repLevels = column.repetitionLevels.toList()
                    
                    // Use definedData which already filters nulls, but if it's empty, 
                    // the values might be stored differently in the data array
                    val flattenedValues = if (column.definedData.isNotEmpty()) {
                        column.definedData.toList()
                    } else {
                        // For nullable lists, values might be in data array at specific positions
                        // corresponding to definition levels at max level
                        val maxDefLevel = defLevels.maxOrNull() ?: 0
                        val values = mutableListOf<Any>()
                        var valueIndex = 0
                        for (defLevel in defLevels) {
                            if (defLevel == maxDefLevel) {
                                // There should be a value here
                                if (valueIndex < column.size) {
                                    val value = column.get(valueIndex)
                                    if (value != null) {
                                        values.add(value)
                                    }
                                }
                                valueIndex++
                            }
                        }
                        values
                    }
                    
                    // Check if we need to convert ByteArray to String for List<String>
                    val elementType = param.type.arguments.firstOrNull()?.type?.classifier
                    val convertedValues = if (elementType == String::class && flattenedValues.firstOrNull() is ByteArray) {
                        flattenedValues.map { value ->
                            if (value is ByteArray) String(value, Charsets.UTF_8) else value
                        }
                    } else {
                        flattenedValues
                    }
                    
                    // Reconstruct the list data using the flattened values and levels
                    // Use the actual max definition level from the data
                    val actualMaxDefLevel = defLevels.maxOrNull() ?: 2
                    val reconstructed = com.molo17.parquetkt.data.NestedDataReconstructor.reconstructLists<Any>(
                        values = convertedValues,
                        definitionLevels = defLevels,
                        repetitionLevels = repLevels,
                        maxDefinitionLevel = actualMaxDefLevel,
                        nullable = param.type.isMarkedNullable
                    )
                    
                    listColumns[param.name!!] = reconstructed
                }
            } else {
                // Check if this is a nested data class (Struct)
                val classifier = param.type.classifier as? KClass<*>
                if (classifier != null && classifier.isData) {
                    // This is a struct - reconstruct from flattened columns
                    val structObjects = reconstructStruct(rowGroup, param.name!!, classifier, rowGroup.rowCount)
                    structColumns[param.name!!] = structObjects
                }
            }
        }
        
        // Build a map of column name to column for non-list, non-struct columns
        val nonListColumns = mutableMapOf<String, DataColumn<*>>()
        rowGroup.columns.forEach { column ->
            if (column.repetitionLevels == null) {
                nonListColumns[column.field.name] = column
            }
        }
        
        return (0 until rowGroup.rowCount).map { rowIndex ->
            val args = parameters.map { param ->
                // Check if this is a list column that was reconstructed
                if (listColumns.containsKey(param.name)) {
                    val reconstructedLists = listColumns[param.name!!]!!
                    if (rowIndex < reconstructedLists.size) {
                        reconstructedLists[rowIndex]
                    } else {
                        null
                    }
                } else if (structColumns.containsKey(param.name)) {
                    // Check if this is a struct column that was reconstructed
                    val reconstructedStructs = structColumns[param.name!!]!!
                    if (rowIndex < reconstructedStructs.size) {
                        reconstructedStructs[rowIndex]
                    } else {
                        null
                    }
                } else {
                    // Get value from non-list, non-struct column directly
                    val column = nonListColumns[param.name]
                    val value = column?.get(rowIndex)
                    when {
                        value == null -> if (param.type.isMarkedNullable) null 
                            else throw IllegalStateException("Non-nullable parameter ${param.name} is null")
                        // Convert ByteArray to String for String parameters
                        value is ByteArray && param.type.classifier == String::class -> 
                            String(value, Charsets.UTF_8)
                        else -> value
                    }
                }
            }.toTypedArray()
            
            constructor.call(*args)
        }
    }
    
    private fun reconstructStruct(
        rowGroup: RowGroup,
        structName: String,
        structClass: KClass<*>,
        rowCount: Int
    ): List<Any?> {
        // Get the constructor and parameters for the struct
        val constructor = structClass.constructors.first()
        val parameters = constructor.parameters
        
        // Find all columns that belong to this struct
        val structColumnMap = mutableMapOf<String, DataColumn<*>>()
        for (column in rowGroup.columns) {
            // Check if column name matches a struct field
            for (param in parameters) {
                if (column.field.name == param.name) {
                    structColumnMap[param.name!!] = column
                }
            }
        }
        
        // Reconstruct struct objects for each row
        return (0 until rowCount).map { rowIndex ->
            val args = parameters.map { param ->
                val column = structColumnMap[param.name]
                val value = column?.get(rowIndex)
                when {
                    value == null -> if (param.type.isMarkedNullable) null
                        else throw IllegalStateException("Non-nullable parameter ${param.name} is null in struct $structName")
                    // Convert ByteArray to String for String parameters
                    value is ByteArray && param.type.classifier == String::class ->
                        String(value, Charsets.UTF_8)
                    else -> value
                }
            }
            constructor.call(*args.toTypedArray())
        }
    }
    
    private fun reconstructMap(
        rowGroup: RowGroup,
        mapName: String,
        rowCount: Int
    ): List<Map<*, *>?> {
        // Find key and value columns for this map
        // Map columns are named "key" and "value"
        val keyColumn = rowGroup.columns.find { it.field.name == "key" }
        val valueColumn = rowGroup.columns.find { it.field.name == "value" }
        
        if (keyColumn == null || valueColumn == null) {
            // No map data found, return list of nulls
            return List(rowCount) { null }
        }
        
        // Get repetition and definition levels
        val repLevels = keyColumn.repetitionLevels?.toList() ?: emptyList()
        val defLevels = keyColumn.definitionLevels?.toList() ?: emptyList()
        
        if (defLevels.isEmpty()) {
            // No levels, return list of empty maps
            return List(rowCount) { emptyMap<Any?, Any?>() }
        }
        
        // Reconstruct maps using levels - similar to list reconstruction
        val maps = mutableListOf<Map<*, *>?>()
        var currentMap = mutableMapOf<Any?, Any?>()
        var valueIndex = 0
        var i = 0
        
        while (i < defLevels.size) {
            val defLevel = defLevels[i]
            val repLevel = repLevels.getOrElse(i) { 0 }
            
            when {
                defLevel == 0 -> {
                    // Null map
                    if (currentMap.isNotEmpty()) {
                        maps.add(currentMap.toMap())
                        currentMap = mutableMapOf()
                    }
                    maps.add(null)
                    i++
                }
                defLevel == 1 -> {
                    // Empty map
                    if (currentMap.isNotEmpty()) {
                        maps.add(currentMap.toMap())
                        currentMap = mutableMapOf()
                    }
                    maps.add(emptyMap<Any?, Any?>())
                    i++
                }
                defLevel == 3 -> {
                    // Key-value pair present
                    if (repLevel == 0 && currentMap.isNotEmpty()) {
                        // Start of new map, save previous
                        maps.add(currentMap.toMap())
                        currentMap = mutableMapOf()
                    }
                    
                    // Add key-value pair
                    if (valueIndex < keyColumn.size) {
                        var key = keyColumn.get(valueIndex)
                        var value = valueColumn.get(valueIndex)
                        
                        // Convert ByteArray to String if needed
                        if (key is ByteArray) key = String(key, Charsets.UTF_8)
                        if (value is ByteArray) value = String(value, Charsets.UTF_8)
                        
                        currentMap[key] = value
                        valueIndex++
                    }
                    i++
                }
                else -> {
                    // Unknown definition level, skip
                    i++
                }
            }
        }
        
        // Add the last map if not empty
        if (currentMap.isNotEmpty()) {
            maps.add(currentMap.toMap())
        }
        
        // Ensure we have the right number of maps
        while (maps.size < rowCount) {
            maps.add(null)
        }
        
        return maps
    }
    
    fun deserializeSequence(rowGroups: List<RowGroup>): Sequence<T> {
        return rowGroups.asSequence().flatMap { deserialize(it).asSequence() }
    }
    
    companion object {
        inline fun <reified T : Any> create(): ParquetDeserializer<T> {
            return ParquetDeserializer(T::class)
        }
    }
}
