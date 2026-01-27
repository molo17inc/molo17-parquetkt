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
        val columns = schema.fields.map { field ->
            serializeColumn(data, field)
        }
        return RowGroup(schema, columns)
    }
    
    private fun serializeColumn(data: List<T>, field: DataField): DataColumn<*> {
        val property = properties.find { it.name == field.name }
            ?: throw IllegalArgumentException("Property ${field.name} not found in class ${clazz.simpleName}")
        
        property.isAccessible = true
        
        // Check if property is a List type
        if (isListProperty(property)) {
            return serializeListColumn(data, field, property)
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
        
        parameters.forEach { param ->
            if (param.type.classifier == List::class) {
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
            }
        }
        
        // Build a map of column name to column for non-list columns
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
                } else {
                    // Get value from non-list column directly
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
    
    fun deserializeSequence(rowGroups: List<RowGroup>): Sequence<T> = sequence {
        rowGroups.forEach { rowGroup ->
            yieldAll(deserialize(rowGroup))
        }
    }
    
    companion object {
        inline fun <reified T : Any> create(): ParquetDeserializer<T> {
            return ParquetDeserializer(T::class)
        }
    }
}
