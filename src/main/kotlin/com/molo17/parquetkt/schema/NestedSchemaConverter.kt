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


package com.molo17.parquetkt.schema

import com.molo17.parquetkt.thrift.FieldRepetitionType
import com.molo17.parquetkt.thrift.SchemaElement

/**
 * Converts between nested field structures and Parquet's flattened Thrift schema representation.
 * 
 * Parquet stores schemas as a flattened list of SchemaElements where groups (structs, lists, maps)
 * are represented with num_children to indicate how many following elements belong to that group.
 */
object NestedSchemaConverter {
    
    /**
     * Convert a list of NestedFields to Parquet's flattened SchemaElement list.
     * The first element is always the root schema element.
     */
    fun toThriftSchema(fields: List<NestedField>, schemaName: String = "schema"): List<SchemaElement> {
        val elements = mutableListOf<SchemaElement>()
        
        // Add root schema element
        elements.add(SchemaElement(
            name = schemaName,
            numChildren = fields.size,
            repetitionType = null,
            type = null
        ))
        
        // Add all fields
        fields.forEach { field ->
            addFieldToSchema(field, elements)
        }
        
        return elements
    }
    
    /**
     * Recursively add a field and its children to the schema element list.
     */
    private fun addFieldToSchema(field: NestedField, elements: MutableList<SchemaElement>) {
        when (field) {
            is NestedField.Primitive -> {
                elements.add(SchemaElement(
                    type = field.dataType,
                    name = field.name,
                    repetitionType = field.repetition.toThrift(),
                    numChildren = null
                ))
            }
            is NestedField.Group -> {
                // Add the group element
                elements.add(SchemaElement(
                    name = field.name,
                    numChildren = field.children.size,
                    repetitionType = field.repetition.toThrift(),
                    type = null,
                    convertedType = if (field.logicalType != LogicalType.NONE) 
                        field.logicalType.toConvertedType() else null
                ))
                
                // Recursively add children
                field.children.forEach { child ->
                    addFieldToSchema(child, elements)
                }
            }
        }
    }
    
    /**
     * Convert Parquet's flattened SchemaElement list back to nested field structure.
     */
    fun fromThriftSchema(elements: List<SchemaElement>): List<NestedField> {
        if (elements.isEmpty()) {
            throw IllegalArgumentException("Schema elements cannot be empty")
        }
        
        val root = elements[0]
        if (root.numChildren == null || root.numChildren == 0) {
            throw IllegalArgumentException("Root schema must have children")
        }
        
        val (fields, _) = parseFields(elements, 1, root.numChildren!!)
        return fields
    }
    
    /**
     * Parse fields from the flattened list starting at the given index.
     * Returns the parsed fields and the next index to continue from.
     */
    private fun parseFields(
        elements: List<SchemaElement>,
        startIndex: Int,
        count: Int
    ): Pair<List<NestedField>, Int> {
        val fields = mutableListOf<NestedField>()
        var currentIndex = startIndex
        
        repeat(count) {
            val element = elements[currentIndex]
            currentIndex++
            
            if (element.numChildren != null && element.numChildren!! > 0) {
                // This is a group
                val (children, nextIndex) = parseFields(elements, currentIndex, element.numChildren!!)
                currentIndex = nextIndex
                
                fields.add(NestedField.Group(
                    name = element.name,
                    children = children,
                    repetition = element.repetitionType?.fromThrift() ?: Repetition.REQUIRED,
                    logicalType = element.convertedType?.toLogicalType() ?: LogicalType.NONE
                ))
            } else {
                // This is a primitive field
                fields.add(NestedField.Primitive(
                    name = element.name,
                    dataType = element.type ?: ParquetType.BYTE_ARRAY,
                    repetition = element.repetitionType?.fromThrift() ?: Repetition.REQUIRED,
                    logicalType = element.convertedType?.toLogicalType() ?: LogicalType.NONE
                ))
            }
        }
        
        return fields to currentIndex
    }
    
    private fun Repetition.toThrift(): FieldRepetitionType {
        return when (this) {
            Repetition.REQUIRED -> FieldRepetitionType.REQUIRED
            Repetition.OPTIONAL -> FieldRepetitionType.OPTIONAL
            Repetition.REPEATED -> FieldRepetitionType.REPEATED
        }
    }
    
    private fun FieldRepetitionType.fromThrift(): Repetition {
        return when (this) {
            FieldRepetitionType.REQUIRED -> Repetition.REQUIRED
            FieldRepetitionType.OPTIONAL -> Repetition.OPTIONAL
            FieldRepetitionType.REPEATED -> Repetition.REPEATED
        }
    }
    
    private fun LogicalType.toConvertedType(): com.molo17.parquetkt.thrift.ConvertedType? {
        return when (this) {
            LogicalType.STRING -> com.molo17.parquetkt.thrift.ConvertedType.UTF8
            LogicalType.DATE -> com.molo17.parquetkt.thrift.ConvertedType.DATE
            LogicalType.TIMESTAMP_MILLIS -> com.molo17.parquetkt.thrift.ConvertedType.TIMESTAMP_MILLIS
            LogicalType.TIMESTAMP_MICROS -> com.molo17.parquetkt.thrift.ConvertedType.TIMESTAMP_MICROS
            LogicalType.DECIMAL -> com.molo17.parquetkt.thrift.ConvertedType.DECIMAL
            LogicalType.LIST -> com.molo17.parquetkt.thrift.ConvertedType.LIST
            LogicalType.MAP -> com.molo17.parquetkt.thrift.ConvertedType.MAP
            LogicalType.NONE -> null
            else -> null
        }
    }
    
    private fun com.molo17.parquetkt.thrift.ConvertedType.toLogicalType(): LogicalType {
        return when (this) {
            com.molo17.parquetkt.thrift.ConvertedType.UTF8 -> LogicalType.STRING
            com.molo17.parquetkt.thrift.ConvertedType.DATE -> LogicalType.DATE
            com.molo17.parquetkt.thrift.ConvertedType.TIMESTAMP_MILLIS -> LogicalType.TIMESTAMP_MILLIS
            com.molo17.parquetkt.thrift.ConvertedType.TIMESTAMP_MICROS -> LogicalType.TIMESTAMP_MICROS
            com.molo17.parquetkt.thrift.ConvertedType.DECIMAL -> LogicalType.DECIMAL
            com.molo17.parquetkt.thrift.ConvertedType.LIST -> LogicalType.LIST
            com.molo17.parquetkt.thrift.ConvertedType.MAP -> LogicalType.MAP
            else -> LogicalType.NONE
        }
    }
}
