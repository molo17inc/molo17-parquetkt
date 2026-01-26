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

class ParquetSchema(
    val fields: List<DataField>,
    val nestedFields: List<NestedField>? = null
) {
    val fieldCount: Int
        get() = fields.size
    
    val hasNestedStructure: Boolean
        get() = nestedFields != null
    
    /**
     * Get all leaf (primitive) fields, including those in nested structures.
     */
    val leafFields: List<DataField>
        get() = if (nestedFields != null) {
            nestedFields.flatMap { field ->
                when (field) {
                    is NestedField.Primitive -> listOf(field.toDataField())
                    is NestedField.Group -> field.getLeafFields().map { it.toDataField() }
                }
            }
        } else {
            fields
        }
    
    fun getField(name: String): DataField? {
        return fields.find { it.name == name }
    }
    
    fun getField(index: Int): DataField {
        return fields[index]
    }
    
    fun hasField(name: String): Boolean {
        return fields.any { it.name == name }
    }
    
    fun getFieldIndex(name: String): Int {
        return fields.indexOfFirst { it.name == name }
    }
    
    fun getNestedField(name: String): NestedField? {
        return nestedFields?.find { it.name == name }
    }
    
    override fun toString(): String {
        return buildString {
            appendLine("ParquetSchema {")
            if (hasNestedStructure) {
                nestedFields?.forEach { field ->
                    appendLine(formatNestedField(field, 1))
                }
            } else {
                fields.forEach { field ->
                    appendLine("  ${field.name}: ${field.dataType} (${field.repetition})")
                }
            }
            appendLine("}")
        }
    }
    
    private fun formatNestedField(field: NestedField, indent: Int): String {
        val prefix = "  ".repeat(indent)
        return when (field) {
            is NestedField.Primitive -> "$prefix${field.name}: ${field.dataType} (${field.repetition})"
            is NestedField.Group -> buildString {
                appendLine("${prefix}${field.name}: GROUP (${field.repetition}) {")
                field.children.forEach { child ->
                    appendLine(formatNestedField(child, indent + 1))
                }
                append("$prefix}")
            }
        }
    }
    
    companion object {
        fun create(vararg fields: DataField): ParquetSchema {
            return ParquetSchema(fields.toList())
        }
        
        fun create(fields: List<DataField>): ParquetSchema {
            return ParquetSchema(fields)
        }
        
        /**
         * Create a schema with nested field support.
         */
        fun createNested(vararg fields: NestedField): ParquetSchema {
            // Extract leaf fields for backward compatibility
            val leafFields = fields.flatMap { field ->
                when (field) {
                    is NestedField.Primitive -> listOf(field.toDataField())
                    is NestedField.Group -> field.getLeafFields().map { it.toDataField() }
                }
            }
            return ParquetSchema(leafFields, fields.toList())
        }
        
        fun createNested(fields: List<NestedField>): ParquetSchema {
            val leafFields = fields.flatMap { field ->
                when (field) {
                    is NestedField.Primitive -> listOf(field.toDataField())
                    is NestedField.Group -> field.getLeafFields().map { it.toDataField() }
                }
            }
            return ParquetSchema(leafFields, fields)
        }
    }
}
