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
    val fields: List<DataField>
) {
    val fieldCount: Int
        get() = fields.size
    
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
    
    override fun toString(): String {
        return buildString {
            appendLine("ParquetSchema {")
            fields.forEach { field ->
                appendLine("  ${field.name}: ${field.dataType} (${field.repetition})")
            }
            appendLine("}")
        }
    }
    
    companion object {
        fun create(vararg fields: DataField): ParquetSchema {
            return ParquetSchema(fields.toList())
        }
        
        fun create(fields: List<DataField>): ParquetSchema {
            return ParquetSchema(fields)
        }
    }
}
