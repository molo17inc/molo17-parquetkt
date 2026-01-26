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


package com.molo17.parquetkt.data

import com.molo17.parquetkt.schema.ParquetSchema

class RowGroup(
    val schema: ParquetSchema,
    val columns: List<DataColumn<*>>
) {
    val rowCount: Int
        get() = columns.firstOrNull()?.size ?: 0
    
    val columnCount: Int
        get() = columns.size
    
    init {
        require(columns.size == schema.fieldCount) {
            "Number of columns (${columns.size}) must match schema field count (${schema.fieldCount})"
        }
        
        val firstSize = columns.firstOrNull()?.size ?: 0
        require(columns.all { it.size == firstSize }) {
            "All columns must have the same number of rows"
        }
    }
    
    fun getColumn(index: Int): DataColumn<*> {
        return columns[index]
    }
    
    fun getColumn(name: String): DataColumn<*>? {
        val index = schema.getFieldIndex(name)
        return if (index >= 0) columns[index] else null
    }
    
    fun getRow(index: Int): Map<String, Any?> {
        require(index in 0 until rowCount) {
            "Row index $index out of bounds [0, $rowCount)"
        }
        
        return schema.fields.mapIndexed { colIndex, field ->
            field.name to columns[colIndex].get(index)
        }.toMap()
    }
    
    override fun toString(): String {
        return "RowGroup(rows=$rowCount, columns=$columnCount)"
    }
}
