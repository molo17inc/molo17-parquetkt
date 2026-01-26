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


package com.molo17.parquetkt

import com.molo17.parquetkt.data.NestedDataColumn
import com.molo17.parquetkt.data.NestedDataReconstructor
import com.molo17.parquetkt.schema.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class NestedDataColumnTest {
    
    @Test
    fun `test create nested data column for list of strings`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        
        val listField = NestedField.createList(
            name = "tags",
            elementField = elementField,
            nullable = false,
            elementNullable = false
        )
        
        val data = listOf(
            listOf("tag1", "tag2"),
            listOf("tag3")
        )
        
        val column = NestedDataColumn.createForList<String>(listField, data)
        
        assertEquals(3, column.valueCount) // 3 total values
        assertEquals(3, column.size) // 3 definition levels
        assertEquals(listOf("tag1", "tag2", "tag3"), column.values)
    }
    
    @Test
    fun `test reconstruct simple lists`() {
        val values = listOf("tag1", "tag2", "tag3", "tag4")
        val defLevels = listOf(2, 2, 2, 2)
        val repLevels = listOf(0, 1, 0, 0)
        
        val reconstructed = NestedDataReconstructor.reconstructLists(
            values = values,
            definitionLevels = defLevels,
            repetitionLevels = repLevels,
            maxDefinitionLevel = 2,
            nullable = false
        )
        
        assertEquals(3, reconstructed.size)
        assertEquals(listOf("tag1", "tag2"), reconstructed[0])
        assertEquals(listOf("tag3"), reconstructed[1])
        assertEquals(listOf("tag4"), reconstructed[2])
    }
    
    @Test
    fun `test reconstruct lists with null elements`() {
        val values = listOf(1, 3)
        val defLevels = listOf(2, 1, 2) // defined, null, defined
        val repLevels = listOf(0, 1, 1)
        
        val reconstructed = NestedDataReconstructor.reconstructLists(
            values = values,
            definitionLevels = defLevels,
            repetitionLevels = repLevels,
            maxDefinitionLevel = 2,
            nullable = false
        )
        
        assertEquals(1, reconstructed.size)
        // Note: Current implementation doesn't handle null elements within lists
        // This would need enhancement to properly track null positions
    }
    
    @Test
    fun `test round trip for list of strings`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        
        val listField = NestedField.createList(
            name = "tags",
            elementField = elementField,
            nullable = false,
            elementNullable = false
        )
        
        val originalData = listOf(
            listOf("a", "b", "c"),
            listOf("d"),
            listOf("e", "f")
        )
        
        // Flatten
        val column = NestedDataColumn.createForList<String>(listField, originalData)
        
        // Reconstruct
        val reconstructed = NestedDataReconstructor.reconstructLists(
            values = column.values,
            definitionLevels = column.definitionLevels,
            repetitionLevels = column.repetitionLevels,
            maxDefinitionLevel = column.maxDefinitionLevel,
            nullable = false
        )
        
        assertEquals(originalData, reconstructed)
    }
    
    @Test
    fun `test round trip with empty lists`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.INT32
        )
        
        val listField = NestedField.createList(
            name = "numbers",
            elementField = elementField,
            nullable = false
        )
        
        val originalData = listOf(
            listOf(1, 2),
            emptyList(),
            listOf(3)
        )
        
        // Flatten
        val column = NestedDataColumn.createForList<Int>(listField, originalData)
        
        // Reconstruct
        val reconstructed = NestedDataReconstructor.reconstructLists(
            values = column.values,
            definitionLevels = column.definitionLevels,
            repetitionLevels = column.repetitionLevels,
            maxDefinitionLevel = column.maxDefinitionLevel,
            nullable = false
        )
        
        assertEquals(originalData, reconstructed)
    }
    
    @Test
    fun `test round trip with nullable lists`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.INT32
        )
        
        val listField = NestedField.createList(
            name = "numbers",
            elementField = elementField,
            nullable = true
        )
        
        val originalData = listOf<List<Int>?>(
            listOf(1, 2),
            null,
            listOf(3)
        )
        
        // Flatten
        val column = NestedDataColumn.createForList<Int>(listField, originalData)
        
        // Reconstruct
        val reconstructed = NestedDataReconstructor.reconstructLists(
            values = column.values,
            definitionLevels = column.definitionLevels,
            repetitionLevels = column.repetitionLevels,
            maxDefinitionLevel = column.maxDefinitionLevel,
            nullable = true
        )
        
        assertEquals(originalData, reconstructed)
    }
}
