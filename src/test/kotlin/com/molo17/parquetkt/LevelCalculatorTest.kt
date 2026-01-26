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

import com.molo17.parquetkt.encoding.LevelCalculator
import com.molo17.parquetkt.schema.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class LevelCalculatorTest {
    
    @Test
    fun `test max definition level for required primitive`() {
        val field = NestedField.Primitive(
            name = "id",
            dataType = ParquetType.INT64,
            repetition = Repetition.REQUIRED
        )
        
        val maxDefLevel = LevelCalculator.calculateMaxDefinitionLevel(field)
        assertEquals(0, maxDefLevel)
    }
    
    @Test
    fun `test max definition level for optional primitive`() {
        val field = NestedField.Primitive(
            name = "name",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING,
            repetition = Repetition.OPTIONAL
        )
        
        val maxDefLevel = LevelCalculator.calculateMaxDefinitionLevel(field)
        assertEquals(1, maxDefLevel)
    }
    
    @Test
    fun `test max definition level for required list`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        
        val listField = NestedField.createList(
            name = "tags",
            elementField = elementField,
            nullable = false,
            elementNullable = true
        )
        
        val maxDefLevel = LevelCalculator.calculateMaxDefinitionLevel(listField)
        // Required list + repeated group + optional element = 2
        assertEquals(2, maxDefLevel)
    }
    
    @Test
    fun `test max definition level for optional list`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.INT32
        )
        
        val listField = NestedField.createList(
            name = "numbers",
            elementField = elementField,
            nullable = true,
            elementNullable = false
        )
        
        val maxDefLevel = LevelCalculator.calculateMaxDefinitionLevel(listField)
        // Optional list + repeated group + required element = 3
        assertEquals(3, maxDefLevel)
    }
    
    @Test
    fun `test max repetition level for required primitive`() {
        val field = NestedField.Primitive(
            name = "id",
            dataType = ParquetType.INT64,
            repetition = Repetition.REQUIRED
        )
        
        val maxRepLevel = LevelCalculator.calculateMaxRepetitionLevel(field)
        assertEquals(0, maxRepLevel)
    }
    
    @Test
    fun `test max repetition level for list`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.INT32
        )
        
        val listField = NestedField.createList(
            name = "numbers",
            elementField = elementField,
            nullable = false
        )
        
        val maxRepLevel = LevelCalculator.calculateMaxRepetitionLevel(listField)
        // One repeated group in the list structure
        assertEquals(1, maxRepLevel)
    }
    
    @Test
    fun `test flatten simple list of strings`() {
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
            listOf("tag1", "tag2", "tag3"),
            listOf("tag4")
        )
        
        val (values, defLevels, repLevels) = LevelCalculator.flattenNestedData<String>(
            data = data,
            field = listField
        )
        
        assertEquals(listOf("tag1", "tag2", "tag3", "tag4"), values)
        assertEquals(4, defLevels.size)
        assertEquals(4, repLevels.size)
        
        // First element of first list: rep=0 (new record)
        assertEquals(0, repLevels[0])
        // Second element of first list: rep=1 (continuing list)
        assertEquals(1, repLevels[1])
        // Third element of first list: rep=1 (continuing list)
        assertEquals(1, repLevels[2])
        // First element of second list: rep=0 (new record/new list)
        assertEquals(0, repLevels[3])
    }
    
    @Test
    fun `test flatten list with null elements`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.INT32
        )
        
        val listField = NestedField.createList(
            name = "numbers",
            elementField = elementField,
            nullable = false,
            elementNullable = true
        )
        
        val data = listOf(
            listOf(1, null, 3)
        )
        
        val (values, defLevels, repLevels) = LevelCalculator.flattenNestedData<Int>(
            data = data,
            field = listField
        )
        
        // Only non-null values are in the values list
        assertEquals(listOf(1, 3), values)
        // But we have 3 definition levels (one for each element including null)
        assertEquals(3, defLevels.size)
        assertEquals(3, repLevels.size)
    }
    
    @Test
    fun `test flatten empty list`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.INT32
        )
        
        val listField = NestedField.createList(
            name = "numbers",
            elementField = elementField,
            nullable = false
        )
        
        val data = listOf(
            emptyList<Int>()
        )
        
        val (values, defLevels, repLevels) = LevelCalculator.flattenNestedData<Int>(
            data = data,
            field = listField
        )
        
        // Empty list: no values
        assertEquals(0, values.size)
        // But one definition level indicating empty list
        assertEquals(1, defLevels.size)
        assertEquals(1, repLevels.size)
    }
    
    @Test
    fun `test flatten null list`() {
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.INT32
        )
        
        val listField = NestedField.createList(
            name = "numbers",
            elementField = elementField,
            nullable = true
        )
        
        val data = listOf<List<Int>?>(
            null
        )
        
        val (values, defLevels, repLevels) = LevelCalculator.flattenNestedData<Int>(
            data = data,
            field = listField
        )
        
        // Null list: no values
        assertEquals(0, values.size)
        // One definition level indicating null
        assertEquals(1, defLevels.size)
        assertEquals(1, repLevels.size)
    }
}
