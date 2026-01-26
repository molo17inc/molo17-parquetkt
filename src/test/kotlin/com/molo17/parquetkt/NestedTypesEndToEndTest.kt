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

import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.data.NestedDataColumn
import com.molo17.parquetkt.schema.*
import com.molo17.parquetkt.serialization.NestedSchemaReflector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals

data class UserWithTags(
    val id: Long,
    val name: String,
    val tags: List<String>
)

data class UserWithScores(
    val id: Long,
    val name: String,
    val scores: List<Int>
)

class NestedTypesEndToEndTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test schema reflection for data class with list`() {
        // Verify that schema reflection can detect List<String> properties
        val schema = NestedSchemaReflector.reflectNestedSchema<UserWithTags>()
        
        assertEquals(true, schema.hasNestedStructure)
        assertEquals(3, schema.nestedFields?.size)
        
        val tagsField = schema.getNestedField("tags")
        assert(tagsField is NestedField.Group)
        assertEquals(LogicalType.LIST, (tagsField as NestedField.Group).logicalType)
    }
    
    @Test
    fun `test flatten and reconstruct list data manually`() {
        // Test the core flatten/reconstruct logic works
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
            listOf("vip", "premium"),
            listOf("standard"),
            listOf("trial", "new")
        )
        
        // Flatten
        val column = NestedDataColumn.createForList<String>(listField, data)
        
        // Verify flattened data
        assertEquals(5, column.valueCount) // 5 total strings
        assertEquals(listOf("vip", "premium", "standard", "trial", "new"), column.values)
        
        // Reconstruct
        val reconstructed = com.molo17.parquetkt.data.NestedDataReconstructor.reconstructLists(
            values = column.values,
            definitionLevels = column.definitionLevels,
            repetitionLevels = column.repetitionLevels,
            maxDefinitionLevel = column.maxDefinitionLevel,
            nullable = false
        )
        
        assertEquals(data, reconstructed)
    }
    
    @Test
    fun `test write and read simple list manually with low-level API`() {
        // This test demonstrates what needs to be integrated
        // For now, it just verifies the components work
        
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        
        val listField = NestedField.createList(
            name = "tags",
            elementField = elementField,
            nullable = false
        )
        
        val data = listOf(
            listOf("tag1", "tag2"),
            listOf("tag3")
        )
        
        // Create column
        val column = NestedDataColumn.createForList<String>(listField, data)
        
        // Verify we have the right structure
        assertEquals(3, column.valueCount)
        assertEquals(3, column.definitionLevels.size)
        assertEquals(3, column.repetitionLevels.size)
        
        // TODO: Once encoder integration is complete, write to actual file
        // val file = File(tempDir, "test_list.parquet")
        // ParquetFile.write(file.absolutePath, ..., column)
        
        // TODO: Once decoder integration is complete, read from file
        // val readColumn = ParquetFile.read(file.absolutePath)
        
        // For now, just verify reconstruction works
        val reconstructed = com.molo17.parquetkt.data.NestedDataReconstructor.reconstructLists<String>(
            values = column.values,
            definitionLevels = column.definitionLevels,
            repetitionLevels = column.repetitionLevels,
            maxDefinitionLevel = column.maxDefinitionLevel,
            nullable = false
        )
        
        assertEquals(data, reconstructed)
    }
    
    @Test
    fun `test nested schema conversion to thrift`() {
        // Verify nested schemas can be serialized to Thrift format
        val idField = NestedField.Primitive("id", ParquetType.INT64)
        val nameField = NestedField.Primitive("name", ParquetType.BYTE_ARRAY, LogicalType.STRING)
        
        val elementField = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        val tagsField = NestedField.createList("tags", elementField, nullable = false)
        
        val fields = listOf(idField, nameField, tagsField)
        
        // Convert to Thrift
        val thriftSchema = NestedSchemaConverter.toThriftSchema(fields, "UserWithTags")
        
        // Verify structure
        assert(thriftSchema.isNotEmpty())
        assertEquals("UserWithTags", thriftSchema[0].name)
        assertEquals(3, thriftSchema[0].numChildren) // id, name, tags
        
        // The tags field should be a group with LIST logical type
        val tagsElement = thriftSchema.find { it.name == "tags" }
        assert(tagsElement != null)
        assert(tagsElement!!.numChildren != null && tagsElement.numChildren!! > 0)
    }
}
