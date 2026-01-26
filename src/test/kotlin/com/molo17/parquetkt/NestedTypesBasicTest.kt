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

import com.molo17.parquetkt.schema.*
import com.molo17.parquetkt.serialization.NestedSchemaReflector
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

data class Address(
    val street: String,
    val city: String,
    val zipCode: String
)

data class PersonWithAddress(
    val id: Long,
    val name: String,
    val address: Address
)

data class PersonWithTags(
    val id: Long,
    val name: String,
    val tags: List<String>
)

data class PersonWithMetadata(
    val id: Long,
    val name: String,
    val metadata: Map<String, String>
)

class NestedTypesBasicTest {
    
    @Test
    fun `test create list field with 3-level structure`() {
        val stringElement = NestedField.Primitive(
            name = "element",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        
        val listField = NestedField.createList(
            name = "tags",
            elementField = stringElement,
            nullable = false,
            elementNullable = true
        )
        
        assertTrue(listField is NestedField.Group)
        assertEquals("tags", listField.name)
        assertEquals(LogicalType.LIST, listField.logicalType)
        assertEquals(1, listField.children.size)
        
        val listGroup = listField.children[0] as NestedField.Group
        assertEquals("list", listGroup.name)
        assertEquals(Repetition.REPEATED, listGroup.repetition)
    }
    
    @Test
    fun `test create map field structure`() {
        val keyField = NestedField.Primitive(
            name = "key",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        
        val valueField = NestedField.Primitive(
            name = "value",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        
        val mapField = NestedField.createMap(
            name = "metadata",
            keyField = keyField,
            valueField = valueField,
            nullable = false,
            valueNullable = true
        )
        
        assertTrue(mapField is NestedField.Group)
        assertEquals("metadata", mapField.name)
        assertEquals(LogicalType.MAP, mapField.logicalType)
        assertEquals(1, mapField.children.size)
        
        val keyValueGroup = mapField.children[0] as NestedField.Group
        assertEquals("key_value", keyValueGroup.name)
        assertEquals(Repetition.REPEATED, keyValueGroup.repetition)
        assertEquals(2, keyValueGroup.children.size)
    }
    
    @Test
    fun `test create struct field`() {
        val fields = listOf(
            NestedField.Primitive("street", ParquetType.BYTE_ARRAY, LogicalType.STRING),
            NestedField.Primitive("city", ParquetType.BYTE_ARRAY, LogicalType.STRING),
            NestedField.Primitive("zipCode", ParquetType.BYTE_ARRAY, LogicalType.STRING)
        )
        
        val structField = NestedField.createStruct(
            name = "address",
            fields = fields,
            nullable = false
        )
        
        assertTrue(structField is NestedField.Group)
        assertEquals("address", structField.name)
        assertEquals(3, structField.children.size)
    }
    
    @Test
    fun `test nested schema creation`() {
        val idField = NestedField.Primitive(
            name = "id",
            dataType = ParquetType.INT64
        )
        
        val nameField = NestedField.Primitive(
            name = "name",
            dataType = ParquetType.BYTE_ARRAY,
            logicalType = LogicalType.STRING
        )
        
        val schema = ParquetSchema.createNested(idField, nameField)
        
        assertNotNull(schema)
        assertTrue(schema.hasNestedStructure)
        assertEquals(2, schema.nestedFields?.size)
        assertEquals(2, schema.leafFields.size)
    }
    
    @Test
    fun `test schema converter to thrift`() {
        val fields = listOf(
            NestedField.Primitive("id", ParquetType.INT64),
            NestedField.Primitive("name", ParquetType.BYTE_ARRAY, LogicalType.STRING)
        )
        
        val thriftSchema = NestedSchemaConverter.toThriftSchema(fields, "test_schema")
        
        assertNotNull(thriftSchema)
        assertEquals(3, thriftSchema.size) // root + 2 fields
        assertEquals("test_schema", thriftSchema[0].name)
        assertEquals(2, thriftSchema[0].numChildren)
    }
    
    @Test
    fun `test schema converter from thrift`() {
        val fields = listOf(
            NestedField.Primitive("id", ParquetType.INT64),
            NestedField.Primitive("name", ParquetType.BYTE_ARRAY, LogicalType.STRING)
        )
        
        val thriftSchema = NestedSchemaConverter.toThriftSchema(fields)
        val reconstructed = NestedSchemaConverter.fromThriftSchema(thriftSchema)
        
        assertEquals(2, reconstructed.size)
        assertEquals("id", reconstructed[0].name)
        assertEquals("name", reconstructed[1].name)
    }
    
    @Test
    fun `test reflect nested data class with struct`() {
        val schema = NestedSchemaReflector.reflectNestedSchema<PersonWithAddress>()
        
        assertNotNull(schema)
        assertTrue(schema.hasNestedStructure)
        assertEquals(3, schema.nestedFields?.size)
        
        val addressField = schema.getNestedField("address")
        assertNotNull(addressField)
        assertTrue(addressField is NestedField.Group)
        assertEquals(3, (addressField as NestedField.Group).children.size)
    }
    
    @Test
    fun `test reflect data class with list`() {
        val schema = NestedSchemaReflector.reflectNestedSchema<PersonWithTags>()
        
        assertNotNull(schema)
        assertTrue(schema.hasNestedStructure)
        
        val tagsField = schema.getNestedField("tags")
        assertNotNull(tagsField)
        assertTrue(tagsField is NestedField.Group)
        assertEquals(LogicalType.LIST, (tagsField as NestedField.Group).logicalType)
    }
    
    @Test
    fun `test reflect data class with map`() {
        val schema = NestedSchemaReflector.reflectNestedSchema<PersonWithMetadata>()
        
        assertNotNull(schema)
        assertTrue(schema.hasNestedStructure)
        
        val metadataField = schema.getNestedField("metadata")
        assertNotNull(metadataField)
        assertTrue(metadataField is NestedField.Group)
        assertEquals(LogicalType.MAP, (metadataField as NestedField.Group).logicalType)
    }
}
