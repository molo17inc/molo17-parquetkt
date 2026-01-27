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

import com.molo17.parquetkt.serialization.SchemaReflector
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class StructsAndMapsTest {
    
    data class Address(
        val street: String,
        val city: String
    )
    
    data class Person(
        val name: String,
        val age: Int,
        val address: Address
    )
    
    @Test
    fun `test schema reflection for struct`() {
        val schema = SchemaReflector.reflectSchema<Person>()
        
        assertNotNull(schema)
        println("Schema: $schema")
        
        // Schema should have nested fields
        assertEquals(true, schema.hasNestedStructure)
        
        // Should have 3 top-level fields: name, age, address
        assertEquals(3, schema.nestedFields?.size)
        
        // Leaf fields should be flattened: name, age, address.street, address.city
        assertEquals(4, schema.leafFields.size)
    }
    
    @Test
    fun `test schema reflection for map`() {
        data class Config(
            val name: String,
            val settings: Map<String, Int>
        )
        
        val schema = SchemaReflector.reflectSchema<Config>()
        
        assertNotNull(schema)
        println("Schema: $schema")
        
        // Schema should have nested fields
        assertEquals(true, schema.hasNestedStructure)
    }
    
    @Test
    fun `test struct serialization`() {
        val data = listOf(
            Person("Alice", 30, Address("123 Main St", "NYC")),
            Person("Bob", 25, Address("456 Oak Ave", "LA"))
        )
        
        val schema = SchemaReflector.reflectSchema<Person>()
        val serializer = com.molo17.parquetkt.serialization.ParquetSerializer.create<Person>()
        
        val rowGroup = serializer.serialize(data, schema)
        
        // Should have 4 columns: name, age, address.street, address.city
        assertEquals(4, rowGroup.columns.size)
        
        // Check column names
        val columnNames = rowGroup.columns.map { it.field.name }.toSet()
        assertEquals(setOf("name", "age", "street", "city"), columnNames)
        
        // Check values - handle both String and ByteArray
        val nameColumn = rowGroup.columns.find { it.field.name == "name" }!!
        val name0 = nameColumn.get(0)
        val name1 = nameColumn.get(1)
        assertEquals("Alice", if (name0 is ByteArray) String(name0) else name0)
        assertEquals("Bob", if (name1 is ByteArray) String(name1) else name1)
        
        val ageColumn = rowGroup.columns.find { it.field.name == "age" }!!
        assertEquals(30, ageColumn.get(0))
        assertEquals(25, ageColumn.get(1))
        
        val streetColumn = rowGroup.columns.find { it.field.name == "street" }!!
        val street0 = streetColumn.get(0)
        val street1 = streetColumn.get(1)
        assertEquals("123 Main St", if (street0 is ByteArray) String(street0) else street0)
        assertEquals("456 Oak Ave", if (street1 is ByteArray) String(street1) else street1)
        
        val cityColumn = rowGroup.columns.find { it.field.name == "city" }!!
        val city0 = cityColumn.get(0)
        val city1 = cityColumn.get(1)
        assertEquals("NYC", if (city0 is ByteArray) String(city0) else city0)
        assertEquals("LA", if (city1 is ByteArray) String(city1) else city1)
    }
    
    @Test
    fun `test struct end-to-end serialization and deserialization`() {
        val data = listOf(
            Person("Alice", 30, Address("123 Main St", "NYC")),
            Person("Bob", 25, Address("456 Oak Ave", "LA")),
            Person("Charlie", 35, Address("789 Pine Rd", "SF"))
        )
        
        val schema = SchemaReflector.reflectSchema<Person>()
        val serializer = com.molo17.parquetkt.serialization.ParquetSerializer.create<Person>()
        val deserializer = com.molo17.parquetkt.serialization.ParquetDeserializer.create<Person>()
        
        // Serialize
        val rowGroup = serializer.serialize(data, schema)
        
        // Deserialize
        val result = deserializer.deserialize(rowGroup)
        
        // Verify
        assertEquals(3, result.size)
        assertEquals(data, result)
    }
    
    @Test
    fun `test map serialization`() {
        data class Config(
            val name: String,
            val settings: Map<String, Int>
        )
        
        val data = listOf(
            Config("app1", mapOf("timeout" to 30, "retries" to 3)),
            Config("app2", mapOf("timeout" to 60))
        )
        
        val schema = SchemaReflector.reflectSchema<Config>()
        val serializer = com.molo17.parquetkt.serialization.ParquetSerializer.create<Config>()
        
        val rowGroup = serializer.serialize(data, schema)
        
        // Should have 3 columns: name, key, value
        assertEquals(3, rowGroup.columns.size)
        
        // Check column names
        val columnNames = rowGroup.columns.map { it.field.name }.toSet()
        assertEquals(setOf("name", "key", "value"), columnNames)
        
        // Check key column has correct values
        val keyColumn = rowGroup.columns.find { it.field.name == "key" }!!
        val key0 = keyColumn.get(0)
        val key1 = keyColumn.get(1)
        val key2 = keyColumn.get(2)
        
        // Keys should be flattened: ["timeout", "retries", "timeout"]
        val keys = listOf(key0, key1, key2).map { 
            if (it is ByteArray) String(it) else it 
        }
        assertEquals(setOf("timeout", "retries"), keys.toSet())
    }
    
    @Test
    fun `test map end-to-end serialization and deserialization`() {
        data class Config(
            val name: String,
            val settings: Map<String, Int>
        )
        
        val data = listOf(
            Config("app1", mapOf("timeout" to 30, "retries" to 3)),
            Config("app2", mapOf("timeout" to 60, "maxSize" to 100))
        )
        
        val schema = SchemaReflector.reflectSchema<Config>()
        val serializer = com.molo17.parquetkt.serialization.ParquetSerializer.create<Config>()
        val deserializer = com.molo17.parquetkt.serialization.ParquetDeserializer.create<Config>()
        
        // Serialize
        val rowGroup = serializer.serialize(data, schema)
        
        // Deserialize
        val result = deserializer.deserialize(rowGroup)
        
        // Verify
        assertEquals(2, result.size)
        assertEquals("app1", result[0].name)
        assertEquals("app2", result[1].name)
        
        // Check maps
        assertEquals(2, result[0].settings.size)
        assertEquals(30, result[0].settings["timeout"])
        assertEquals(3, result[0].settings["retries"])
        
        assertEquals(2, result[1].settings.size)
        assertEquals(60, result[1].settings["timeout"])
        assertEquals(100, result[1].settings["maxSize"])
    }
    
    @Test
    fun `test map with empty and null values`() {
        data class Config(
            val name: String,
            val settings: Map<String, Int>?
        )
        
        val data = listOf(
            Config("app1", mapOf("timeout" to 30)),
            Config("app2", emptyMap()),
            Config("app3", null)
        )
        
        val schema = SchemaReflector.reflectSchema<Config>()
        val serializer = com.molo17.parquetkt.serialization.ParquetSerializer.create<Config>()
        val deserializer = com.molo17.parquetkt.serialization.ParquetDeserializer.create<Config>()
        
        // Serialize
        val rowGroup = serializer.serialize(data, schema)
        
        // Deserialize
        val result = deserializer.deserialize(rowGroup)
        
        // Verify
        assertEquals(3, result.size)
        assertEquals(1, result[0].settings?.size)
        assertEquals(30, result[0].settings?.get("timeout"))
        assertEquals(0, result[1].settings?.size)
        assertEquals(null, result[2].settings)
    }
}
