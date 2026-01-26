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
import com.molo17.parquetkt.serialization.SchemaReflector
import com.molo17.parquetkt.serialization.ParquetDeserializer
import com.molo17.parquetkt.serialization.ParquetSerializer
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals

class NestedTypesSerializationTest {
    
    @TempDir
    lateinit var tempDir: File
    
    data class UserWithTags(
        val id: Long,
        val name: String,
        val tags: List<String>
    )
    
    data class ProductWithCategories(
        val id: Int,
        val name: String,
        val categories: List<String>,
        val prices: List<Double>
    )
    
    data class PersonWithNullableList(
        val id: Int,
        val name: String,
        val hobbies: List<String>?
    )
    
    @Test
    fun `test serialize and deserialize data class with List of strings`() {
        val data = listOf(
            UserWithTags(1, "Alice", listOf("vip", "premium")),
            UserWithTags(2, "Bob", listOf("standard")),
            UserWithTags(3, "Charlie", listOf("trial", "new", "active"))
        )
        
        val schema = SchemaReflector.reflectSchema<UserWithTags>()
        val serializer = ParquetSerializer.create<UserWithTags>()
        val rowGroup = serializer.serialize(data, schema)
        
        val deserializer = ParquetDeserializer.create<UserWithTags>()
        val result = deserializer.deserialize(rowGroup)
        
        assertEquals(data, result)
    }
    
    @Test
    fun `test write and read data class with List of strings to actual file`() {
        val data = listOf(
            UserWithTags(1, "Alice", listOf("vip", "premium")),
            UserWithTags(2, "Bob", listOf("standard")),
            UserWithTags(3, "Charlie", listOf("trial", "new"))
        )
        
        val file = File(tempDir, "users_with_lists.parquet")
        
        // Write
        val schema = SchemaReflector.reflectSchema<UserWithTags>()
        val serializer = ParquetSerializer.create<UserWithTags>()
        val rowGroup = serializer.serialize(data, schema)
        ParquetFile.write(file.absolutePath, schema, listOf(rowGroup))
        
        // Read
        val rowGroups = ParquetFile.read(file)
        val deserializer = ParquetDeserializer.create<UserWithTags>()
        val readData = deserializer.deserializeSequence(rowGroups).toList()
        
        // Verify
        assertEquals(data, readData)
    }
    
    @Test
    fun `test serialize data class with empty lists`() {
        val data = listOf(
            UserWithTags(1, "Alice", emptyList()),
            UserWithTags(2, "Bob", listOf("tag1")),
            UserWithTags(3, "Charlie", emptyList())
        )
        
        val schema = SchemaReflector.reflectSchema<UserWithTags>()
        val serializer = ParquetSerializer.create<UserWithTags>()
        val rowGroup = serializer.serialize(data, schema)
        
        val deserializer = ParquetDeserializer.create<UserWithTags>()
        val result = deserializer.deserialize(rowGroup)
        
        assertEquals(data, result)
    }
    
    @Test
    fun `test serialize data class with multiple list properties`() {
        val data = listOf(
            ProductWithCategories(1, "Laptop", listOf("electronics", "computers"), listOf(999.99, 1099.99)),
            ProductWithCategories(2, "Book", listOf("education", "literature"), listOf(19.99)),
            ProductWithCategories(3, "Phone", listOf("electronics", "mobile", "gadgets"), listOf(699.99, 799.99, 899.99))
        )
        
        val schema = SchemaReflector.reflectSchema<ProductWithCategories>()
        val serializer = ParquetSerializer.create<ProductWithCategories>()
        val rowGroup = serializer.serialize(data, schema)
        
        val deserializer = ParquetDeserializer.create<ProductWithCategories>()
        val result = deserializer.deserialize(rowGroup)
        
        assertEquals(data, result)
    }
    
    @Test
    fun `test write and read data class with multiple list properties to file`() {
        val data = listOf(
            ProductWithCategories(1, "Laptop", listOf("electronics", "computers"), listOf(999.99, 1099.99)),
            ProductWithCategories(2, "Book", listOf("education"), listOf(19.99)),
            ProductWithCategories(3, "Phone", listOf("electronics", "mobile"), listOf(699.99, 799.99))
        )
        
        val file = File(tempDir, "products_with_lists.parquet")
        
        // Write
        val schema = SchemaReflector.reflectSchema<ProductWithCategories>()
        val serializer = ParquetSerializer.create<ProductWithCategories>()
        val rowGroup = serializer.serialize(data, schema)
        ParquetFile.write(file.absolutePath, schema, listOf(rowGroup))
        
        // Read
        val rowGroups = ParquetFile.read(file)
        val deserializer = ParquetDeserializer.create<ProductWithCategories>()
        val readData = deserializer.deserializeSequence(rowGroups).toList()
        
        // Verify
        assertEquals(data, readData)
    }
    
    @Test
    fun `test serialize data class with nullable list property`() {
        val data = listOf(
            PersonWithNullableList(1, "Alice", listOf("reading", "coding")),
            PersonWithNullableList(2, "Bob", null),
            PersonWithNullableList(3, "Charlie", listOf("gaming"))
        )
        
        val schema = SchemaReflector.reflectSchema<PersonWithNullableList>()
        val serializer = ParquetSerializer.create<PersonWithNullableList>()
        val rowGroup = serializer.serialize(data, schema)
        
        val deserializer = ParquetDeserializer.create<PersonWithNullableList>()
        val result = deserializer.deserialize(rowGroup)
        
        assertEquals(data, result)
    }
    
    @Test
    fun `test write and read data class with nullable list to file`() {
        val data = listOf(
            PersonWithNullableList(1, "Alice", listOf("reading", "coding")),
            PersonWithNullableList(2, "Bob", null),
            PersonWithNullableList(3, "Charlie", emptyList())
        )
        
        val file = File(tempDir, "persons_with_nullable_lists.parquet")
        
        // Write
        val schema = SchemaReflector.reflectSchema<PersonWithNullableList>()
        val serializer = ParquetSerializer.create<PersonWithNullableList>()
        val rowGroup = serializer.serialize(data, schema)
        ParquetFile.write(file.absolutePath, schema, listOf(rowGroup))
        
        // Read
        val rowGroups = ParquetFile.read(file)
        val deserializer = ParquetDeserializer.create<PersonWithNullableList>()
        val readData = deserializer.deserializeSequence(rowGroups).toList()
        
        // Verify
        assertEquals(data, readData)
    }
    
    @Test
    fun `test serialize data class with lists of different primitive types`() {
        data class DataWithVariousLists(
            val id: Int,
            val strings: List<String>,
            val ints: List<Int>,
            val longs: List<Long>,
            val doubles: List<Double>,
            val booleans: List<Boolean>
        )
        
        val data = listOf(
            DataWithVariousLists(
                1,
                listOf("a", "b"),
                listOf(1, 2, 3),
                listOf(100L, 200L),
                listOf(1.5, 2.5, 3.5),
                listOf(true, false, true)
            ),
            DataWithVariousLists(
                2,
                listOf("x"),
                listOf(10),
                listOf(1000L),
                listOf(10.5),
                listOf(false)
            )
        )
        
        val schema = SchemaReflector.reflectSchema<DataWithVariousLists>()
        val serializer = ParquetSerializer.create<DataWithVariousLists>()
        val rowGroup = serializer.serialize(data, schema)
        
        val deserializer = ParquetDeserializer.create<DataWithVariousLists>()
        val result = deserializer.deserialize(rowGroup)
        
        assertEquals(data, result)
    }
    
    @Test
    fun `test write and read large dataset with lists to file`() {
        val data = (1..100).map { i ->
            UserWithTags(
                id = i.toLong(),
                name = "User$i",
                tags = (1..(i % 5 + 1)).map { "tag$it" }
            )
        }
        
        val file = File(tempDir, "large_users_with_lists.parquet")
        
        // Write
        val schema = SchemaReflector.reflectSchema<UserWithTags>()
        val serializer = ParquetSerializer.create<UserWithTags>()
        val rowGroup = serializer.serialize(data, schema)
        ParquetFile.write(file.absolutePath, schema, listOf(rowGroup))
        
        // Read
        val rowGroups = ParquetFile.read(file)
        val deserializer = ParquetDeserializer.create<UserWithTags>()
        val readData = deserializer.deserializeSequence(rowGroups).toList()
        
        // Verify
        assertEquals(data.size, readData.size)
        assertEquals(data, readData)
    }
}
