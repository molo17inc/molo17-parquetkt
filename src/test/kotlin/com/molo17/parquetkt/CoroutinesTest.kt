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

import com.molo17.parquetkt.core.ParquetFileAsync
import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

data class AsyncPerson(
    val id: Long,
    val name: String,
    val age: Int,
    val salary: Double
)

class CoroutinesTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test async write and read with objects`() = runBlocking {
        val people = listOf(
            AsyncPerson(1L, "Alice", 30, 75000.0),
            AsyncPerson(2L, "Bob", 25, 65000.0),
            AsyncPerson(3L, "Charlie", 35, 85000.0)
        )
        
        val file = File(tempDir, "async_people.parquet")
        
        // Write asynchronously
        ParquetFileAsync.writeObjects(file, people, CompressionCodec.SNAPPY)
        
        // Read asynchronously
        val readPeople = ParquetFileAsync.readObjects<AsyncPerson>(file)
        
        assertEquals(3, readPeople.size)
        assertEquals(people, readPeople)
    }
    
    @Test
    fun `test async write and read with row groups`() = runBlocking {
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.string("name"),
            DataField.int32("age")
        )
        
        val idColumn = DataColumn.createRequired(
            DataField.int64("id"),
            listOf(1L, 2L, 3L)
        )
        
        val nameColumn = DataColumn.createRequired(
            DataField.string("name"),
            listOf("Alice", "Bob", "Charlie")
        )
        
        val ageColumn = DataColumn.createRequired(
            DataField.int32("age"),
            listOf(30, 25, 35)
        )
        
        val rowGroup = RowGroup(schema, listOf(idColumn, nameColumn, ageColumn))
        val file = File(tempDir, "async_rowgroups.parquet")
        
        // Write asynchronously
        ParquetFileAsync.write(file, schema, listOf(rowGroup))
        
        // Read asynchronously
        val readRowGroups = ParquetFileAsync.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(3, readRowGroups[0].rowCount)
    }
    
    @Test
    fun `test async read as Flow`() = runBlocking {
        val people = listOf(
            AsyncPerson(1L, "Alice", 30, 75000.0),
            AsyncPerson(2L, "Bob", 25, 65000.0),
            AsyncPerson(3L, "Charlie", 35, 85000.0)
        )
        
        val file = File(tempDir, "async_flow.parquet")
        
        // Write asynchronously
        ParquetFileAsync.writeObjects(file, people)
        
        // Read as Flow
        val readPeople = ParquetFileAsync.readObjectsAsFlow<AsyncPerson>(file).toList()
        
        assertEquals(3, readPeople.size)
        assertEquals(people, readPeople)
    }
    
    @Test
    fun `test async read schema`() = runBlocking {
        val people = listOf(
            AsyncPerson(1L, "Alice", 30, 75000.0)
        )
        
        val file = File(tempDir, "async_schema.parquet")
        
        // Write asynchronously
        ParquetFileAsync.writeObjects(file, people)
        
        // Read schema asynchronously
        val schema = ParquetFileAsync.readSchema(file)
        
        assertNotNull(schema)
        assertEquals(4, schema.fieldCount)
    }
    
    @Test
    fun `test async with multiple compression codecs`() = runBlocking {
        val people = listOf(
            AsyncPerson(1L, "Alice", 30, 75000.0),
            AsyncPerson(2L, "Bob", 25, 65000.0)
        )
        
        val codecs = listOf(
            CompressionCodec.UNCOMPRESSED,
            CompressionCodec.SNAPPY,
            CompressionCodec.GZIP,
            CompressionCodec.ZSTD
        )
        
        codecs.forEach { codec ->
            val file = File(tempDir, "async_${codec.name.lowercase()}.parquet")
            
            // Write with specific codec
            ParquetFileAsync.writeObjects(file, people, codec)
            
            // Read back
            val readPeople = ParquetFileAsync.readObjects<AsyncPerson>(file)
            
            assertEquals(2, readPeople.size)
            assertEquals(people, readPeople)
        }
    }
    
    @Test
    fun `test async Flow with filtering`() = runBlocking {
        val people = listOf(
            AsyncPerson(1L, "Alice", 30, 75000.0),
            AsyncPerson(2L, "Bob", 25, 65000.0),
            AsyncPerson(3L, "Charlie", 35, 85000.0),
            AsyncPerson(4L, "David", 28, 70000.0)
        )
        
        val file = File(tempDir, "async_flow_filter.parquet")
        
        // Write asynchronously
        ParquetFileAsync.writeObjects(file, people)
        
        // Read as Flow and filter
        val filteredPeople = ParquetFileAsync.readObjectsAsFlow<AsyncPerson>(file)
            .toList()
            .filter { it.age > 28 }
        
        assertEquals(2, filteredPeople.size)
        assertEquals(listOf("Alice", "Charlie"), filteredPeople.map { it.name }.sorted())
    }
}
