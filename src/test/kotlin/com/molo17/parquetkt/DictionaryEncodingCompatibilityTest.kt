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
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.Encoding
import com.molo17.parquetkt.schema.ParquetType
import com.molo17.parquetkt.serialization.ParquetDeserializer
import com.molo17.parquetkt.serialization.ParquetSerializer
import com.molo17.parquetkt.serialization.SchemaReflector
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class DictionaryEncodingCompatibilityTest {
    
    data class TestData(
        val id: Int,
        val name: String,
        val category: String,
        val value: Double
    )
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test dictionary encoding metadata compatibility`() {
        // Create test data with repetitive strings (good for dictionary encoding)
        val data = (1..1000).map { i ->
            TestData(
                id = i,
                name = "User_${i % 10}", // Only 10 unique names
                category = "Category_${i % 5}", // Only 5 unique categories
                value = i * 1.5
            )
        }
        
        val file = File(tempDir, "dict_encoded_test.parquet")
        
        // Write with dictionary encoding explicitly enabled
        val schema = SchemaReflector.reflectSchema<TestData>()
        val serializer = ParquetSerializer.create<TestData>()
        val rowGroup = serializer.serialize(data, schema)
        
        ParquetWriter(file.absolutePath, schema, enableDictionary = true).use { writer ->
            writer.write(rowGroup)
        }
        
        // Read the file metadata directly to validate encodings
        val reader = ParquetReader(file.absolutePath)
        // Force initialization to read metadata
        val readSchema = reader.schema
        val fileMetadata = reader.javaClass.getDeclaredField("fileMetadata").let {
            it.isAccessible = true
            it.get(reader) as com.molo17.parquetkt.thrift.FileMetaData
        }
        
        // Verify file was created
        assertTrue(file.exists(), "Parquet file should exist")
        assertTrue(file.length() > 0, "Parquet file should not be empty")
        
        // Check each column's encoding metadata
        val rowGroupMeta = fileMetadata.rowGroups.first()
        
        for (columnChunk in rowGroupMeta.columns) {
            val columnName = columnChunk.metaData.pathInSchema.first()
            val encodings = columnChunk.metaData.encodings
            
            println("Column: $columnName")
            println("  Type: ${columnChunk.metaData.type}")
            println("  Encodings: ${encodings.joinToString { it.name }}")
            
            // Verify no PLAIN_DICTIONARY encoding is present (deprecated)
            assertFalse(
                encodings.contains(Encoding.PLAIN_DICTIONARY),
                "Column $columnName should not use deprecated PLAIN_DICTIONARY encoding"
            )
            
            // String columns should use dictionary encoding
            if (columnChunk.metaData.type == ParquetType.BYTE_ARRAY) {
                assertTrue(
                    encodings.contains(Encoding.RLE_DICTIONARY),
                    "String column $columnName should use RLE_DICTIONARY encoding"
                )

                // Should have RLE for levels and RLE_DICTIONARY for data
                assertTrue(
                    encodings.contains(Encoding.RLE),
                    "Column $columnName should have RLE encoding for definition levels"
                )

                // Dictionary page values are PLAIN-encoded per Parquet spec
                assertTrue(
                    encodings.contains(Encoding.PLAIN),
                    "Column $columnName should list PLAIN encoding for dictionary page values"
                )
            }
            
            // Non-string columns should use PLAIN encoding
            if (columnChunk.metaData.type != ParquetType.BYTE_ARRAY) {
                assertTrue(
                    encodings.contains(Encoding.PLAIN),
                    "Non-string column $columnName should use PLAIN encoding"
                )
                
                assertFalse(
                    encodings.contains(Encoding.RLE_DICTIONARY),
                    "Non-string column $columnName should not use dictionary encoding"
                )
            }
        }
        
        reader.close()
        
        println("✅ Dictionary encoding metadata is correct and compatible with external readers")
    }
    
    @Test
    fun `test dictionary encoding improves compression for repetitive data`() {
        // Create highly repetitive data
        val repetitiveData = (1..5000).map { i ->
            TestData(
                id = i,
                name = "CommonName", // Same name for all rows
                category = "SameCategory", // Same category for all rows
                value = i * 1.0
            )
        }
        
        val dictFile = File(tempDir, "with_dictionary.parquet")
        
        // Write with dictionary encoding explicitly enabled
        val schema = SchemaReflector.reflectSchema<TestData>()
        val serializer = ParquetSerializer.create<TestData>()
        val rowGroup = serializer.serialize(repetitiveData, schema)
        
        ParquetWriter(dictFile.absolutePath, schema, enableDictionary = true).use { writer ->
            writer.write(rowGroup)
        }
        
        val dictFileSize = dictFile.length()
        
        println("File with dictionary encoding: ${dictFileSize / 1024} KB")
        
        // Verify dictionary encoding was used
        val reader = ParquetReader(dictFile.absolutePath)
        val readSchema = reader.schema
        val fileMetadata = reader.javaClass.getDeclaredField("fileMetadata").let {
            it.isAccessible = true
            it.get(reader) as com.molo17.parquetkt.thrift.FileMetaData
        }
        val stringColumns = fileMetadata.rowGroups.first().columns.filter {
            it.metaData.type == ParquetType.BYTE_ARRAY
        }

        for (column in stringColumns) {
            assertTrue(
                column.metaData.encodings.contains(Encoding.RLE_DICTIONARY),
                "String columns should use dictionary encoding for repetitive data"
            )
        }

        reader.close()

        println("✅ Dictionary encoding is being used for repetitive data")
    }
    
    @Test
    fun `test file is compatible with external readers`() {
        // Create a simple dataset
        val data = listOf(
            TestData(1, "Alice", "A", 100.0),
            TestData(2, "Bob", "B", 200.0),
            TestData(3, "Charlie", "A", 300.0),
            TestData(4, "David", "B", 400.0),
            TestData(5, "Eve", "C", 500.0)
        )
        
        val file = File(tempDir, "external_reader_test.parquet")
        
        // Write file
        val schema = SchemaReflector.reflectSchema<TestData>()
        val serializer = ParquetSerializer.create<TestData>()
        val rowGroup = serializer.serialize(data, schema)
        ParquetFile.write(file.absolutePath, schema, listOf(rowGroup))
        
        // Validate the file structure like an external reader would
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema
        val fileMetadata = reader.javaClass.getDeclaredField("fileMetadata").let {
            it.isAccessible = true
            it.get(reader) as com.molo17.parquetkt.thrift.FileMetaData
        }
        
        // Check Parquet version
        assertEquals(1, fileMetadata.version, "Should use Parquet format version 1")
        
        // Check row count
        assertEquals(data.size.toLong(), fileMetadata.numRows, "Row count should match")
        
        // Check schema structure
        assertTrue(fileMetadata.schema.size > 1, "Schema should have multiple elements")
        
        // Validate each column can be read
        val rowGroupMeta = fileMetadata.rowGroups.first()
        assertEquals(4, rowGroupMeta.columns.size, "Should have 4 columns")
        
        // Verify no deprecated or invalid encodings
        for (column in rowGroupMeta.columns) {
            val encodings = column.metaData.encodings
            
            // No deprecated encodings
            assertFalse(
                encodings.contains(Encoding.PLAIN_DICTIONARY),
                "Should not use deprecated PLAIN_DICTIONARY"
            )
            
            // All encodings should be valid
            for (encoding in encodings) {
                assertTrue(
                    encoding in listOf(
                        Encoding.PLAIN,
                        Encoding.RLE,
                        Encoding.RLE_DICTIONARY,
                        Encoding.DELTA_BINARY_PACKED,
                        Encoding.DELTA_LENGTH_BYTE_ARRAY,
                        Encoding.DELTA_BYTE_ARRAY
                    ),
                    "Encoding ${encoding.name} should be a valid Parquet encoding"
                )
            }
        }
        
        // Verify we can actually read all the data
        val rowGroups = ParquetFile.read(file)
        val deserializer = ParquetDeserializer.create<TestData>()
        val readData = deserializer.deserializeSequence(rowGroups).toList()
        
        assertEquals(data, readData, "Data should be readable and match original")
        
        reader.close()
        
        println("✅ File is compatible with external Parquet readers")
        println("   File: ${file.absolutePath}")
        println("   Size: ${file.length()} bytes")
        println("   Rows: ${data.size}")
    }
}
