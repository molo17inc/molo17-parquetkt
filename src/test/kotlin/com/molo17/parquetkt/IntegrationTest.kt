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
import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class IntegrationTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test basic write and read with primitive types`() {
        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.string("name"),
            DataField.double("value")
        )
        
        val idColumn = DataColumn.createRequired(
            DataField.int32("id"),
            listOf(1, 2, 3, 4, 5)
        )
        
        val nameColumn = DataColumn.createRequired(
            DataField.string("name"),
            listOf("Alice", "Bob", "Charlie", "Diana", "Eve").map { it.toByteArray() }
        )
        
        val valueColumn = DataColumn.createRequired(
            DataField.double("value"),
            listOf(100.5, 200.75, 300.25, 400.0, 500.5)
        )
        
        val rowGroup = RowGroup(schema, listOf(idColumn, nameColumn, valueColumn))
        val file = File(tempDir, "test.parquet")
        
        ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.SNAPPY)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(5, readRowGroups[0].rowCount)
        assertEquals(3, readRowGroups[0].columnCount)
    }
    
    @Test
    fun `test write and read with nullable fields`() {
        val schema = ParquetSchema.create(
            DataField.int64("id", nullable = false),
            DataField.string("email", nullable = true),
            DataField.int32("age", nullable = false)
        )
        
        val idColumn = DataColumn.createRequired(
            DataField.int64("id"),
            listOf(1L, 2L, 3L)
        )
        
        val emailColumn = DataColumn.create(
            DataField.string("email", nullable = true),
            listOf("alice@example.com".toByteArray(), null, "charlie@example.com".toByteArray()),
            definitionLevels = intArrayOf(1, 0, 1)
        )
        
        val ageColumn = DataColumn.createRequired(
            DataField.int32("age"),
            listOf(30, 25, 35)
        )
        
        val rowGroup = RowGroup(schema, listOf(idColumn, emailColumn, ageColumn))
        val file = File(tempDir, "nullable.parquet")
        
        ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.UNCOMPRESSED)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(3, readRowGroups[0].rowCount)
        
        val readEmailColumn = readRowGroups[0].getColumn("email")
        assertNotNull(readEmailColumn)
        assertEquals(3, readEmailColumn.size)
    }
    
    @Test
    fun `test different compression codecs`() {
        val schema = ParquetSchema.create(
            DataField.int32("value")
        )
        
        val column = DataColumn.createRequired(
            DataField.int32("value"),
            (1..100).toList()
        )
        
        val rowGroup = RowGroup(schema, listOf(column))
        
        val codecs = listOf(
            CompressionCodec.UNCOMPRESSED,
            CompressionCodec.SNAPPY,
            CompressionCodec.GZIP,
            CompressionCodec.ZSTD
        )
        
        for (codec in codecs) {
            val file = File(tempDir, "test_${codec.name.lowercase()}.parquet")
            
            ParquetFile.write(file, schema, listOf(rowGroup), codec)
            
            val readRowGroups = ParquetFile.read(file)
            assertEquals(1, readRowGroups.size)
            assertEquals(100, readRowGroups[0].rowCount)
        }
    }
    
    @Test
    fun `test multiple row groups`() {
        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.string("data")
        )
        
        val rowGroups = (1..3).map { groupNum ->
            val idColumn = DataColumn.createRequired(
                DataField.int32("id"),
                ((groupNum - 1) * 10 + 1..(groupNum * 10)).toList()
            )
            
            val dataColumn = DataColumn.createRequired(
                DataField.string("data"),
                ((groupNum - 1) * 10 + 1..(groupNum * 10)).map { "data_$it".toByteArray() }
            )
            
            RowGroup(schema, listOf(idColumn, dataColumn))
        }
        
        val file = File(tempDir, "multi_rowgroup.parquet")
        
        ParquetFile.write(file, schema, rowGroups, CompressionCodec.SNAPPY)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(3, readRowGroups.size)
        readRowGroups.forEach { rg ->
            assertEquals(10, rg.rowCount)
        }
    }
    
    @Test
    fun `test all primitive types`() {
        val schema = ParquetSchema.create(
            DataField.boolean("bool_col"),
            DataField.int32("int32_col"),
            DataField.int64("int64_col"),
            DataField.float("float_col"),
            DataField.double("double_col"),
            DataField.byteArray("bytes_col")
        )
        
        val boolColumn = DataColumn.createRequired(
            DataField.boolean("bool_col"),
            listOf(true, false, true)
        )
        
        val int32Column = DataColumn.createRequired(
            DataField.int32("int32_col"),
            listOf(1, 2, 3)
        )
        
        val int64Column = DataColumn.createRequired(
            DataField.int64("int64_col"),
            listOf(100L, 200L, 300L)
        )
        
        val floatColumn = DataColumn.createRequired(
            DataField.float("float_col"),
            listOf(1.5f, 2.5f, 3.5f)
        )
        
        val doubleColumn = DataColumn.createRequired(
            DataField.double("double_col"),
            listOf(10.5, 20.5, 30.5)
        )
        
        val bytesColumn = DataColumn.createRequired(
            DataField.byteArray("bytes_col"),
            listOf("hello".toByteArray(), "world".toByteArray(), "test".toByteArray())
        )
        
        val rowGroup = RowGroup(
            schema,
            listOf(boolColumn, int32Column, int64Column, floatColumn, doubleColumn, bytesColumn)
        )
        
        val file = File(tempDir, "all_types.parquet")
        
        ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.SNAPPY)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(3, readRowGroups[0].rowCount)
        assertEquals(6, readRowGroups[0].columnCount)
    }
}
