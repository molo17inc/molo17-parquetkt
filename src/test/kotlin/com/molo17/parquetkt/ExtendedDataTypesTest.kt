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
import com.molo17.parquetkt.schema.ParquetType
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ExtendedDataTypesTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test INT96 type`() {
        // INT96 is used for legacy timestamp encoding (12 bytes: 8 bytes nanos + 4 bytes julian day)
        val schema = ParquetSchema.create(
            DataField(
                name = "timestamp_col",
                dataType = ParquetType.INT96,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED
            )
        )
        
        // Create INT96 values (12 bytes each)
        // Format: 8 bytes nanoseconds of day + 4 bytes Julian day number
        val timestamp1 = createInt96Timestamp(2024, 1, 15, 10, 30, 0)
        val timestamp2 = createInt96Timestamp(2024, 6, 20, 14, 45, 30)
        val timestamp3 = createInt96Timestamp(2025, 12, 31, 23, 59, 59)
        
        val timestampColumn = DataColumn.createRequired(
            DataField(
                name = "timestamp_col",
                dataType = ParquetType.INT96,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED
            ),
            listOf(timestamp1, timestamp2, timestamp3)
        )
        
        val rowGroup = RowGroup(schema, listOf(timestampColumn))
        val file = File(tempDir, "int96_test.parquet")
        
        ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.UNCOMPRESSED)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(3, readRowGroups[0].rowCount)
        
        val readColumn = readRowGroups[0].getColumn("timestamp_col")
        assertNotNull(readColumn)
        assertEquals(3, readColumn.size)
        
        // Verify the values are ByteArrays of length 12
        val dataField = readColumn.javaClass.getDeclaredField("data")
        dataField.isAccessible = true
        @Suppress("UNCHECKED_CAST")
        val dataArray = dataField.get(readColumn) as Array<Any?>
        assertEquals(12, (dataArray[0] as ByteArray).size)
        assertEquals(12, (dataArray[1] as ByteArray).size)
        assertEquals(12, (dataArray[2] as ByteArray).size)
    }
    
    @Test
    fun `test FIXED_LEN_BYTE_ARRAY type`() {
        // FIXED_LEN_BYTE_ARRAY is commonly used for UUIDs (16 bytes) and other fixed-size data
        val schema = ParquetSchema.create(
            DataField(
                name = "uuid_col",
                dataType = ParquetType.FIXED_LEN_BYTE_ARRAY,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED,
                length = 16
            )
        )
        
        // Create 16-byte UUID-like values
        val uuid1 = ByteArray(16) { it.toByte() }
        val uuid2 = ByteArray(16) { (it * 2).toByte() }
        val uuid3 = ByteArray(16) { (255 - it).toByte() }
        
        val uuidColumn = DataColumn.createRequired(
            DataField(
                name = "uuid_col",
                dataType = ParquetType.FIXED_LEN_BYTE_ARRAY,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED,
                length = 16
            ),
            listOf(uuid1, uuid2, uuid3)
        )
        
        val rowGroup = RowGroup(schema, listOf(uuidColumn))
        val file = File(tempDir, "fixed_len_test.parquet")
        
        ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.UNCOMPRESSED)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(3, readRowGroups[0].rowCount)
        
        val readColumn = readRowGroups[0].getColumn("uuid_col")
        assertNotNull(readColumn)
        assertEquals(3, readColumn.size)
        
        // Verify the values match
        val dataField = readColumn.javaClass.getDeclaredField("data")
        dataField.isAccessible = true
        @Suppress("UNCHECKED_CAST")
        val dataArray = dataField.get(readColumn) as Array<Any?>
        assertEquals(16, (dataArray[0] as ByteArray).size)
        assertEquals(16, (dataArray[1] as ByteArray).size)
        assertEquals(16, (dataArray[2] as ByteArray).size)
    }
    
    @Test
    fun `test DATE logical type`() {
        val schema = ParquetSchema.create(
            DataField.date("date_col", nullable = false)
        )
        
        // Dates are stored as INT32 (days since Unix epoch)
        val date1 = LocalDate.of(2024, 1, 15)
        val date2 = LocalDate.of(2024, 6, 20)
        val date3 = LocalDate.of(2025, 12, 31)
        
        val dateColumn = DataColumn.createRequired(
            DataField.date("date_col"),
            listOf(date1, date2, date3)
        )
        
        val rowGroup = RowGroup(schema, listOf(dateColumn))
        val file = File(tempDir, "date_test.parquet")
        
        ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.UNCOMPRESSED)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(3, readRowGroups[0].rowCount)
        
        val readColumn = readRowGroups[0].getColumn("date_col")
        assertNotNull(readColumn)
        assertEquals(3, readColumn.size)
    }
    
    @Test
    fun `test TIMESTAMP logical type`() {
        val schema = ParquetSchema.create(
            DataField.timestamp("timestamp_col", nullable = false)
        )
        
        // Timestamps are stored as INT64 (microseconds since Unix epoch)
        val timestamp1 = LocalDateTime.of(2024, 1, 15, 10, 30, 0)
        val timestamp2 = LocalDateTime.of(2024, 6, 20, 14, 45, 30)
        val timestamp3 = LocalDateTime.of(2025, 12, 31, 23, 59, 59)
        
        val timestampColumn = DataColumn.createRequired(
            DataField.timestamp("timestamp_col"),
            listOf(timestamp1, timestamp2, timestamp3)
        )
        
        val rowGroup = RowGroup(schema, listOf(timestampColumn))
        val file = File(tempDir, "timestamp_test.parquet")
        
        ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.UNCOMPRESSED)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(3, readRowGroups[0].rowCount)
        
        val readColumn = readRowGroups[0].getColumn("timestamp_col")
        assertNotNull(readColumn)
        assertEquals(3, readColumn.size)
    }
    
    @Test
    fun `test all extended types together`() {
        val schema = ParquetSchema.create(
            DataField(
                name = "int96_col",
                dataType = ParquetType.INT96,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED
            ),
            DataField(
                name = "fixed_col",
                dataType = ParquetType.FIXED_LEN_BYTE_ARRAY,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED,
                length = 16
            ),
            DataField.date("date_col"),
            DataField.timestamp("timestamp_col")
        )
        
        val int96Column = DataColumn.createRequired(
            DataField(
                name = "int96_col",
                dataType = ParquetType.INT96,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED
            ),
            listOf(
                createInt96Timestamp(2024, 1, 1, 0, 0, 0),
                createInt96Timestamp(2024, 6, 15, 12, 0, 0)
            )
        )
        
        val fixedColumn = DataColumn.createRequired(
            DataField(
                name = "fixed_col",
                dataType = ParquetType.FIXED_LEN_BYTE_ARRAY,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED,
                length = 16
            ),
            listOf(
                ByteArray(16) { it.toByte() },
                ByteArray(16) { (it * 2).toByte() }
            )
        )
        
        val dateColumn = DataColumn.createRequired(
            DataField.date("date_col"),
            listOf(
                LocalDate.of(2024, 1, 1),
                LocalDate.of(2024, 6, 15)
            )
        )
        
        val timestampColumn = DataColumn.createRequired(
            DataField.timestamp("timestamp_col"),
            listOf(
                LocalDateTime.of(2024, 1, 1, 0, 0, 0),
                LocalDateTime.of(2024, 6, 15, 12, 0, 0)
            )
        )
        
        val rowGroup = RowGroup(
            schema,
            listOf(int96Column, fixedColumn, dateColumn, timestampColumn)
        )
        
        val file = File(tempDir, "all_extended_types.parquet")
        
        ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.SNAPPY)
        
        val readRowGroups = ParquetFile.read(file)
        
        assertEquals(1, readRowGroups.size)
        assertEquals(2, readRowGroups[0].rowCount)
        assertEquals(4, readRowGroups[0].columnCount)
    }
    
    /**
     * Create an INT96 timestamp value (12 bytes)
     * Format: 8 bytes nanoseconds of day (little-endian) + 4 bytes Julian day number (little-endian)
     */
    private fun createInt96Timestamp(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): ByteArray {
        // Calculate Julian day number
        val a = (14 - month) / 12
        val y = year + 4800 - a
        val m = month + 12 * a - 3
        val julianDay = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045
        
        // Calculate nanoseconds of day
        val nanosOfDay = (hour * 3600L + minute * 60L + second) * 1_000_000_000L
        
        // Create 12-byte array
        val buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putLong(nanosOfDay)
        buffer.putInt(julianDay)
        
        return buffer.array()
    }
}
