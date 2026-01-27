package com.molo17.parquetkt

import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.LogicalType
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.ParquetType
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class LogicalTypeMetadataTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test DATE logical type is written to metadata`() {
        val file = File(tempDir, "dates_manual.parquet")
        
        // Create schema manually with DATE logical type
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.date("appt_date"),
            DataField.string("description")
        )
        
        // Verify schema has DATE logical type
        val apptDateField = schema.fields.find { it.name == "appt_date" }
        assertNotNull(apptDateField)
        assertEquals(LogicalType.DATE, apptDateField.logicalType)
        assertEquals(ParquetType.INT32, apptDateField.dataType)
        
        // Write some data
        val writer = ParquetWriter(file.absolutePath, schema)
        
        // Write a row (date as days since epoch: 2024-01-15 = 19737 days since 1970-01-01)
        val dateValue = LocalDate.of(2024, 1, 15).toEpochDay().toInt()
        val columns = listOf(
            com.molo17.parquetkt.data.DataColumn(schema.fields[0], arrayOf(1L)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[1], arrayOf(dateValue)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[2], arrayOf("Test appointment"))
        )
        writer.writeRowGroup(columns)
        
        writer.close()
        
        // Read back and verify metadata was written correctly
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema
        reader.close()
        
        val readApptDateField = readSchema.fields.find { it.name == "appt_date" }
        assertNotNull(readApptDateField)
        assertEquals(LogicalType.DATE, readApptDateField.logicalType)
        assertEquals(ParquetType.INT32, readApptDateField.dataType)
        
        println("✅ DATE logical type correctly written to Parquet metadata")
        println("   Field: appt_date")
        println("   Physical type: ${readApptDateField.dataType}")
        println("   Logical type: ${readApptDateField.logicalType}")
        println("   This will be displayed as date32[day] in modern Parquet readers like Arrow/DuckDB")
    }
    
    @Test
    fun `test TIMESTAMP logical type is written to metadata`() {
        val file = File(tempDir, "timestamps.parquet")
        
        // Create schema with TIMESTAMP_MICROS logical type
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.timestampMicros("confirmed_on"),
            DataField.string("status")
        )
        
        // Verify schema has TIMESTAMP_MICROS logical type
        val timestampField = schema.fields.find { it.name == "confirmed_on" }
        assertNotNull(timestampField)
        assertEquals(LogicalType.TIMESTAMP_MICROS, timestampField.logicalType)
        assertEquals(ParquetType.INT64, timestampField.dataType)
        
        // Write some data
        val writer = ParquetWriter(file.absolutePath, schema)
        
        val timestampValue = 1705334400000000L // 2024-01-15 12:00:00 UTC in microseconds
        val columns = listOf(
            com.molo17.parquetkt.data.DataColumn(schema.fields[0], arrayOf(1L)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[1], arrayOf(timestampValue)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[2], arrayOf("confirmed"))
        )
        writer.writeRowGroup(columns)
        
        writer.close()
        
        // Read back and verify
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema
        reader.close()
        
        val readTimestampField = readSchema.fields.find { it.name == "confirmed_on" }
        assertNotNull(readTimestampField)
        assertEquals(LogicalType.TIMESTAMP_MICROS, readTimestampField.logicalType)
        assertEquals(ParquetType.INT64, readTimestampField.dataType)
        
        println("✅ TIMESTAMP logical type correctly written to Parquet metadata")
        println("   Field: confirmed_on")
        println("   Physical type: ${readTimestampField.dataType}")
        println("   Logical type: ${readTimestampField.logicalType}")
        println("   This will be displayed as timestamp[us, tz=UTC] in modern Parquet readers")
    }
    
    @Test
    fun `test TIME logical type is written to metadata`() {
        val file = File(tempDir, "times.parquet")
        
        // Create schema with TIME_MICROS logical type
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.time("appt_time"),
            DataField.string("description")
        )
        
        // Verify schema has TIME_MICROS logical type
        val timeField = schema.fields.find { it.name == "appt_time" }
        assertNotNull(timeField)
        assertEquals(LogicalType.TIME_MICROS, timeField.logicalType)
        assertEquals(ParquetType.INT64, timeField.dataType)
        
        // Write some data
        val writer = ParquetWriter(file.absolutePath, schema)
        
        val timeValue = 43200000000L // 12:00:00 in microseconds
        val columns = listOf(
            com.molo17.parquetkt.data.DataColumn(schema.fields[0], arrayOf(1L)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[1], arrayOf(timeValue)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[2], arrayOf("Lunch appointment"))
        )
        writer.writeRowGroup(columns)
        
        writer.close()
        
        // Read back and verify
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema
        reader.close()
        
        val readTimeField = readSchema.fields.find { it.name == "appt_time" }
        assertNotNull(readTimeField)
        assertEquals(LogicalType.TIME_MICROS, readTimeField.logicalType)
        assertEquals(ParquetType.INT64, readTimeField.dataType)
        
        println("✅ TIME logical type correctly written to Parquet metadata")
        println("   Field: appt_time")
        println("   Physical type: ${readTimeField.dataType}")
        println("   Logical type: ${readTimeField.logicalType}")
        println("   This will be displayed as time64[us] in modern Parquet readers")
    }
    
    @Test
    fun `test STRING logical type is written to metadata`() {
        val file = File(tempDir, "strings.parquet")
        
        // Create schema with STRING logical type
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.string("name"),
            DataField.string("email")
        )
        
        // Verify schema has STRING logical type
        val nameField = schema.fields.find { it.name == "name" }
        assertNotNull(nameField)
        assertEquals(LogicalType.STRING, nameField.logicalType)
        assertEquals(ParquetType.BYTE_ARRAY, nameField.dataType)
        
        // Write some data
        val writer = ParquetWriter(file.absolutePath, schema)
        
        val columns = listOf(
            com.molo17.parquetkt.data.DataColumn(schema.fields[0], arrayOf(1L)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[1], arrayOf("Alice")),
            com.molo17.parquetkt.data.DataColumn(schema.fields[2], arrayOf("alice@example.com"))
        )
        writer.writeRowGroup(columns)
        
        writer.close()
        
        // Read back and verify
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema
        reader.close()
        
        val readNameField = readSchema.fields.find { it.name == "name" }
        assertNotNull(readNameField)
        assertEquals(LogicalType.STRING, readNameField.logicalType)
        assertEquals(ParquetType.BYTE_ARRAY, readNameField.dataType)
        
        println("✅ STRING logical type correctly written to Parquet metadata")
        println("   Field: name")
        println("   Physical type: ${readNameField.dataType}")
        println("   Logical type: ${readNameField.logicalType}")
        println("   This will be displayed as utf8 in modern Parquet readers")
    }
    
    @Test
    fun `test DECIMAL logical type is written to metadata`() {
        val file = File(tempDir, "decimals.parquet")
        
        // Create schema with DECIMAL logical type
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.decimal("price", precision = 10, scale = 2),
            DataField.string("currency")
        )
        
        // Verify schema has DECIMAL logical type with precision and scale
        val priceField = schema.fields.find { it.name == "price" }
        assertNotNull(priceField)
        assertEquals(LogicalType.DECIMAL, priceField.logicalType)
        assertEquals(ParquetType.FIXED_LEN_BYTE_ARRAY, priceField.dataType)
        assertEquals(10, priceField.precision)
        assertEquals(2, priceField.scale)
        
        // Write some data
        val writer = ParquetWriter(file.absolutePath, schema)
        
        // For DECIMAL, we need to encode the value as bytes
        // 123.45 with scale 2 = 12345 as a big integer
        val decimalBytes = ByteArray(16) { 0 }
        val value = 12345
        decimalBytes[15] = (value and 0xFF).toByte()
        decimalBytes[14] = ((value shr 8) and 0xFF).toByte()
        
        val columns = listOf(
            com.molo17.parquetkt.data.DataColumn(schema.fields[0], arrayOf(1L)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[1], arrayOf(decimalBytes)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[2], arrayOf("USD"))
        )
        writer.writeRowGroup(columns)
        
        writer.close()
        
        // Read back and verify
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema
        reader.close()
        
        val readPriceField = readSchema.fields.find { it.name == "price" }
        assertNotNull(readPriceField)
        assertEquals(LogicalType.DECIMAL, readPriceField.logicalType)
        assertEquals(ParquetType.FIXED_LEN_BYTE_ARRAY, readPriceField.dataType)
        assertEquals(10, readPriceField.precision)
        assertEquals(2, readPriceField.scale)
        
        println("✅ DECIMAL logical type correctly written to Parquet metadata")
        println("   Field: price")
        println("   Physical type: ${readPriceField.dataType}")
        println("   Logical type: ${readPriceField.logicalType}")
        println("   Precision: ${readPriceField.precision}, Scale: ${readPriceField.scale}")
        println("   This will be displayed as decimal(10,2) in modern Parquet readers")
    }
    
    @Test
    fun `test all temporal types together`() {
        val file = File(tempDir, "all_temporal.parquet")
        
        // Create schema with all temporal types
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.date("birth_date"),
            DataField.time("meeting_time"),
            DataField.timestamp("created_at"),
            DataField.timestampMicros("updated_at"),
            DataField.string("description")
        )
        
        // Write some data
        val writer = ParquetWriter(file.absolutePath, schema)
        
        val columns = listOf(
            com.molo17.parquetkt.data.DataColumn(schema.fields[0], arrayOf(1L)),
            com.molo17.parquetkt.data.DataColumn(schema.fields[1], arrayOf(LocalDate.of(1990, 5, 15).toEpochDay().toInt())),
            com.molo17.parquetkt.data.DataColumn(schema.fields[2], arrayOf(36000000000L)), // 10:00:00 in micros
            com.molo17.parquetkt.data.DataColumn(schema.fields[3], arrayOf(1705334400000L)), // millis
            com.molo17.parquetkt.data.DataColumn(schema.fields[4], arrayOf(1705334400000000L)), // micros
            com.molo17.parquetkt.data.DataColumn(schema.fields[5], arrayOf("Test record"))
        )
        writer.writeRowGroup(columns)
        
        writer.close()
        
        // Read back and verify all fields
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema
        reader.close()
        
        val birthDate = readSchema.fields.find { it.name == "birth_date" }
        assertNotNull(birthDate)
        assertEquals(LogicalType.DATE, birthDate.logicalType)
        
        val meetingTime = readSchema.fields.find { it.name == "meeting_time" }
        assertNotNull(meetingTime)
        assertEquals(LogicalType.TIME_MICROS, meetingTime.logicalType)
        
        val createdAt = readSchema.fields.find { it.name == "created_at" }
        assertNotNull(createdAt)
        assertEquals(LogicalType.TIMESTAMP_MILLIS, createdAt.logicalType)
        
        val updatedAt = readSchema.fields.find { it.name == "updated_at" }
        assertNotNull(updatedAt)
        assertEquals(LogicalType.TIMESTAMP_MICROS, updatedAt.logicalType)
        
        println("✅ All temporal logical types correctly written to Parquet metadata")
        println("   birth_date: ${birthDate.logicalType} (date32[day])")
        println("   meeting_time: ${meetingTime.logicalType} (time64[us])")
        println("   created_at: ${createdAt.logicalType} (timestamp[ms, tz=UTC])")
        println("   updated_at: ${updatedAt.logicalType} (timestamp[us, tz=UTC])")
    }
}
