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
}
