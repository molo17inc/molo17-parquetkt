package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class StreamingWriteTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test streaming write with multiple row groups`() {
        val file = File(tempDir, "streaming_test.parquet")
        
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.string("name"),
            DataField.int32("value")
        )
        
        // Create writer with small maxRowGroupsInMemory for testing
        val writer = ParquetWriter(
            file.absolutePath, 
            schema,
            maxRowGroupsInMemory = 3  // Flush after 3 row groups
        )
        
        // Write 10 row groups - should trigger multiple auto-flushes
        val rowsPerGroup = 100
        for (groupIdx in 0 until 10) {
            val ids = Array<Long?>(rowsPerGroup) { (groupIdx * rowsPerGroup + it).toLong() }
            val names = Array<String?>(rowsPerGroup) { "User_${groupIdx * rowsPerGroup + it}" }
            val values = Array<Int?>(rowsPerGroup) { it }
            
            val columns = listOf(
                DataColumn(schema.fields[0], ids),
                DataColumn(schema.fields[1], names),
                DataColumn(schema.fields[2], values)
            )
            
            writer.writeRowGroup(columns)
        }
        
        writer.close()
        
        // Verify file was written correctly
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema
        assertNotNull(readSchema)
        assertEquals(3, readSchema.fieldCount)
        
        // Verify we can read all row groups
        val totalRows = (0 until reader.rowGroupCount).sumOf { 
            reader.readRowGroup(it).rowCount 
        }
        assertEquals(1000, totalRows)
        
        reader.close()
        
        println("✅ Streaming write test passed")
        println("   File: ${file.absolutePath}")
        println("   Size: ${file.length()} bytes")
        println("   Row groups: ${reader.rowGroupCount}")
        println("   Total rows: $totalRows")
    }
    
    @Test
    fun `test manual flush control`() {
        val file = File(tempDir, "manual_flush_test.parquet")
        
        val schema = ParquetSchema.create(
            DataField.string("data")
        )
        
        // Create writer with high maxRowGroupsInMemory (no auto-flush)
        val writer = ParquetWriter(
            file.absolutePath, 
            schema,
            maxRowGroupsInMemory = 100
        )
        
        // Write 5 row groups
        for (i in 0 until 5) {
            val data = Array<String?>(50) { "Row_${i * 50 + it}" }
            val columns = listOf(DataColumn(schema.fields[0], data))
            writer.writeRowGroup(columns)
            
            // Manually flush after every 2 row groups
            if ((i + 1) % 2 == 0) {
                writer.flushRowGroups()
            }
        }
        
        writer.close()
        
        // Verify file
        val reader = ParquetReader(file.absolutePath)
        assertEquals(5, reader.rowGroupCount)
        reader.close()
        
        println("✅ Manual flush control test passed")
        println("   Successfully wrote and read 5 row groups with manual flushing")
    }
    
    @Test
    fun `test memory efficiency with reduced buffer sizes`() {
        val file = File(tempDir, "memory_efficient.parquet")
        
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.string("description")
        )
        
        // Use smaller row group and page sizes
        val writer = ParquetWriter(
            outputPath = file.absolutePath,
            schema = schema,
            rowGroupSize = 1 * 1024 * 1024,  // 1 MB row groups
            pageSize = 64 * 1024,             // 64 KB pages
            maxRowGroupsInMemory = 5
        )
        
        // Write data
        val rowCount = 1000
        val ids = Array<Long?>(rowCount) { it.toLong() }
        val descriptions = Array<String?>(rowCount) { "Description for row $it with some text to make it larger" }
        
        val columns = listOf(
            DataColumn(schema.fields[0], ids),
            DataColumn(schema.fields[1], descriptions)
        )
        
        writer.writeRowGroup(columns)
        writer.close()
        
        // Verify
        val reader = ParquetReader(file.absolutePath)
        val rowGroup = reader.readRowGroup(0)
        assertEquals(rowCount, rowGroup.rowCount)
        reader.close()
        
        println("✅ Memory efficiency test passed")
        println("   File size: ${file.length()} bytes")
        println("   Rows written: $rowCount")
    }
}
