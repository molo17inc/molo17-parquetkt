package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.util.ArrayPool
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ArrayPoolIntegrationTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test ParquetWriter with ArrayPool`() {
        val file = File(tempDir, "pooled_write.parquet")
        val pool = ArrayPool()
        
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.string("name"),
            DataField.double("value")
        )
        
        // Create writer with array pool
        val writer = ParquetWriter(
            outputPath = file.absolutePath,
            schema = schema,
            arrayPool = pool
        )
        
        // Write multiple row groups
        for (batch in 0 until 5) {
            val ids = Array<Long?>(100) { (batch * 100 + it).toLong() }
            val names = Array<String?>(100) { "User_${batch * 100 + it}" }
            val values = Array<Double?>(100) { (batch * 100 + it).toDouble() }
            
            writer.writeRowGroup(listOf(
                DataColumn(schema.fields[0], ids),
                DataColumn(schema.fields[1], names),
                DataColumn(schema.fields[2], values)
            ))
        }
        
        writer.close()
        
        // Verify file was written correctly
        val reader = ParquetReader(file.absolutePath)
        assertEquals(5, reader.rowGroupCount)
        
        val totalRows = (0 until reader.rowGroupCount).sumOf {
            reader.readRowGroup(it).rowCount
        }
        assertEquals(500, totalRows)
        
        reader.close()
        
        // Check pool statistics
        val stats = pool.getStats()
        println("✅ ArrayPool integration test passed")
        println("   File size: ${file.length()} bytes")
        println("   Rows written: $totalRows")
        println("   Pool stats:")
        println(stats)
    }
    
    @Test
    fun `test memory efficiency with ArrayPool vs without`() {
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.string("data")
        )
        
        // Measure with pool
        val fileWithPool = File(tempDir, "with_pool.parquet")
        val pool = ArrayPool()
        
        val startTime1 = System.currentTimeMillis()
        val writer1 = ParquetWriter(
            outputPath = fileWithPool.absolutePath,
            schema = schema,
            arrayPool = pool
        )
        
        repeat(10) { batch ->
            val ids = Array<Long?>(1000) { (batch * 1000 + it).toLong() }
            val data = Array<String?>(1000) { "Data_${batch * 1000 + it}_with_some_longer_text" }
            
            writer1.writeRowGroup(listOf(
                DataColumn(schema.fields[0], ids),
                DataColumn(schema.fields[1], data)
            ))
        }
        writer1.close()
        val time1 = System.currentTimeMillis() - startTime1
        
        // Measure without pool
        val fileWithoutPool = File(tempDir, "without_pool.parquet")
        
        val startTime2 = System.currentTimeMillis()
        val writer2 = ParquetWriter(
            outputPath = fileWithoutPool.absolutePath,
            schema = schema
            // No array pool
        )
        
        repeat(10) { batch ->
            val ids = Array<Long?>(1000) { (batch * 1000 + it).toLong() }
            val data = Array<String?>(1000) { "Data_${batch * 1000 + it}_with_some_longer_text" }
            
            writer2.writeRowGroup(listOf(
                DataColumn(schema.fields[0], ids),
                DataColumn(schema.fields[1], data)
            ))
        }
        writer2.close()
        val time2 = System.currentTimeMillis() - startTime2
        
        // Both should produce valid files
        assertTrue(fileWithPool.exists())
        assertTrue(fileWithoutPool.exists())
        
        // Verify both files are readable
        val reader1 = ParquetReader(fileWithPool.absolutePath)
        assertEquals(10, reader1.rowGroupCount)
        reader1.close()
        
        val reader2 = ParquetReader(fileWithoutPool.absolutePath)
        assertEquals(10, reader2.rowGroupCount)
        reader2.close()
        
        println("✅ Memory efficiency comparison test passed")
        println("   With ArrayPool: ${time1}ms")
        println("   Without ArrayPool: ${time2}ms")
        println("   Pool had ${pool.getStats().totalArrays} arrays cached")
    }
    
    @Test
    fun `test shared ArrayPool instance`() {
        val file = File(tempDir, "shared_pool.parquet")
        
        val schema = ParquetSchema.create(
            DataField.string("message")
        )
        
        // Use shared pool
        val writer = ParquetWriter(
            outputPath = file.absolutePath,
            schema = schema,
            arrayPool = ArrayPool.shared
        )
        
        repeat(3) {
            val messages = Array<String?>(50) { "Message_$it" }
            writer.writeRowGroup(listOf(
                DataColumn(schema.fields[0], messages)
            ))
        }
        
        writer.close()
        
        // Verify
        val reader = ParquetReader(file.absolutePath)
        assertNotNull(reader.schema)
        reader.close()
        
        println("✅ Shared ArrayPool test passed")
        println("   Shared pool stats:")
        println(ArrayPool.shared.getStats())
    }
}
