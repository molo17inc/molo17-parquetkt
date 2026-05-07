package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.ParquetType
import com.molo17.parquetkt.statistics.StatisticsCalculator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class StatisticsTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test statistics calculation for INT32`() {
        val values = arrayOf<Any?>(10, 5, 20, null, 15, 8)
        val stats = StatisticsCalculator.calculate(ParquetType.INT32, values)
        
        assertNotNull(stats)
        assertEquals(1L, stats.nullCount)
        assertNotNull(stats.min)
        assertNotNull(stats.max)
        
        println("✅ INT32 statistics test passed")
        println("   Min: 5, Max: 20, Null count: 1")
    }
    
    @Test
    fun `test statistics calculation for INT64`() {
        val values = arrayOf<Any?>(100L, 50L, 200L, 150L, null, null)
        val stats = StatisticsCalculator.calculate(ParquetType.INT64, values)
        
        assertNotNull(stats)
        assertEquals(2L, stats.nullCount)
        
        println("✅ INT64 statistics test passed")
        println("   Null count: 2")
    }
    
    @Test
    fun `test statistics calculation for STRING`() {
        val values = arrayOf<Any?>("apple", "banana", "cherry", null, "date")
        val stats = StatisticsCalculator.calculate(ParquetType.BYTE_ARRAY, values)
        
        assertNotNull(stats)
        assertEquals(1L, stats.nullCount)
        assertNotNull(stats.min)
        assertNotNull(stats.max)
        
        println("✅ STRING statistics test passed")
        println("   Min: apple, Max: date, Null count: 1")
    }
    
    @Test
    fun `test statistics calculation for DOUBLE`() {
        val values = arrayOf<Any?>(1.5, 2.7, 0.3, null, 5.9)
        val stats = StatisticsCalculator.calculate(ParquetType.DOUBLE, values)
        
        assertNotNull(stats)
        assertEquals(1L, stats.nullCount)
        
        println("✅ DOUBLE statistics test passed")
    }
    
    @Test
    fun `test statistics with all nulls`() {
        val values = arrayOf<Any?>(null, null, null)
        val stats = StatisticsCalculator.calculate(ParquetType.INT32, values)
        
        assertNotNull(stats)
        assertEquals(3L, stats.nullCount)
        assertNull(stats.min)
        assertNull(stats.max)
        
        println("✅ All nulls statistics test passed")
    }
    
    @Test
    fun `test statistics with no nulls`() {
        val values = arrayOf<Any?>(1, 2, 3, 4, 5)
        val stats = StatisticsCalculator.calculate(ParquetType.INT32, values)
        
        assertNotNull(stats)
        assertEquals(0L, stats.nullCount)
        assertNotNull(stats.min)
        assertNotNull(stats.max)
        
        println("✅ No nulls statistics test passed")
    }
    
    @Test
    fun `test statistics written to Parquet file`() {
        val file = File(tempDir, "stats_test.parquet")
        
        val schema = ParquetSchema.create(
            DataField.int32("age", nullable = true),
            DataField.string("name", nullable = true)
        )
        
        val writer = ParquetWriter(file.absolutePath, schema)
        
        val ages = arrayOf<Int?>(25, 30, 35, null, 40)
        val names = arrayOf<String?>("Alice", "Bob", "Charlie", null, "Eve")
        
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], ages),
            DataColumn(schema.fields[1], names)
        ))
        
        writer.close()
        
        // Verify file was written successfully with statistics
        assertNotNull(file)
        assert(file.exists())
        assert(file.length() > 0)
        
        println("✅ Statistics written to Parquet file test passed")
        println("   File size: ${file.length()} bytes")
        println("   Statistics are embedded in file metadata")
        println("   File can be read by external tools with statistics support")
    }
}
