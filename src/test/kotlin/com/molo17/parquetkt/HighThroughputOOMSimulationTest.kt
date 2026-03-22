package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.util.ArrayPool
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.random.Random
import kotlin.test.assertTrue

/**
 * Test suite that simulates production OOM scenarios observed in Gluesync under heavy load.
 * 
 * Production scenario characteristics (from monitoring data - March 2026):
 * - JVM heap: ~8GB max, baseline ~2GB, spikes to 7-8GB before OOM
 * - High transaction rates: 400K-600K rows/minute during peak (6,600-10,000 rows/sec)
 * - Multiple concurrent entities being processed simultaneously
 * - Variable batch sizes (CDC changes: small batches, snapshots: large batches)
 * - Sustained load over hours (e.g., 3am batch processing window)
 * - Memory pressure builds up gradually until OOM around 3am
 * - OOM correlates with high row transfer rates + multiple concurrent entities
 * 
 * To run with constrained heap (simulating production):
 * ```
 * ./gradlew test --tests "HighThroughputOOMSimulationTest" -Dorg.gradle.jvmargs="-Xmx512m"
 * ```
 * 
 * These tests verify that the ParquetWriter memory optimizations prevent OOM
 * under similar conditions.
 */
class HighThroughputOOMSimulationTest {

    @TempDir
    lateinit var tempDir: File

    /**
     * Simulates the production scenario where multiple entities are being synced concurrently,
     * each writing to their own parquet file with variable batch sizes.
     * 
     * This is the closest simulation to the actual Gluesync production workload.
     */
    @Test
    fun `simulate concurrent entity sync with variable batch sizes`() {
        val runtime = Runtime.getRuntime()
        val baselineHeap = currentHeapUsage(runtime)
        var peakHeap = baselineHeap
        
        val entityCount = 5 // Simulate 5 concurrent entities
        val batchesPerEntity = 50 // Each entity processes 50 batches
        val totalRowsWritten = AtomicLong(0)
        
        // Track writers per entity (simulating FileParquetManager's transactionData map)
        val entityWriters = ConcurrentHashMap<String, ParquetWriter>()
        val entitySchemas = ConcurrentHashMap<String, ParquetSchema>()
        val entityFiles = ConcurrentHashMap<String, File>()
        
        // Create schemas with varying column counts (like different tables in production)
        for (i in 0 until entityCount) {
            val columnCount = 10 + (i * 5) // 10, 15, 20, 25, 30 columns
            val fields = (0 until columnCount).map { col ->
                when (col % 4) {
                    0 -> DataField.int64("id_$col")
                    1 -> DataField.string("text_$col")
                    2 -> DataField.int32("num_$col")
                    else -> DataField.double("decimal_$col")
                }
            }
            entitySchemas["entity_$i"] = ParquetSchema.create(fields)
            entityFiles["entity_$i"] = File(tempDir, "entity_${i}.parquet")
        }
        
        val executor = Executors.newFixedThreadPool(entityCount)
        val startTime = System.currentTimeMillis()
        
        // Submit concurrent tasks for each entity
        val futures = (0 until entityCount).map { entityIdx ->
            executor.submit {
                val entityId = "entity_$entityIdx"
                val schema = entitySchemas[entityId]!!
                val file = entityFiles[entityId]!!
                
                // Use createAuto - the adaptive constructor
                val writer = ParquetWriter.createAuto(file, schema)
                entityWriters[entityId] = writer
                
                val random = Random(entityIdx)
                
                for (batch in 0 until batchesPerEntity) {
                    // Variable batch sizes: 100-2000 rows (simulating CDC vs snapshot batches)
                    val batchSize = 100 + random.nextInt(1900)
                    
                    val columns = schema.fields.mapIndexed { colIdx, field ->
                        when (colIdx % 4) {
                            0 -> {
                                val data = Array<Long?>(batchSize) { (batch * batchSize + it).toLong() }
                                DataColumn(field, data)
                            }
                            1 -> {
                                val data = Array<String?>(batchSize) { 
                                    "Entity${entityIdx}_Batch${batch}_Row${it}_" + "x".repeat(random.nextInt(100))
                                }
                                DataColumn(field, data)
                            }
                            2 -> {
                                val data = Array<Int?>(batchSize) { it + batch * 1000 }
                                DataColumn(field, data)
                            }
                            else -> {
                                val data = Array<Double?>(batchSize) { it * 1.5 + batch }
                                DataColumn(field, data)
                            }
                        }
                    }
                    
                    writer.writeRowGroup(columns)
                    totalRowsWritten.addAndGet(batchSize.toLong())
                    
                    // Periodically check memory (simulating production monitoring)
                    if (batch % 10 == 0) {
                        synchronized(this) {
                            peakHeap = max(peakHeap, currentHeapUsage(runtime))
                        }
                    }
                }
                
                writer.close()
            }
        }
        
        // Wait for all entities to complete
        futures.forEach { it.get() }
        executor.shutdown()
        executor.awaitTermination(5, TimeUnit.MINUTES)
        
        val durationMs = System.currentTimeMillis() - startTime
        val heapDelta = max(0L, peakHeap - baselineHeap)
        val throughput = totalRowsWritten.get() / (durationMs / 1000.0)
        
        // Verify all files were written correctly
        var totalReadRows = 0L
        entityFiles.values.forEach { file ->
            assertTrue(file.exists(), "File ${file.name} should exist")
            val reader = ParquetReader(file.absolutePath)
            repeat(reader.rowGroupCount) { 
                totalReadRows += reader.readRowGroup(it).rowCount 
            }
            reader.close()
        }
        
        println("=" .repeat(70))
        println("CONCURRENT ENTITY SYNC SIMULATION RESULTS")
        println("=" .repeat(70))
        println("Entities: $entityCount")
        println("Batches per entity: $batchesPerEntity")
        println("Total rows written: ${totalRowsWritten.get()}")
        println("Total rows verified: $totalReadRows")
        println("Duration: ${durationMs}ms")
        println("Throughput: ${"%.0f".format(throughput)} rows/sec")
        println("Peak heap delta: ${formatBytes(heapDelta)}")
        println("Max heap available: ${formatBytes(runtime.maxMemory())}")
        println("ArrayPool stats: ${ArrayPool.shared.getStats()}")
        println("=" .repeat(70))
        
        // Memory budget: should not exceed 50% of max heap for this workload
        val maxAllowedHeap = runtime.maxMemory() / 2
        assertTrue(
            heapDelta <= maxAllowedHeap,
            "Heap increase ${formatBytes(heapDelta)} exceeds 50% of max heap ${formatBytes(maxAllowedHeap)}"
        )
        
        assertTrue(
            totalReadRows == totalRowsWritten.get(),
            "All written rows should be readable"
        )
    }

    /**
     * Simulates sustained high-throughput writes over an extended period,
     * similar to the 3am batch processing that caused OOM in production.
     * 
     * This test writes continuously for a set duration, monitoring memory throughout.
     */
    @Test
    fun `simulate sustained high throughput writes with memory monitoring`() {
        val runtime = Runtime.getRuntime()
        val baselineHeap = currentHeapUsage(runtime)
        var peakHeap = baselineHeap
        val memorySnapshots = mutableListOf<Long>()
        
        // Schema similar to production tables (20 columns with mixed types)
        val schema = ParquetSchema.create(
            (0 until 20).map { idx ->
                when (idx % 5) {
                    0 -> DataField.int64("pk_$idx")
                    1 -> DataField.string("varchar_$idx")
                    2 -> DataField.int32("int_$idx")
                    3 -> DataField.double("decimal_$idx")
                    else -> DataField.string("text_$idx", nullable = true)
                }
            }
        )
        
        val file = File(tempDir, "sustained_throughput.parquet")
        val writer = ParquetWriter.createAuto(file, schema)
        
        val targetDurationMs = 10_000L // 10 seconds of sustained writes
        val batchSize = 1000 // 1000 rows per batch
        val startTime = System.currentTimeMillis()
        var totalRows = 0L
        var batchCount = 0
        
        while (System.currentTimeMillis() - startTime < targetDurationMs) {
            val columns = schema.fields.mapIndexed { colIdx, field ->
                when (colIdx % 5) {
                    0 -> DataColumn(field, Array<Long?>(batchSize) { (batchCount * batchSize + it).toLong() })
                    1 -> DataColumn(field, Array<String?>(batchSize) { "Row_${batchCount}_$it" })
                    2 -> DataColumn(field, Array<Int?>(batchSize) { it })
                    3 -> DataColumn(field, Array<Double?>(batchSize) { it * 0.01 })
                    else -> DataColumn(field, Array<String?>(batchSize) { if (it % 3 == 0) null else "Optional_$it" })
                }
            }
            
            writer.writeRowGroup(columns)
            totalRows += batchSize
            batchCount++
            
            // Memory snapshot every 100 batches
            if (batchCount % 100 == 0) {
                val currentHeap = currentHeapUsage(runtime)
                memorySnapshots.add(currentHeap)
                peakHeap = max(peakHeap, currentHeap)
            }
        }
        
        writer.close()
        
        val durationMs = System.currentTimeMillis() - startTime
        val heapDelta = max(0L, peakHeap - baselineHeap)
        val throughput = totalRows / (durationMs / 1000.0)
        
        // Verify file
        val reader = ParquetReader(file.absolutePath)
        var readRows = 0L
        repeat(reader.rowGroupCount) { readRows += reader.readRowGroup(it).rowCount }
        reader.close()
        
        // Check for memory growth trend (should be stable, not growing)
        val memoryGrowthTrend = if (memorySnapshots.size >= 2) {
            val firstHalf = memorySnapshots.take(memorySnapshots.size / 2).average()
            val secondHalf = memorySnapshots.drop(memorySnapshots.size / 2).average()
            (secondHalf - firstHalf) / firstHalf * 100
        } else 0.0
        
        println("=" .repeat(70))
        println("SUSTAINED HIGH THROUGHPUT SIMULATION RESULTS")
        println("=" .repeat(70))
        println("Duration: ${durationMs}ms")
        println("Total batches: $batchCount")
        println("Total rows: $totalRows")
        println("Verified rows: $readRows")
        println("Throughput: ${"%.0f".format(throughput)} rows/sec")
        println("Peak heap delta: ${formatBytes(heapDelta)}")
        println("Memory growth trend: ${"%.1f".format(memoryGrowthTrend)}%")
        println("File size: ${formatBytes(file.length())}")
        val rowGroupReader = ParquetReader(file.absolutePath)
        val rowGroupCount = rowGroupReader.rowGroupCount
        rowGroupReader.close()
        println("Row groups in file: $rowGroupCount")
        println("=" .repeat(70))
        
        // Memory should not grow more than 20% over the test duration
        assertTrue(
            memoryGrowthTrend < 20.0,
            "Memory growth trend ${memoryGrowthTrend}% indicates potential memory leak"
        )
        
        assertTrue(readRows == totalRows, "All rows should be readable")
    }

    /**
     * Simulates the worst-case scenario: large snapshot with many columns,
     * which is when OOM is most likely to occur.
     */
    @Test
    fun `simulate large snapshot with wide table schema`() {
        val runtime = Runtime.getRuntime()
        val baselineHeap = currentHeapUsage(runtime)
        
        // Wide table: 100 columns (common in enterprise databases)
        val columnCount = 100
        val schema = ParquetSchema.create(
            (0 until columnCount).map { idx ->
                when (idx % 6) {
                    0 -> DataField.int64("id_$idx")
                    1 -> DataField.string("name_$idx")
                    2 -> DataField.int32("count_$idx")
                    3 -> DataField.double("amount_$idx")
                    4 -> DataField.string("description_$idx", nullable = true)
                    else -> DataField.int64("timestamp_$idx")
                }
            }
        )
        
        val file = File(tempDir, "wide_table_snapshot.parquet")
        
        // createAuto should detect the wide schema and use aggressive settings
        val writer = ParquetWriter.createAuto(file, schema)
        
        val totalRows = 100_000 // 100K rows snapshot
        val batchSize = 500 // Small batches to simulate streaming
        var peakHeap = baselineHeap
        
        val startTime = System.currentTimeMillis()
        
        for (batch in 0 until (totalRows / batchSize)) {
            val columns = schema.fields.mapIndexed { colIdx, field ->
                when (colIdx % 6) {
                    0 -> DataColumn(field, Array<Long?>(batchSize) { (batch * batchSize + it).toLong() })
                    1 -> DataColumn(field, Array<String?>(batchSize) { "Name_${batch}_$it" })
                    2 -> DataColumn(field, Array<Int?>(batchSize) { it })
                    3 -> DataColumn(field, Array<Double?>(batchSize) { it * 1.5 })
                    4 -> DataColumn(field, Array<String?>(batchSize) { if (it % 2 == 0) "Desc_$it" else null })
                    else -> DataColumn(field, Array<Long?>(batchSize) { System.currentTimeMillis() })
                }
            }
            
            writer.writeRowGroup(columns)
            
            if (batch % 20 == 0) {
                peakHeap = max(peakHeap, currentHeapUsage(runtime))
            }
        }
        
        writer.close()
        
        val durationMs = System.currentTimeMillis() - startTime
        val heapDelta = max(0L, peakHeap - baselineHeap)
        
        // Verify
        val reader = ParquetReader(file.absolutePath)
        var readRows = 0L
        repeat(reader.rowGroupCount) { readRows += reader.readRowGroup(it).rowCount }
        reader.close()
        
        println("=" .repeat(70))
        println("WIDE TABLE SNAPSHOT SIMULATION RESULTS")
        println("=" .repeat(70))
        println("Columns: $columnCount")
        println("Total rows: $totalRows")
        println("Verified rows: $readRows")
        println("Duration: ${durationMs}ms")
        println("Peak heap delta: ${formatBytes(heapDelta)}")
        println("File size: ${formatBytes(file.length())}")
        println("=" .repeat(70))
        
        // For wide tables, memory budget is tighter
        val maxAllowedHeap = runtime.maxMemory() / 3 // 33% max
        assertTrue(
            heapDelta <= maxAllowedHeap,
            "Wide table heap increase ${formatBytes(heapDelta)} exceeds budget ${formatBytes(maxAllowedHeap)}"
        )
        
        assertTrue(readRows == totalRows.toLong(), "All rows should be readable")
    }

    /**
     * Stress test that intentionally creates memory pressure by:
     * 1. Allocating background memory to reduce available heap
     * 2. Writing at production-like throughput rates
     * 3. Verifying the writer gracefully handles memory pressure without OOM
     * 
     * This test is designed to fail fast if memory management is broken.
     */
    @Test
    fun `stress test with artificial memory pressure`() {
        val runtime = Runtime.getRuntime()
        
        // Force GC to get accurate baseline
        System.gc()
        Thread.sleep(100)
        
        val baselineHeap = currentHeapUsage(runtime)
        val maxHeap = runtime.maxMemory()
        
        // Allocate ~40% of heap to create memory pressure
        val pressureSize = (maxHeap * 0.4).toLong().coerceAtMost(500 * 1024 * 1024) // Max 500MB
        val pressureArrays = mutableListOf<ByteArray>()
        var allocatedPressure = 0L
        
        try {
            while (allocatedPressure < pressureSize) {
                val chunkSize = minOf(10 * 1024 * 1024, (pressureSize - allocatedPressure).toInt()) // 10MB chunks
                pressureArrays.add(ByteArray(chunkSize))
                allocatedPressure += chunkSize
            }
        } catch (e: OutOfMemoryError) {
            // If we can't even allocate pressure, skip this test
            println("⚠️ Could not allocate memory pressure, skipping stress test")
            return
        }
        
        println("Allocated ${formatBytes(allocatedPressure)} of memory pressure")
        
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.string("data"),
            DataField.int32("value"),
            DataField.double("amount"),
            DataField.string("description")
        )
        
        val file = File(tempDir, "stress_test.parquet")
        val writer = ParquetWriter.createAuto(file, schema)
        
        var peakHeap = currentHeapUsage(runtime)
        var totalRows = 0L
        val targetRows = 50_000L // Write 50K rows under pressure
        val batchSize = 500
        
        val startTime = System.currentTimeMillis()
        
        try {
            while (totalRows < targetRows) {
                val columns = listOf(
                    DataColumn(schema.fields[0], Array<Long?>(batchSize) { totalRows + it }),
                    DataColumn(schema.fields[1], Array<String?>(batchSize) { "Data_${totalRows + it}_padding" }),
                    DataColumn(schema.fields[2], Array<Int?>(batchSize) { it }),
                    DataColumn(schema.fields[3], Array<Double?>(batchSize) { it * 1.5 }),
                    DataColumn(schema.fields[4], Array<String?>(batchSize) { "Description for row ${totalRows + it}" })
                )
                
                writer.writeRowGroup(columns)
                totalRows += batchSize
                
                peakHeap = max(peakHeap, currentHeapUsage(runtime))
            }
            
            writer.close()
            
        } finally {
            // Release pressure memory
            pressureArrays.clear()
            System.gc()
        }
        
        val durationMs = System.currentTimeMillis() - startTime
        val throughput = totalRows / (durationMs / 1000.0)
        
        // Verify file
        val reader = ParquetReader(file.absolutePath)
        var readRows = 0L
        repeat(reader.rowGroupCount) { readRows += reader.readRowGroup(it).rowCount }
        reader.close()
        
        println("=" .repeat(70))
        println("STRESS TEST WITH MEMORY PRESSURE RESULTS")
        println("=" .repeat(70))
        println("Memory pressure applied: ${formatBytes(allocatedPressure)}")
        println("Max heap: ${formatBytes(maxHeap)}")
        println("Peak heap used: ${formatBytes(peakHeap)}")
        println("Total rows: $totalRows")
        println("Verified rows: $readRows")
        println("Duration: ${durationMs}ms")
        println("Throughput: ${"%.0f".format(throughput)} rows/sec")
        println("=" .repeat(70))
        
        assertTrue(readRows == totalRows, "All rows should be readable after stress test")
        println("✅ Stress test passed - writer handled memory pressure gracefully")
    }

    /**
     * Test that matches production throughput rates: 6,600-10,000 rows/sec
     * This verifies the writer can sustain production-level throughput.
     */
    @Test
    fun `verify production throughput rate is achievable`() {
        val runtime = Runtime.getRuntime()
        
        // Production-like schema (15 columns)
        val schema = ParquetSchema.create(
            (0 until 15).map { idx ->
                when (idx % 5) {
                    0 -> DataField.int64("col_$idx")
                    1 -> DataField.string("col_$idx")
                    2 -> DataField.int32("col_$idx")
                    3 -> DataField.double("col_$idx")
                    else -> DataField.string("col_$idx", nullable = true)
                }
            }
        )
        
        val file = File(tempDir, "throughput_test.parquet")
        val writer = ParquetWriter.createAuto(file, schema)
        
        val targetThroughput = 6_600.0 // Minimum production rate: 6,600 rows/sec
        val testDurationSec = 5 // Run for 5 seconds
        val batchSize = 1000
        
        var totalRows = 0L
        var peakHeap = currentHeapUsage(runtime)
        val startTime = System.currentTimeMillis()
        
        while ((System.currentTimeMillis() - startTime) < testDurationSec * 1000) {
            val columns = schema.fields.mapIndexed { colIdx, field ->
                when (colIdx % 5) {
                    0 -> DataColumn(field, Array<Long?>(batchSize) { totalRows + it })
                    1 -> DataColumn(field, Array<String?>(batchSize) { "Value_${totalRows + it}" })
                    2 -> DataColumn(field, Array<Int?>(batchSize) { it })
                    3 -> DataColumn(field, Array<Double?>(batchSize) { it * 0.01 })
                    else -> DataColumn(field, Array<String?>(batchSize) { if (it % 3 == 0) null else "Opt_$it" })
                }
            }
            
            writer.writeRowGroup(columns)
            totalRows += batchSize
            peakHeap = max(peakHeap, currentHeapUsage(runtime))
        }
        
        writer.close()
        
        val actualDurationSec = (System.currentTimeMillis() - startTime) / 1000.0
        val actualThroughput = totalRows / actualDurationSec
        
        // Verify
        val reader = ParquetReader(file.absolutePath)
        var readRows = 0L
        repeat(reader.rowGroupCount) { readRows += reader.readRowGroup(it).rowCount }
        reader.close()
        
        println("=" .repeat(70))
        println("PRODUCTION THROUGHPUT VERIFICATION")
        println("=" .repeat(70))
        println("Target throughput: ${"%.0f".format(targetThroughput)} rows/sec")
        println("Actual throughput: ${"%.0f".format(actualThroughput)} rows/sec")
        println("Total rows: $totalRows")
        println("Duration: ${"%.2f".format(actualDurationSec)} sec")
        println("Peak heap: ${formatBytes(peakHeap)}")
        println("Throughput ratio: ${"%.1f".format(actualThroughput / targetThroughput)}x target")
        println("=" .repeat(70))
        
        assertTrue(
            actualThroughput >= targetThroughput,
            "Throughput ${"%.0f".format(actualThroughput)} rows/sec is below production minimum ${"%.0f".format(targetThroughput)} rows/sec"
        )
        
        assertTrue(readRows == totalRows, "All rows should be readable")
        println("✅ Production throughput test passed")
    }

    private fun currentHeapUsage(runtime: Runtime): Long {
        return max(0L, runtime.totalMemory() - runtime.freeMemory())
    }

    private fun formatBytes(bytes: Long): String {
        if (bytes <= 0) return "0 B"
        val units = arrayOf("B", "KB", "MB", "GB")
        var value = bytes.toDouble()
        var unitIndex = 0
        while (value >= 1024 && unitIndex < units.lastIndex) {
            value /= 1024
            unitIndex++
        }
        return String.format("%.2f %s", value, units[unitIndex])
    }
}
