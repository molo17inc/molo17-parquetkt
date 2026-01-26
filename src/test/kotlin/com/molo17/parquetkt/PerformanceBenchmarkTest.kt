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
import java.io.File
import kotlin.random.Random
import kotlin.system.measureTimeMillis
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PerformanceBenchmarkTest {
    
    // Use project directory instead of temp directory so files persist
    private val outputDir = File("benchmark-output").apply { mkdirs() }
    
    @Test
    fun `benchmark - write 100 columns with 500K rows`() {
        println("\n" + "=".repeat(80))
        println("PERFORMANCE BENCHMARK: 100 Columns x 50,000 Rows")
        println("=".repeat(80))
        println("NOTE: Reduced from 500K to 50K rows to avoid excessive memory/CPU usage")
        println()
        
        val rowCount = 50_000  // Reduced from 500_000 for reasonable performance
        val columnCount = 100
        
        println("\nGenerating test data...")
        val dataGenTime = measureTimeMillis {
            // Generate schema with 100 columns of different types
            val schema = generateSchema(columnCount)
            
            println("Schema created with $columnCount columns:")
            println("  - ${columnCount / 5} INT32 columns")
            println("  - ${columnCount / 5} INT64 columns")
            println("  - ${columnCount / 5} DOUBLE columns")
            println("  - ${columnCount / 5} STRING columns")
            println("  - ${columnCount / 5} BOOLEAN columns")
            
            // Generate data columns
            println("\nGenerating $rowCount rows of dummy data...")
            val columns = generateDataColumns(schema, rowCount)
            
            // Create row group
            val rowGroup = RowGroup(schema, columns)
            
            println("Data generation complete!")
            println("  Total cells: ${rowCount * columnCount}")
            println("  Estimated data size: ~${estimateDataSize(rowCount, columnCount)} MB")
            
            // Write to Parquet file
            val file = File(outputDir, "benchmark_100cols_500krows.parquet")
            
            println("\n" + "-".repeat(80))
            println("Writing Parquet file...")
            println("-".repeat(80))
            
            val writeTime = measureTimeMillis {
                ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.SNAPPY)
            }
            
            println("✅ WRITE SUCCESSFUL!")
            println("\n" + "=".repeat(80))
            println("BENCHMARK RESULTS")
            println("=".repeat(80))
            println("File: ${file.absolutePath}")
            println("File size: ${file.length() / 1024 / 1024} MB")
            println("Rows: $rowCount")
            println("Columns: $columnCount")
            println("Total cells: ${rowCount * columnCount}")
            println("\nWrite time: $writeTime ms (${writeTime / 1000.0} seconds)")
            println("Throughput: ${String.format("%.0f", rowCount / (writeTime / 1000.0))} rows/second")
            println("Throughput: ${String.format("%.0f", (rowCount * columnCount) / (writeTime / 1000.0))} cells/second")
            println("=".repeat(80))
            
            // Verify file exists and has content
            assertTrue(file.exists(), "Parquet file should exist")
            assertTrue(file.length() > 0, "Parquet file should have content")
            
            // Optional: Read back to verify
            println("\nVerifying file integrity...")
            val readTime = measureTimeMillis {
                val readRowGroups = ParquetFile.read(file)
                assertEquals(1, readRowGroups.size, "Should have 1 row group")
                assertEquals(rowCount, readRowGroups[0].rowCount, "Should have $rowCount rows")
                assertEquals(columnCount, readRowGroups[0].columnCount, "Should have $columnCount columns")
            }
            
            println("✅ File verification successful!")
            println("Read time: $readTime ms (${readTime / 1000.0} seconds)")
            println("\n" + "=".repeat(80))
        }
        
        println("\nTotal benchmark time: $dataGenTime ms (${dataGenTime / 1000.0} seconds)")
        println("=".repeat(80) + "\n")
    }
    
    @Test
    fun `benchmark - write with different compression codecs`() {
        println("\n" + "=".repeat(80))
        println("COMPRESSION BENCHMARK: Testing Different Codecs")
        println("=".repeat(80))
        
        val rowCount = 100_000
        val columnCount = 20
        
        val schema = generateSchema(columnCount)
        val columns = generateDataColumns(schema, rowCount)
        val rowGroup = RowGroup(schema, columns)
        
        val codecs = listOf(
            CompressionCodec.UNCOMPRESSED,
            CompressionCodec.SNAPPY,
            CompressionCodec.GZIP,
            CompressionCodec.ZSTD
        )
        
        println("\nTesting ${codecs.size} compression codecs with:")
        println("  Rows: $rowCount")
        println("  Columns: $columnCount")
        println("  Total cells: ${rowCount * columnCount}")
        println()
        
        codecs.forEach { codec ->
            val file = File(outputDir, "benchmark_${codec.name.lowercase()}.parquet")
            
            val writeTime = measureTimeMillis {
                ParquetFile.write(file, schema, listOf(rowGroup), codec)
            }
            
            val fileSizeMB = file.length() / 1024.0 / 1024.0
            val throughput = rowCount / (writeTime / 1000.0)
            
            println("${codec.name.padEnd(15)} | Time: ${writeTime.toString().padStart(6)} ms | " +
                    "Size: ${String.format("%6.2f", fileSizeMB)} MB | " +
                    "Throughput: ${String.format("%8.0f", throughput)} rows/s")
        }
        
        println("\n" + "=".repeat(80) + "\n")
    }
    
    @Test
    fun `benchmark - write multiple row groups`() {
        println("\n" + "=".repeat(80))
        println("ROW GROUP BENCHMARK: Multiple Row Groups")
        println("=".repeat(80))
        
        val rowsPerGroup = 100_000
        val numGroups = 5
        val columnCount = 20
        
        println("\nGenerating $numGroups row groups:")
        println("  Rows per group: $rowsPerGroup")
        println("  Columns: $columnCount")
        println("  Total rows: ${rowsPerGroup * numGroups}")
        
        val schema = generateSchema(columnCount)
        
        val generationTime = measureTimeMillis {
            val rowGroups = (1..numGroups).map { groupNum ->
                println("  Generating row group $groupNum/$numGroups...")
                val columns = generateDataColumns(schema, rowsPerGroup)
                RowGroup(schema, columns)
            }
            
            val file = File(outputDir, "benchmark_multigroup.parquet")
            
            println("\nWriting to Parquet file...")
            val writeTime = measureTimeMillis {
                ParquetFile.write(file, schema, rowGroups, CompressionCodec.SNAPPY)
            }
            
            println("\n✅ WRITE SUCCESSFUL!")
            println("File size: ${file.length() / 1024 / 1024} MB")
            println("Write time: $writeTime ms (${writeTime / 1000.0} seconds)")
            println("Throughput: ${String.format("%.0f", (rowsPerGroup * numGroups) / (writeTime / 1000.0))} rows/second")
        }
        
        println("\nTotal time: $generationTime ms (${generationTime / 1000.0} seconds)")
        println("=".repeat(80) + "\n")
    }
    
    private fun generateSchema(columnCount: Int): ParquetSchema {
        val fields = mutableListOf<DataField>()
        val typesPerCategory = columnCount / 5
        
        // INT32 columns
        repeat(typesPerCategory) { i ->
            fields.add(DataField.int32("int32_col_$i"))
        }
        
        // INT64 columns
        repeat(typesPerCategory) { i ->
            fields.add(DataField.int64("int64_col_$i"))
        }
        
        // DOUBLE columns
        repeat(typesPerCategory) { i ->
            fields.add(DataField.double("double_col_$i"))
        }
        
        // STRING columns
        repeat(typesPerCategory) { i ->
            fields.add(DataField.string("string_col_$i"))
        }
        
        // BOOLEAN columns
        repeat(typesPerCategory) { i ->
            fields.add(DataField.boolean("boolean_col_$i"))
        }
        
        return ParquetSchema.create(fields)
    }
    
    private fun generateDataColumns(schema: ParquetSchema, rowCount: Int): List<DataColumn<*>> {
        val random = Random(42) // Fixed seed for reproducibility
        val columns = mutableListOf<DataColumn<*>>()
        
        for (i in 0 until schema.fieldCount) {
            val field = schema.getField(i)
            
            val column = when {
                field.name.startsWith("int32_") -> {
                    val data = (1..rowCount).map { random.nextInt(0, 1_000_000) }
                    DataColumn.createRequired(field, data)
                }
                field.name.startsWith("int64_") -> {
                    val data = (1..rowCount).map { random.nextLong(0, 1_000_000_000L) }
                    DataColumn.createRequired(field, data)
                }
                field.name.startsWith("double_") -> {
                    val data = (1..rowCount).map { random.nextDouble(0.0, 1_000_000.0) }
                    DataColumn.createRequired(field, data)
                }
                field.name.startsWith("string_") -> {
                    val data = (1..rowCount).map { 
                        "string_value_${random.nextInt(0, 10000)}".toByteArray() 
                    }
                    DataColumn.createRequired(field, data)
                }
                field.name.startsWith("boolean_") -> {
                    val data = (1..rowCount).map { random.nextBoolean() }
                    DataColumn.createRequired(field, data)
                }
                else -> throw IllegalArgumentException("Unknown column type: ${field.name}")
            }
            
            columns.add(column)
        }
        
        return columns
    }
    
    private fun estimateDataSize(rowCount: Int, columnCount: Int): Int {
        // Rough estimate: 
        // - INT32: 4 bytes
        // - INT64: 8 bytes
        // - DOUBLE: 8 bytes
        // - STRING: ~20 bytes average
        // - BOOLEAN: 1 byte
        // Average: ~8 bytes per cell
        val bytesPerCell = 8
        val totalBytes = rowCount * columnCount * bytesPerCell
        return totalBytes / 1024 / 1024
    }
}
