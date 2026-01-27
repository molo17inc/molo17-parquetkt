package com.molo17.parquetkt

import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.schema.Encoding
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class VerifyBenchmarkFileTest {
    
    @Test
    fun `verify benchmark file has no dictionary encoding`() {
        val benchmarkFile = File("benchmark-output/benchmark_100cols_500krows.parquet")
        
        if (!benchmarkFile.exists()) {
            println("⚠️ Benchmark file not found, skipping test")
            return
        }
        
        println("Checking benchmark file: ${benchmarkFile.absolutePath}")
        println("File size: ${benchmarkFile.length() / 1024} KB")
        println("Last modified: ${java.util.Date(benchmarkFile.lastModified())}")
        
        val reader = ParquetReader(benchmarkFile.absolutePath)
        val schema = reader.schema
        val fileMetadata = reader.javaClass.getDeclaredField("fileMetadata").let {
            it.isAccessible = true
            it.get(reader) as com.molo17.parquetkt.thrift.FileMetaData
        }
        
        println("\nFile metadata:")
        println("  Version: ${fileMetadata.version}")
        println("  Rows: ${fileMetadata.numRows}")
        println("  Row groups: ${fileMetadata.rowGroups.size}")
        
        val rowGroupMeta = fileMetadata.rowGroups.first()
        println("\nChecking ${rowGroupMeta.columns.size} columns:")
        
        var hasAnyDictionaryEncoding = false
        var hasPlainDictionary = false
        var hasRleDictionary = false
        
        for ((index, columnChunk) in rowGroupMeta.columns.withIndex()) {
            val columnName = columnChunk.metaData.pathInSchema.first()
            val encodings = columnChunk.metaData.encodings
            
            if (index < 5 || encodings.any { it == Encoding.PLAIN_DICTIONARY || it == Encoding.RLE_DICTIONARY }) {
                println("  Column $index: $columnName")
                println("    Type: ${columnChunk.metaData.type}")
                println("    Encodings: ${encodings.joinToString { it.name }}")
            }
            
            if (encodings.contains(Encoding.PLAIN_DICTIONARY)) {
                hasPlainDictionary = true
                println("    ❌ ERROR: Contains PLAIN_DICTIONARY!")
            }
            
            if (encodings.contains(Encoding.RLE_DICTIONARY)) {
                hasRleDictionary = true
                println("    ⚠️ WARNING: Contains RLE_DICTIONARY!")
            }
            
            if (encodings.any { it == Encoding.PLAIN_DICTIONARY || it == Encoding.RLE_DICTIONARY }) {
                hasAnyDictionaryEncoding = true
            }
        }
        
        reader.close()
        
        println("\nSummary:")
        println("  Has PLAIN_DICTIONARY: $hasPlainDictionary")
        println("  Has RLE_DICTIONARY: $hasRleDictionary")
        println("  Has any dictionary encoding: $hasAnyDictionaryEncoding")
        
        assertFalse(hasPlainDictionary, "File should not contain PLAIN_DICTIONARY encoding")
        assertFalse(hasRleDictionary, "File should not contain RLE_DICTIONARY encoding (disabled by default)")
        assertFalse(hasAnyDictionaryEncoding, "File should not contain any dictionary encoding")
        
        println("\n✅ Benchmark file has no dictionary encoding - should work with online readers")
    }
}
