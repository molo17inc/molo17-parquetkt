package com.molo17.parquetkt

import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.schema.Encoding
import com.molo17.parquetkt.serialization.ParquetSerializer
import com.molo17.parquetkt.serialization.SchemaReflector
import org.junit.jupiter.api.Test
import java.io.File

class GenerateTestFileForOnlineReader {
    
    data class SimpleTestData(
        val id: Int,
        val name: String,
        val value: Double,
        val active: Boolean
    )
    
    @Test
    fun `generate simple test file for online reader`() {
        // Create simple test data
        val data = (1..100).map { i ->
            SimpleTestData(
                id = i,
                name = "User_$i",
                value = i * 10.5,
                active = i % 2 == 0
            )
        }
        
        val outputFile = File("test-output/online_reader_test.parquet")
        outputFile.parentFile?.mkdirs()
        
        // Write file
        val schema = SchemaReflector.reflectSchema<SimpleTestData>()
        val serializer = ParquetSerializer.create<SimpleTestData>()
        val rowGroup = serializer.serialize(data, schema)
        ParquetFile.write(outputFile.absolutePath, schema, listOf(rowGroup))
        
        println("✅ Test file generated successfully!")
        println("   File: ${outputFile.absolutePath}")
        println("   Size: ${outputFile.length()} bytes (${outputFile.length() / 1024} KB)")
        println("   Rows: ${data.size}")
        
        // Verify the file
        val reader = ParquetReader(outputFile.absolutePath)
        val readSchema = reader.schema
        val fileMetadata = reader.javaClass.getDeclaredField("fileMetadata").let {
            it.isAccessible = true
            it.get(reader) as com.molo17.parquetkt.thrift.FileMetaData
        }
        
        println("\n📊 File Metadata:")
        println("   Parquet version: ${fileMetadata.version}")
        println("   Total rows: ${fileMetadata.numRows}")
        println("   Row groups: ${fileMetadata.rowGroups.size}")
        
        println("\n📋 Column Encodings:")
        val rowGroupMeta = fileMetadata.rowGroups.first()
        for (columnChunk in rowGroupMeta.columns) {
            val columnName = columnChunk.metaData.pathInSchema.first()
            val encodings = columnChunk.metaData.encodings
            val type = columnChunk.metaData.type
            
            println("   - $columnName ($type): ${encodings.joinToString { it.name }}")
            
            // Verify no dictionary encoding
            if (encodings.contains(Encoding.PLAIN_DICTIONARY)) {
                println("     ❌ ERROR: Contains PLAIN_DICTIONARY!")
            }
            if (encodings.contains(Encoding.RLE_DICTIONARY)) {
                println("     ❌ ERROR: Contains RLE_DICTIONARY!")
            }
        }
        
        reader.close()
        
        println("\n✅ File is ready for testing with online Parquet reader")
        println("   Upload this file: ${outputFile.absolutePath}")
    }
    
    @Test
    fun `generate larger test file for online reader`() {
        // Create larger dataset with more variety
        val data = (1..10000).map { i ->
            SimpleTestData(
                id = i,
                name = "TestUser_${i}_${if (i % 3 == 0) "Admin" else "Regular"}",
                value = i * 3.14159,
                active = i % 7 != 0
            )
        }
        
        val outputFile = File("test-output/online_reader_test_large.parquet")
        outputFile.parentFile?.mkdirs()
        
        // Write file
        val schema = SchemaReflector.reflectSchema<SimpleTestData>()
        val serializer = ParquetSerializer.create<SimpleTestData>()
        val rowGroup = serializer.serialize(data, schema)
        ParquetFile.write(outputFile.absolutePath, schema, listOf(rowGroup))
        
        println("✅ Large test file generated successfully!")
        println("   File: ${outputFile.absolutePath}")
        println("   Size: ${outputFile.length()} bytes (${outputFile.length() / 1024} KB)")
        println("   Rows: ${data.size}")
        
        // Quick verification
        val reader = ParquetReader(outputFile.absolutePath)
        val fileMetadata = reader.javaClass.getDeclaredField("fileMetadata").let {
            it.isAccessible = true
            it.get(reader) as com.molo17.parquetkt.thrift.FileMetaData
        }
        
        println("   Parquet version: ${fileMetadata.version}")
        println("   Total rows: ${fileMetadata.numRows}")
        
        reader.close()
        
        println("\n✅ Large file is ready for testing")
        println("   Upload this file: ${outputFile.absolutePath}")
    }
}
