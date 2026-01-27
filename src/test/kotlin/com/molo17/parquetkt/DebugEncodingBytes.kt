package com.molo17.parquetkt

import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.schema.Encoding
import com.molo17.parquetkt.serialization.ParquetSerializer
import com.molo17.parquetkt.serialization.SchemaReflector
import org.junit.jupiter.api.Test
import java.io.File

class DebugEncodingBytes {
    
    data class MinimalData(val value: Int)
    
    @Test
    fun `debug encoding values in file`() {
        val data = listOf(MinimalData(1), MinimalData(2), MinimalData(3))
        
        val outputFile = File("test-output/debug_minimal.parquet")
        outputFile.parentFile?.mkdirs()
        
        val schema = SchemaReflector.reflectSchema<MinimalData>()
        val serializer = ParquetSerializer.create<MinimalData>()
        val rowGroup = serializer.serialize(data, schema)
        ParquetFile.write(outputFile.absolutePath, schema, listOf(rowGroup))
        
        println("File created: ${outputFile.absolutePath}")
        println("File size: ${outputFile.length()} bytes")
        
        // Read and check encodings
        val reader = ParquetReader(outputFile.absolutePath)
        val readSchema = reader.schema // Initialize metadata
        val fileMetadata = reader.javaClass.getDeclaredField("fileMetadata").let {
            it.isAccessible = true
            it.get(reader) as com.molo17.parquetkt.thrift.FileMetaData
        }
        
        val column = fileMetadata.rowGroups.first().columns.first()
        val encodings = column.metaData.encodings
        
        println("\nColumn: ${column.metaData.pathInSchema.first()}")
        println("Type: ${column.metaData.type}")
        println("Encodings count: ${encodings.size}")
        
        for ((index, encoding) in encodings.withIndex()) {
            println("  Encoding[$index]: ${encoding.name} (thrift value: ${encoding.thriftValue})")
        }
        
        // Check what we expect
        println("\nExpected encodings:")
        println("  RLE (3) for definition levels")
        println("  PLAIN (0) for data")
        
        println("\nActual thrift values written: ${encodings.map { it.thriftValue }}")
        
        reader.close()
        
        // Read raw bytes to see what's actually in the file
        val bytes = outputFile.readBytes()
        println("\nFile hex dump (first 200 bytes):")
        println(bytes.take(200).joinToString(" ") { "%02X".format(it) })
    }
}
