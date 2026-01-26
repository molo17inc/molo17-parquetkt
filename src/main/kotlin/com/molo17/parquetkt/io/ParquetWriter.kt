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


package com.molo17.parquetkt.io

import com.molo17.parquetkt.compression.CompressionCodecFactory
import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.encoding.PlainEncoder
import com.molo17.parquetkt.encoding.RleEncoder
import com.molo17.parquetkt.format.BinaryWriter
import com.molo17.parquetkt.format.ParquetConstants
import com.molo17.parquetkt.schema.*
import com.molo17.parquetkt.thrift.*
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream

class ParquetWriter(
    private val outputPath: String,
    private val schema: ParquetSchema,
    private val compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
    private val rowGroupSize: Int = DEFAULT_ROW_GROUP_SIZE,
    private val pageSize: Int = DEFAULT_PAGE_SIZE,
    private val enableDictionary: Boolean = true
) : Closeable {
    
    private val outputStream: OutputStream = FileOutputStream(outputPath)
    private var isClosed = false
    private val rowGroups = mutableListOf<RowGroup>()
    
    fun write(rowGroup: RowGroup) {
        checkNotClosed()
        require(rowGroup.schema == schema) {
            "RowGroup schema must match writer schema"
        }
        rowGroups.add(rowGroup)
    }
    
    fun writeRowGroup(columns: List<DataColumn<*>>) {
        checkNotClosed()
        val rowGroup = RowGroup(schema, columns)
        write(rowGroup)
    }
    
    fun <T> writeColumn(field: String, data: List<T?>) {
        checkNotClosed()
        val dataField = schema.getField(field) 
            ?: throw IllegalArgumentException("Field $field not found in schema")
        
        val column = DataColumn.create(dataField, data)
        
        if (rowGroups.isEmpty()) {
            rowGroups.add(RowGroup(schema, listOf(column)))
        }
    }
    
    override fun close() {
        if (isClosed) return
        
        try {
            flush()
        } finally {
            outputStream.close()
            isClosed = true
        }
    }
    
    private fun flush() {
        if (rowGroups.isEmpty()) return
        
        val writer = BinaryWriter(outputStream)
        
        // 1. Write magic number "PAR1"
        writer.writeBytes(ParquetConstants.MAGIC_BYTES)
        writer.flush()
        
        // 2. Write row groups and collect metadata
        val rowGroupMetadata = ArrayList<com.molo17.parquetkt.thrift.RowGroup>(rowGroups.size)
        var currentOffset = ParquetConstants.MAGIC_LENGTH.toLong()
        
        for (rowGroup in rowGroups) {
            val (metadata, bytesWritten) = writeRowGroupToFile(writer, rowGroup, currentOffset)
            rowGroupMetadata.add(metadata)
            currentOffset += bytesWritten
            writer.flush() // Flush after each row group
        }
        
        // 3. Write file metadata
        val metadataOffset = currentOffset
        val fileMetadata = createFileMetadata(rowGroupMetadata)
        val metadataBytes = serializeFileMetadata(fileMetadata)
        writer.writeBytes(metadataBytes)
        
        // 4. Write footer with metadata length
        writer.writeInt32(metadataBytes.size)
        
        // 5. Write magic number "PAR1" at end
        writer.writeBytes(ParquetConstants.MAGIC_BYTES)
        
        writer.flush()
    }
    
    private fun writeRowGroupToFile(
        writer: BinaryWriter,
        rowGroup: RowGroup,
        startOffset: Long
    ): Pair<com.molo17.parquetkt.thrift.RowGroup, Long> {
        val columnChunks = mutableListOf<ColumnChunk>()
        var currentOffset = startOffset
        var totalByteSize = 0L
        
        for (i in 0 until rowGroup.columnCount) {
            val column = rowGroup.getColumn(i)
            val field = schema.getField(i)
            
            val (columnChunk, bytesWritten) = writeColumnChunk(writer, column, field, currentOffset)
            columnChunks.add(columnChunk)
            currentOffset += bytesWritten
            totalByteSize += bytesWritten
        }
        
        val rowGroupMetadata = com.molo17.parquetkt.thrift.RowGroup(
            columns = columnChunks,
            totalByteSize = totalByteSize,
            numRows = rowGroup.rowCount.toLong(),
            fileOffset = startOffset
        )
        
        return rowGroupMetadata to totalByteSize
    }
    
    private fun writeColumnChunk(
        writer: BinaryWriter,
        column: DataColumn<*>,
        field: DataField,
        startOffset: Long
    ): Pair<ColumnChunk, Long> {
        val dataPageOffset = startOffset
        var currentOffset = startOffset
        
        // Encode and compress data
        val encoder = PlainEncoder(field.dataType)
        val encodedData = encoder.encode(column.definedData as Array<Any>)
        
        val compressor = CompressionCodecFactory.getCompressor(compressionCodec)
        val compressedData = compressor.compress(encodedData)
        
        // Use DATA_PAGE (V1) format for simplicity and better compatibility
        // V1 format: everything (levels + data) is compressed together
        val totalUncompressedSize = encodedData.size
        val totalCompressedSize = compressedData.size
        
        // Write DATA_PAGE (V1) header
        val pageHeader = DataPageHeader(
            numValues = column.size,
            encoding = Encoding.PLAIN,
            definitionLevelEncoding = Encoding.RLE,
            repetitionLevelEncoding = Encoding.RLE
        )
        
        val pageHeaderBytes = serializePageHeader(PageHeader(
            type = PageType.DATA_PAGE,
            uncompressedPageSize = totalUncompressedSize,
            compressedPageSize = totalCompressedSize,
            dataPageHeader = pageHeader
        ))
        
        writer.writeBytes(pageHeaderBytes)
        currentOffset += pageHeaderBytes.size
        
        // Write compressed data
        writer.writeBytes(compressedData)
        currentOffset += compressedData.size
        
        // Flush to ensure all data is written
        writer.flush()
        
        val totalSize = currentOffset - startOffset
        
        // total_compressed_size should include the page header size
        val totalCompressedSizeWithHeader = pageHeaderBytes.size.toLong() + totalCompressedSize
        
        val columnMetadata = ColumnMetaData(
            type = field.dataType,
            encodings = listOf(Encoding.RLE, Encoding.PLAIN),  // RLE for levels, PLAIN for data
            pathInSchema = listOf(field.name),
            codec = compressionCodec,
            numValues = column.size.toLong(),
            totalUncompressedSize = encodedData.size.toLong(),
            totalCompressedSize = totalCompressedSizeWithHeader,
            dataPageOffset = dataPageOffset
        )
        
        val columnChunk = ColumnChunk(
            fileOffset = 0,  // Deprecated field, should be 0
            metaData = columnMetadata
        )
        
        return columnChunk to totalSize
    }
    
    private fun createFileMetadata(rowGroups: List<com.molo17.parquetkt.thrift.RowGroup>): FileMetaData {
        val schemaElements = SchemaConverter.toThriftSchema(schema)
        val totalRows = rowGroups.sumOf { it.numRows }
        
        return FileMetaData(
            version = ParquetConstants.VERSION,
            schema = schemaElements,
            numRows = totalRows,
            rowGroups = rowGroups,
            createdBy = "MOLO17 ParquetKt 1.0.0"
        )
    }
    
    private fun serializeFileMetadata(metadata: FileMetaData): ByteArray {
        return ThriftSerializer.serializeFileMetadata(metadata)
    }
    
    private fun serializePageHeader(header: PageHeader): ByteArray {
        return ThriftSerializer.serializePageHeader(header)
    }
    
    private fun checkNotClosed() {
        check(!isClosed) { "ParquetWriter is closed" }
    }
    
    companion object {
        const val DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024 // 128 MB
        const val DEFAULT_PAGE_SIZE = 1024 * 1024 // 1 MB
        
        fun create(
            file: File,
            schema: ParquetSchema,
            compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
        ): ParquetWriter {
            return ParquetWriter(file.absolutePath, schema, compressionCodec)
        }
        
        fun create(
            path: String,
            schema: ParquetSchema,
            compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
        ): ParquetWriter {
            return ParquetWriter(path, schema, compressionCodec)
        }
    }
}
