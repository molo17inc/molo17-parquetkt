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
import com.molo17.parquetkt.encoding.DictionaryEncoder
import com.molo17.parquetkt.encoding.PlainEncoder
import com.molo17.parquetkt.encoding.RleEncoder
import com.molo17.parquetkt.format.BinaryWriter
import com.molo17.parquetkt.format.ParquetConstants
import com.molo17.parquetkt.schema.*
import com.molo17.parquetkt.thrift.*
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import kotlinx.coroutines.*

class ParquetWriter(
    private val outputPath: String,
    private val schema: ParquetSchema,
    private val compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
    private val rowGroupSize: Int = DEFAULT_ROW_GROUP_SIZE,
    private val pageSize: Int = DEFAULT_PAGE_SIZE,
    private val enableDictionary: Boolean = true,  // Enabled by default for better compression on repetitive data
    private val bufferSize: Int = DEFAULT_BUFFER_SIZE,
    private val enableParallelCompression: Boolean = true,
    private val maxRowGroupsInMemory: Int = DEFAULT_MAX_ROW_GROUPS_IN_MEMORY  // Auto-flush after this many row groups
) : Closeable {
    
    private val outputStream: OutputStream = BufferedOutputStream(FileOutputStream(outputPath), bufferSize)
    private var isClosed = false
    private val rowGroups = mutableListOf<RowGroup>()
    private var isHeaderWritten = false
    private var currentFileOffset = ParquetConstants.MAGIC_LENGTH.toLong()
    private val writtenRowGroupMetadata = mutableListOf<com.molo17.parquetkt.thrift.RowGroup>()
    
    fun write(rowGroup: RowGroup) {
        checkNotClosed()
        require(rowGroup.schema == schema) {
            "RowGroup schema must match writer schema"
        }
        rowGroups.add(rowGroup)
        
        // Auto-flush if we have too many row groups in memory
        if (rowGroups.size >= maxRowGroupsInMemory) {
            flushRowGroups()
        }
    }
    
    fun writeRowGroup(columns: List<DataColumn<*>>) {
        checkNotClosed()
        val rowGroup = RowGroup(schema, columns)
        write(rowGroup)
    }
    
    /**
     * Manually flush accumulated row groups to disk.
     * This is useful for controlling memory usage when writing large datasets.
     * Row groups are automatically flushed when maxRowGroupsInMemory is reached.
     */
    fun flushRowGroups() {
        if (rowGroups.isEmpty()) return
        
        val writer = BinaryWriter(outputStream)
        
        // Write header on first flush
        if (!isHeaderWritten) {
            writer.writeBytes(ParquetConstants.MAGIC_BYTES)
            writer.flush()
            isHeaderWritten = true
        }
        
        // Write accumulated row groups
        for (rowGroup in rowGroups) {
            val (metadata, bytesWritten) = writeRowGroupToFile(writer, rowGroup, currentFileOffset)
            writtenRowGroupMetadata.add(metadata)
            currentFileOffset += bytesWritten
            writer.flush()
        }
        
        // Clear row groups from memory
        rowGroups.clear()
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
        val writer = BinaryWriter(outputStream)
        
        // Flush any remaining row groups
        flushRowGroups()
        
        // If no row groups were written at all, write header now
        if (!isHeaderWritten) {
            writer.writeBytes(ParquetConstants.MAGIC_BYTES)
            writer.flush()
            isHeaderWritten = true
            currentFileOffset = ParquetConstants.MAGIC_LENGTH.toLong()
        }
        
        // Write file metadata using all written row groups
        val fileMetadata = createFileMetadata(writtenRowGroupMetadata)
        val metadataBytes = serializeFileMetadata(fileMetadata)
        writer.writeBytes(metadataBytes)
        
        // Write footer with metadata length
        writer.writeInt32(metadataBytes.size)
        
        // Write magic number "PAR1" at end
        writer.writeBytes(ParquetConstants.MAGIC_BYTES)
        
        writer.flush()
    }
    
    private fun writeRowGroupToFile(
        writer: BinaryWriter,
        rowGroup: RowGroup,
        startOffset: Long
    ): Pair<com.molo17.parquetkt.thrift.RowGroup, Long> {
        if (enableParallelCompression && rowGroup.columnCount > 1) {
            return writeRowGroupParallel(writer, rowGroup, startOffset)
        } else {
            return writeRowGroupSequential(writer, rowGroup, startOffset)
        }
    }
    
    private fun writeRowGroupSequential(
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
    
    private fun writeRowGroupParallel(
        writer: BinaryWriter,
        rowGroup: RowGroup,
        startOffset: Long
    ): Pair<com.molo17.parquetkt.thrift.RowGroup, Long> {
        // Prepare column data in parallel
        val compressedColumns = runBlocking {
            (0 until rowGroup.columnCount).map { i ->
                async(Dispatchers.Default) {
                    val column = rowGroup.getColumn(i)
                    val field = schema.getField(i)
                    prepareColumnChunkData(column, field)
                }
            }.awaitAll()
        }
        
        // Write compressed data sequentially to maintain file structure
        val columnChunks = mutableListOf<ColumnChunk>()
        var currentOffset = startOffset
        var totalByteSize = 0L
        
        for (i in compressedColumns.indices) {
            val field = schema.getField(i)
            val preparedData = compressedColumns[i]
            val (columnChunk, bytesWritten) = writeColumnChunkData(writer, preparedData, field, currentOffset)
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
        
        // Try dictionary encoding for string/byte array columns if enabled
        // Disable for nested/list columns (with repetition levels) for now
        val useDictionary = enableDictionary && 
                           DictionaryEncoder.canUseDictionary(field.dataType) &&
                           column.definedData.isNotEmpty() &&
                           column.repetitionLevels == null
        
        val (encodedData, encoding, dictionaryPageData) = if (useDictionary) {
            tryDictionaryEncoding(column.definedData as Array<Any>, field.dataType)
        } else {
            val encoder = PlainEncoder(field.dataType)
            Triple(encoder.encode(column.definedData as Array<Any>), Encoding.PLAIN, null)
        }
        
        // Prepare uncompressed page data
        val pageDataOutput = ByteArrayOutputStream()
        val pageWriter = BinaryWriter(pageDataOutput)
        
        // Write repetition levels if present (for nested types)
        if (column.repetitionLevels != null && field.maxRepetitionLevel > 0) {
            val encodedRepLevels = com.molo17.parquetkt.encoding.LevelEncoder.encodeLevels(
                column.repetitionLevels.toList()
            )
            pageWriter.writeInt32(encodedRepLevels.size)
            pageWriter.writeBytes(encodedRepLevels)
        }
        
        // Write definition levels if present (for nested types) or if nullable
        if (column.definitionLevels != null) {
            // Use provided definition levels (for nested types)
            val encodedDefLevels = com.molo17.parquetkt.encoding.LevelEncoder.encodeLevels(
                column.definitionLevels.toList()
            )
            pageWriter.writeInt32(encodedDefLevels.size)
            pageWriter.writeBytes(encodedDefLevels)
        } else if (field.isNullable) {
            // Generate definition levels for simple nullable fields (existing behavior)
            val dataField = column.javaClass.getDeclaredField("data")
            dataField.isAccessible = true
            @Suppress("UNCHECKED_CAST")
            val data = dataField.get(column) as Array<Any?>
            val levels = IntArray(data.size)
            for (i in data.indices) {
                levels[i] = if (data[i] == null) 0 else 1
            }
            
            // Use RLE encoder for backward compatibility with existing code
            val rleEncoder = com.molo17.parquetkt.encoding.RleEncoder(1)
            val encodedDefLevels = rleEncoder.encode(levels)
            pageWriter.writeInt32(encodedDefLevels.size)
            pageWriter.writeBytes(encodedDefLevels)
        }
        
        // Write encoded values
        pageWriter.writeBytes(encodedData)
        pageWriter.flush()
        
        val uncompressedPageData = pageDataOutput.toByteArray()
        
        // Compress the entire page (levels + data)
        val compressor = CompressionCodecFactory.getCompressor(compressionCodec)
        val compressedData = compressor.compress(uncompressedPageData)
        
        val totalUncompressedSize = uncompressedPageData.size
        val totalCompressedSize = compressedData.size
        
        // Write dictionary page if present
        if (dictionaryPageData != null) {
            val dictPageHeader = DictionaryPageHeader(
                numValues = dictionaryPageData.numValues,
                encoding = Encoding.PLAIN
            )
            val dictPageHeaderBytes = serializePageHeader(PageHeader(
                type = PageType.DICTIONARY_PAGE,
                uncompressedPageSize = dictionaryPageData.data.size,
                compressedPageSize = dictionaryPageData.data.size,
                dictionaryPageHeader = dictPageHeader
            ))
            writer.writeBytes(dictPageHeaderBytes)
            currentOffset += dictPageHeaderBytes.size
            writer.writeBytes(dictionaryPageData.data)
            currentOffset += dictionaryPageData.data.size
        }
        
        // Write DATA_PAGE (V1) header
        val pageHeader = DataPageHeader(
            numValues = column.size,
            encoding = encoding,
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
        
        val encodings = mutableListOf(Encoding.RLE)
        if (dictionaryPageData != null) {
            encodings.add(Encoding.RLE_DICTIONARY)
        } else {
            encodings.add(encoding)
        }
        
        val columnMetadata = ColumnMetaData(
            type = field.dataType,
            encodings = encodings,
            pathInSchema = listOf(field.name),
            codec = compressionCodec,
            numValues = column.size.toLong(),
            totalUncompressedSize = encodedData.size.toLong(),
            totalCompressedSize = totalCompressedSizeWithHeader,
            dataPageOffset = if (dictionaryPageData != null) currentOffset - (pageHeaderBytes.size + totalCompressedSize) else dataPageOffset,
            dictionaryPageOffset = if (dictionaryPageData != null) startOffset else null
        )
        
        val columnChunk = ColumnChunk(
            fileOffset = 0,  // Deprecated field, should be 0
            metaData = columnMetadata
        )
        
        return columnChunk to totalSize
    }
    
    private data class PreparedColumnData(
        val compressedData: ByteArray,
        val uncompressedSize: Int,
        val encoding: Encoding,
        val dictionaryPageData: DictionaryPageData?,
        val columnSize: Int
    )
    
    private data class DictionaryPageData(
        val data: ByteArray,
        val numValues: Int
    )
    
    private fun prepareColumnChunkData(
        column: DataColumn<*>,
        field: DataField
    ): PreparedColumnData {
        // Try dictionary encoding for string/byte array columns if enabled
        val useDictionary = enableDictionary && 
                           DictionaryEncoder.canUseDictionary(field.dataType) &&
                           column.definedData.isNotEmpty() &&
                           column.repetitionLevels == null
        
        val (encodedData, encoding, dictionaryPageData) = if (useDictionary) {
            tryDictionaryEncoding(column.definedData as Array<Any>, field.dataType)
        } else {
            val encoder = PlainEncoder(field.dataType)
            Triple(encoder.encode(column.definedData as Array<Any>), Encoding.PLAIN, null)
        }
        
        // Prepare uncompressed page data
        val pageDataOutput = ByteArrayOutputStream()
        val pageWriter = BinaryWriter(pageDataOutput)
        
        // Write repetition levels if present
        if (column.repetitionLevels != null && field.maxRepetitionLevel > 0) {
            val encodedRepLevels = com.molo17.parquetkt.encoding.LevelEncoder.encodeLevels(
                column.repetitionLevels.toList()
            )
            pageWriter.writeInt32(encodedRepLevels.size)
            pageWriter.writeBytes(encodedRepLevels)
        }
        
        // Write definition levels if present
        if (column.definitionLevels != null) {
            val encodedDefLevels = com.molo17.parquetkt.encoding.LevelEncoder.encodeLevels(
                column.definitionLevels.toList()
            )
            pageWriter.writeInt32(encodedDefLevels.size)
            pageWriter.writeBytes(encodedDefLevels)
        } else if (field.isNullable) {
            val dataField = column.javaClass.getDeclaredField("data")
            dataField.isAccessible = true
            @Suppress("UNCHECKED_CAST")
            val data = dataField.get(column) as Array<Any?>
            val levels = IntArray(data.size)
            for (i in data.indices) {
                levels[i] = if (data[i] == null) 0 else 1
            }
            val rleEncoder = com.molo17.parquetkt.encoding.RleEncoder(1)
            val encodedDefLevels = rleEncoder.encode(levels)
            pageWriter.writeInt32(encodedDefLevels.size)
            pageWriter.writeBytes(encodedDefLevels)
        }
        
        // Write encoded values
        pageWriter.writeBytes(encodedData)
        pageWriter.flush()
        
        val uncompressedPageData = pageDataOutput.toByteArray()
        
        // Compress the entire page (levels + data)
        val compressor = CompressionCodecFactory.getCompressor(compressionCodec)
        val compressedData = compressor.compress(uncompressedPageData)
        
        return PreparedColumnData(
            compressedData = compressedData,
            uncompressedSize = uncompressedPageData.size,
            encoding = encoding,
            dictionaryPageData = dictionaryPageData,
            columnSize = column.size
        )
    }
    
    private fun writeColumnChunkData(
        writer: BinaryWriter,
        preparedData: PreparedColumnData,
        field: DataField,
        startOffset: Long
    ): Pair<ColumnChunk, Long> {
        var currentOffset = startOffset
        val dictionaryPageOffset: Long?
        val dataPageOffset: Long
        
        // Write dictionary page if present (MUST be before data page)
        if (preparedData.dictionaryPageData != null) {
            dictionaryPageOffset = currentOffset
            
            val dictPageHeader = DictionaryPageHeader(
                numValues = preparedData.dictionaryPageData.numValues,
                encoding = Encoding.PLAIN
            )
            val dictPageHeaderBytes = serializePageHeader(PageHeader(
                type = PageType.DICTIONARY_PAGE,
                uncompressedPageSize = preparedData.dictionaryPageData.data.size,
                compressedPageSize = preparedData.dictionaryPageData.data.size,
                dictionaryPageHeader = dictPageHeader
            ))
            writer.writeBytes(dictPageHeaderBytes)
            currentOffset += dictPageHeaderBytes.size
            writer.writeBytes(preparedData.dictionaryPageData.data)
            currentOffset += preparedData.dictionaryPageData.data.size
        } else {
            dictionaryPageOffset = null
        }
        
        // Record data page offset BEFORE writing it
        dataPageOffset = currentOffset
        
        // Write DATA_PAGE header
        val pageHeader = DataPageHeader(
            numValues = preparedData.columnSize,
            encoding = preparedData.encoding,
            definitionLevelEncoding = Encoding.RLE,
            repetitionLevelEncoding = Encoding.RLE
        )
        
        val pageHeaderBytes = serializePageHeader(PageHeader(
            type = PageType.DATA_PAGE,
            uncompressedPageSize = preparedData.uncompressedSize,
            compressedPageSize = preparedData.compressedData.size,
            dataPageHeader = pageHeader
        ))
        
        writer.writeBytes(pageHeaderBytes)
        currentOffset += pageHeaderBytes.size
        
        // Write compressed data
        writer.writeBytes(preparedData.compressedData)
        currentOffset += preparedData.compressedData.size
        
        // Flush to ensure all data is written
        writer.flush()
        
        val totalSize = currentOffset - startOffset
        val totalCompressedSizeWithHeader = pageHeaderBytes.size.toLong() + preparedData.compressedData.size
        
        val encodings = mutableListOf(Encoding.RLE)
        if (preparedData.dictionaryPageData != null) {
            encodings.add(Encoding.RLE_DICTIONARY)
        } else {
            encodings.add(preparedData.encoding)
        }
        
        val columnMetadata = ColumnMetaData(
            type = field.dataType,
            encodings = encodings,
            pathInSchema = listOf(field.name),
            codec = compressionCodec,
            numValues = preparedData.columnSize.toLong(),
            totalUncompressedSize = preparedData.uncompressedSize.toLong(),
            totalCompressedSize = totalCompressedSizeWithHeader,
            dataPageOffset = dataPageOffset,
            dictionaryPageOffset = dictionaryPageOffset
        )
        
        val columnChunk = ColumnChunk(
            fileOffset = 0,
            metaData = columnMetadata
        )
        
        return columnChunk to totalSize
    }
    
    private fun tryDictionaryEncoding(
        values: Array<Any>,
        dataType: ParquetType
    ): Triple<ByteArray, Encoding, DictionaryPageData?> {
        val dictEncoder = DictionaryEncoder(dataType)
        dictEncoder.addAll(values)
        
        return if (dictEncoder.shouldUseDictionary()) {
            val dictionaryData = dictEncoder.encodeDictionary()
            val indicesData = dictEncoder.encodeIndices()
            val dictionaryPageData = DictionaryPageData(dictionaryData, dictEncoder.getDictionarySize())
            Triple(indicesData, Encoding.RLE_DICTIONARY, dictionaryPageData)
        } else {
            val plainEncoder = PlainEncoder(dataType)
            Triple(plainEncoder.encode(values), Encoding.PLAIN, null)
        }
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
        const val DEFAULT_ROW_GROUP_SIZE = 8 * 1024 * 1024 // 8 MB - reduced from 128 MB to prevent OOM
        const val DEFAULT_PAGE_SIZE = 256 * 1024 // 256 KB - reduced from 1 MB for better memory efficiency
        const val DEFAULT_BUFFER_SIZE = 64 * 1024 // 64 KB buffer for I/O operations
        const val DEFAULT_MAX_ROW_GROUPS_IN_MEMORY = 10 // Auto-flush after 10 row groups (~80 MB with default size)
        
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
