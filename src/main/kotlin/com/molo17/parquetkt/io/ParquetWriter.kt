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
import com.molo17.parquetkt.encoding.LevelEncoder
import com.molo17.parquetkt.format.BinaryWriter
import com.molo17.parquetkt.format.ParquetConstants
import com.molo17.parquetkt.schema.*
import com.molo17.parquetkt.statistics.StatisticsCalculator
import com.molo17.parquetkt.thrift.*
import com.molo17.parquetkt.util.ArrayPool
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

class ParquetWriter(
    private val outputPath: String,
    private val schema: ParquetSchema,
    private val compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
    private val enableDictionary: Boolean = true,  // Enabled by default for better compression on repetitive data
    private val bufferSize: Int = DEFAULT_BUFFER_SIZE,
    private val enableParallelCompression: Boolean = true,
    private val maxRowGroupsInMemory: Int = DEFAULT_MAX_ROW_GROUPS_IN_MEMORY,  // Auto-flush after this many row groups
    private val maxRowsInMemory: Int = DEFAULT_MAX_ROWS_IN_MEMORY,  // Auto-flush if row count threshold is reached
    private val maxBufferedBytes: Long = DEFAULT_MAX_BUFFERED_BYTES,  // Flush when buffered raw data exceeds this size
    private val minFreeMemoryBytes: Long = defaultMinFreeMemoryBytes(),  // Flush eagerly when free heap drops below this (default: 30% of -Xmx)
    private val arrayPool: ArrayPool? = null,  // Optional array pool to reduce GC pressure
    private val enableStrictWriteValidation: Boolean = false // Extra decompression guard; useful for diagnostics, slower
) : Closeable {
    
    private val outputStream: OutputStream = BufferedOutputStream(FileOutputStream(outputPath), bufferSize)
    private var isClosed = false
    private val rowGroups = mutableListOf<RowGroup>()
    private var bufferedRowCount = 0
    private var bufferedByteEstimate = 0L
    private var isHeaderWritten = false
    private var currentFileOffset = ParquetConstants.MAGIC_LENGTH.toLong()
    private val writtenRowGroupMetadata = mutableListOf<com.molo17.parquetkt.thrift.RowGroup>()
    
    // Use provided pool or shared instance if pooling is desired
    private val effectiveArrayPool: ArrayPool? = arrayPool

    // Reused across column writes to avoid repeated large allocations
    private val pageDataOutput = ByteArrayOutputStream(256 * 1024)

    fun write(rowGroup: RowGroup) {
        checkNotClosed()
        require(rowGroup.schema == schema) {
            "RowGroup schema must match writer schema"
        }
        // Pre-add: flush if free heap is already below the safe threshold
        if (shouldFlushForMemory()) {
            flushRowGroups()
        }

        rowGroups.add(rowGroup)
        bufferedRowCount += rowGroup.rowCount
        bufferedByteEstimate += estimateRowGroupBytes(rowGroup)

        // Post-add: flush if any threshold is now exceeded.
        // The byte budget is the primary guard against concurrent writers — each writer
        // independently caps its own in-memory footprint regardless of heap sampling races.
        if (rowGroups.size >= maxRowGroupsInMemory
            || bufferedRowCount >= maxRowsInMemory
            || bufferedByteEstimate >= maxBufferedBytes
            || shouldFlushForMemory()) {
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
        bufferedRowCount = 0
        bufferedByteEstimate = 0L
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
        // Dynamic fallback: even if parallel compression was enabled at construction,
        // fall back to sequential when heap pressure is high.  Parallel compression
        // materializes all compressed columns in memory before writing any of them;
        // under pressure this doubles peak memory and risks OOM.
        val useParallel = enableParallelCompression
                && rowGroup.columnCount > 1
                && !isMemoryPressureHigh()
        return if (useParallel) {
            writeRowGroupParallel(writer, rowGroup, startOffset)
        } else {
            writeRowGroupSequential(writer, rowGroup, startOffset)
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
                async(Dispatchers.IO) {
                    val column = rowGroup.getColumn(i)
                    val field = schema.getField(i)
                    compressionSemaphore.withPermit {
                        prepareColumnChunkData(column, field)
                    }
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
            tryDictionaryEncoding(column.definedData as Array<Any>, field.dataType, field.length)
        } else {
            val encoder = PlainEncoder(field.dataType, field.length)
            Triple(encoder.encode(column.definedData as Array<Any>), Encoding.PLAIN, null)
        }
        
        // Prepare uncompressed page data — reuse writer-scoped buffer to avoid repeated allocation
        pageDataOutput.reset()
        val pageWriter = BinaryWriter(pageDataOutput)
        
        // Write repetition levels if present (for nested types)
        if (column.repetitionLevels != null && field.maxRepetitionLevel > 0) {
            val bitWidth = LevelEncoder.getMaxBitWidth(maxOf(1, field.maxRepetitionLevel))
            val encodedRepLevels = RleEncoder(bitWidth).encode(column.repetitionLevels)
            pageWriter.writeInt32(encodedRepLevels.size)
            pageWriter.writeBytes(encodedRepLevels)
        }

        // Write definition levels if present (for nested types) or if nullable
        if (column.definitionLevels != null) {
            // Use provided definition levels (for nested types)
            val bitWidth = LevelEncoder.getMaxBitWidth(maxOf(1, field.maxDefinitionLevel))
            val encodedDefLevels = RleEncoder(bitWidth).encode(column.definitionLevels)
            pageWriter.writeInt32(encodedDefLevels.size)
            pageWriter.writeBytes(encodedDefLevels)
        } else if (field.isNullable) {
            // Generate definition levels for simple nullable fields.
            val raw = column.rawData
            val levels = IntArray(raw.size) { if (raw[it] == null) 0 else 1 }
            val encodedDefLevels = RleEncoder(1).encode(levels)
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

        validateEncodedPageOrThrow(
            field = field,
            rowCount = column.size,
            definedCount = column.definedData.size,
            encodedValuesSize = encodedData.size,
            uncompressedPageData = uncompressedPageData,
            compressedPageData = compressedData,
            hasDictionary = dictionaryPageData != null
        )
        
        var dictionaryPageSizeWithHeader = 0L

        // Write dictionary page if present
        if (dictionaryPageData != null) {
            val compressor = CompressionCodecFactory.getCompressor(compressionCodec)
            val compressedDictionaryData = compressor.compress(dictionaryPageData.data)

            val dictPageHeader = DictionaryPageHeader(
                numValues = dictionaryPageData.numValues,
                encoding = Encoding.PLAIN
            )
            val dictPageHeaderBytes = serializePageHeader(PageHeader(
                type = PageType.DICTIONARY_PAGE,
                uncompressedPageSize = dictionaryPageData.data.size,
                compressedPageSize = compressedDictionaryData.size,
                dictionaryPageHeader = dictPageHeader
            ))
            writer.writeBytes(dictPageHeaderBytes)
            currentOffset += dictPageHeaderBytes.size
            writer.writeBytes(compressedDictionaryData)
            currentOffset += compressedDictionaryData.size
            dictionaryPageSizeWithHeader = dictPageHeaderBytes.size.toLong() + compressedDictionaryData.size
        }
        
        val numValues = column.definitionLevels?.size ?: column.size

        // Write DATA_PAGE (V1) header
        val pageHeader = DataPageHeader(
            numValues = numValues,
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
        
        val dataPageCompressedSizeWithHeader = pageHeaderBytes.size.toLong() + totalCompressedSize
        val dataPageUncompressedSizeWithHeader = pageHeaderBytes.size.toLong() + totalUncompressedSize
        val totalCompressedSizeWithHeader = dictionaryPageSizeWithHeader + dataPageCompressedSizeWithHeader
        val totalUncompressedSizeWithHeader = dictionaryPageSizeWithHeader + dataPageUncompressedSizeWithHeader

        val encodings = mutableListOf(Encoding.RLE)
        if (dictionaryPageData != null) {
            // Dictionary-encoded chunks use PLAIN for dictionary page values and
            // RLE_DICTIONARY for data page indices.
            encodings.add(Encoding.PLAIN)
            encodings.add(Encoding.RLE_DICTIONARY)
        } else {
            encodings.add(encoding)
        }
        
        val columnMetadata = ColumnMetaData(
            type = field.dataType,
            encodings = encodings,
            pathInSchema = columnPathInSchema(field),
            codec = compressionCodec,
            numValues = numValues.toLong(),
            totalUncompressedSize = totalUncompressedSizeWithHeader,
            totalCompressedSize = totalCompressedSizeWithHeader,
            dataPageOffset = if (dictionaryPageData != null) currentOffset - (pageHeaderBytes.size + totalCompressedSize) else dataPageOffset,
            dictionaryPageOffset = if (dictionaryPageData != null) startOffset else null
        )
        
        val columnChunk = ColumnChunk(
            fileOffset = startOffset,
            metaData = columnMetadata
        )
        
        return columnChunk to totalSize
    }
    
    private data class PreparedColumnData(
        val compressedData: ByteArray,
        val uncompressedSize: Int,
        val encoding: Encoding,
        val dictionaryPageData: DictionaryPageData?,
        val columnSize: Int,
        val statistics: Statistics?
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
            tryDictionaryEncoding(column.definedData as Array<Any>, field.dataType, field.length)
        } else {
            val encoder = PlainEncoder(field.dataType, field.length)
            Triple(encoder.encode(column.definedData as Array<Any>), Encoding.PLAIN, null)
        }
        
        // Prepare uncompressed page data — use a local buffer because this method runs in parallel
        // coroutines; sharing the instance-level pageDataOutput would cause concurrent corruption.
        val localPageOutput = ByteArrayOutputStream(64 * 1024)
        val pageWriter = BinaryWriter(localPageOutput)
        
        // Write repetition levels if present
        if (column.repetitionLevels != null && field.maxRepetitionLevel > 0) {
            val bitWidth = LevelEncoder.getMaxBitWidth(maxOf(1, field.maxRepetitionLevel))
            val encodedRepLevels = RleEncoder(bitWidth).encode(column.repetitionLevels)
            pageWriter.writeInt32(encodedRepLevels.size)
            pageWriter.writeBytes(encodedRepLevels)
        }

        // Write definition levels if present
        if (column.definitionLevels != null) {
            val bitWidth = LevelEncoder.getMaxBitWidth(maxOf(1, field.maxDefinitionLevel))
            val encodedDefLevels = RleEncoder(bitWidth).encode(column.definitionLevels)
            pageWriter.writeInt32(encodedDefLevels.size)
            pageWriter.writeBytes(encodedDefLevels)
        } else if (field.isNullable) {
            val raw = column.rawData
            val levels = IntArray(raw.size) { if (raw[it] == null) 0 else 1 }
            val encodedDefLevels = RleEncoder(1).encode(levels)
            pageWriter.writeInt32(encodedDefLevels.size)
            pageWriter.writeBytes(encodedDefLevels)
        }
        
        // Write encoded values
        pageWriter.writeBytes(encodedData)
        pageWriter.flush()
        
        val uncompressedPageData = localPageOutput.toByteArray()
        
        // Calculate statistics for the column — use rawData to avoid reflection
        val statistics = StatisticsCalculator.calculate(field.dataType, column.rawData)
        
        // Compress the entire page (levels + data)
        val compressor = CompressionCodecFactory.getCompressor(compressionCodec)
        val compressedData = compressor.compress(uncompressedPageData)

        validateEncodedPageOrThrow(
            field = field,
            rowCount = column.size,
            definedCount = column.definedData.size,
            encodedValuesSize = encodedData.size,
            uncompressedPageData = uncompressedPageData,
            compressedPageData = compressedData,
            hasDictionary = dictionaryPageData != null
        )

        return PreparedColumnData(
            compressedData = compressedData,
            uncompressedSize = uncompressedPageData.size,
            encoding = encoding,
            dictionaryPageData = dictionaryPageData,
            columnSize = column.definitionLevels?.size ?: column.size,
            statistics = statistics
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
        
        var dictionaryPageSizeWithHeader = 0L

        // Write dictionary page if present (MUST be before data page)
        if (preparedData.dictionaryPageData != null) {
            dictionaryPageOffset = currentOffset

            val compressor = CompressionCodecFactory.getCompressor(compressionCodec)
            val compressedDictionaryData = compressor.compress(preparedData.dictionaryPageData.data)

            val dictPageHeader = DictionaryPageHeader(
                numValues = preparedData.dictionaryPageData.numValues,
                encoding = Encoding.PLAIN
            )
            val dictPageHeaderBytes = serializePageHeader(PageHeader(
                type = PageType.DICTIONARY_PAGE,
                uncompressedPageSize = preparedData.dictionaryPageData.data.size,
                compressedPageSize = compressedDictionaryData.size,
                dictionaryPageHeader = dictPageHeader
            ))
            writer.writeBytes(dictPageHeaderBytes)
            currentOffset += dictPageHeaderBytes.size
            writer.writeBytes(compressedDictionaryData)
            currentOffset += compressedDictionaryData.size
            dictionaryPageSizeWithHeader = dictPageHeaderBytes.size.toLong() + compressedDictionaryData.size
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
        val dataPageCompressedSizeWithHeader = pageHeaderBytes.size.toLong() + preparedData.compressedData.size
        val dataPageUncompressedSizeWithHeader = pageHeaderBytes.size.toLong() + preparedData.uncompressedSize
        val totalCompressedSizeWithHeader = dictionaryPageSizeWithHeader + dataPageCompressedSizeWithHeader
        val totalUncompressedSizeWithHeader = dictionaryPageSizeWithHeader + dataPageUncompressedSizeWithHeader

        val encodings = mutableListOf(Encoding.RLE)
        if (preparedData.dictionaryPageData != null) {
            // Dictionary-encoded chunks use PLAIN for dictionary page values and
            // RLE_DICTIONARY for data page indices.
            encodings.add(Encoding.PLAIN)
            encodings.add(Encoding.RLE_DICTIONARY)
        } else {
            encodings.add(preparedData.encoding)
        }
        
        val columnMetadata = ColumnMetaData(
            type = field.dataType,
            encodings = encodings,
            pathInSchema = columnPathInSchema(field),
            codec = compressionCodec,
            numValues = preparedData.columnSize.toLong(),
            totalUncompressedSize = totalUncompressedSizeWithHeader,
            totalCompressedSize = totalCompressedSizeWithHeader,
            dataPageOffset = dataPageOffset,
            dictionaryPageOffset = dictionaryPageOffset,
            statistics = preparedData.statistics
        )
        
        val columnChunk = ColumnChunk(
            fileOffset = startOffset,
            metaData = columnMetadata
        )
        
        return columnChunk to totalSize
    }
    
    private fun tryDictionaryEncoding(
        values: Array<Any>,
        dataType: ParquetType,
        typeLength: Int? = null
    ): Triple<ByteArray, Encoding, DictionaryPageData?> {
        val dictEncoder = DictionaryEncoder(dataType, typeLength)
        dictEncoder.addAll(values)
        
        return if (dictEncoder.shouldUseDictionary()) {
            val dictionaryData = dictEncoder.encodeDictionary()
            val indicesData = dictEncoder.encodeIndices()
            val dictionaryPageData = DictionaryPageData(dictionaryData, dictEncoder.getDictionarySize())
            Triple(indicesData, Encoding.RLE_DICTIONARY, dictionaryPageData)
        } else {
            val plainEncoder = PlainEncoder(dataType, typeLength)
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
            createdBy = "MOLO17 ParquetKt ${resolveLibraryVersion()}"
        )
    }
    
    private fun serializeFileMetadata(metadata: FileMetaData): ByteArray {
        return ThriftSerializer.serializeFileMetadata(metadata)
    }
    
    private fun serializePageHeader(header: PageHeader): ByteArray {
        return ThriftSerializer.serializePageHeader(header)
    }

    private fun resolveLibraryVersion(): String {
        return ParquetWriter::class.java.`package`?.implementationVersion
            ?.takeIf { it.isNotBlank() }
            ?: System.getProperty("parquetkt.version")
            ?: System.getenv("RELEASE_VERSION")
            ?: "dev"
    }

    private fun validateEncodedPageOrThrow(
        field: DataField,
        rowCount: Int,
        definedCount: Int,
        encodedValuesSize: Int,
        uncompressedPageData: ByteArray,
        compressedPageData: ByteArray,
        hasDictionary: Boolean
    ) {
        val uncompressedSize = uncompressedPageData.size
        val compressedSize = compressedPageData.size

        val problems = mutableListOf<String>()
        if (rowCount > 0 && definedCount > 0 && encodedValuesSize <= 0) {
            problems += "encodedValuesSize=$encodedValuesSize with rowCount=$rowCount and definedCount=$definedCount"
        }
        if (rowCount > 0 && uncompressedSize <= 0) {
            problems += "uncompressedPageSize=$uncompressedSize with rowCount=$rowCount"
        }
        if (rowCount > 0 && compressedSize <= 0) {
            problems += "compressedPageSize=$compressedSize with rowCount=$rowCount"
        }

        if (enableStrictWriteValidation && rowCount > 0 && uncompressedSize > 0 && compressedSize > 0) {
            try {
                val compressor = CompressionCodecFactory.getCompressor(compressionCodec)
                val decoded = compressor.decompress(compressedPageData, uncompressedSize)
                if (decoded.size != uncompressedSize) {
                    problems += "decompressedSize=${decoded.size} expected=$uncompressedSize"
                }
            } catch (e: Exception) {
                problems += "decompressValidationFailed=${e::class.simpleName}:${e.message}"
            }
        }

        if (problems.isNotEmpty()) {
            val preview = compressedPageData.take(24).joinToString(" ") { "%02x".format(it) }
            val diagnostic = buildString {
                append("PARQUET_WRITE_GUARD_FAILURE")
                append(" | field=").append(field.name)
                append(" | type=").append(field.dataType)
                append(" | logicalType=").append(field.logicalType)
                append(" | codec=").append(compressionCodec)
                append(" | rowCount=").append(rowCount)
                append(" | definedCount=").append(definedCount)
                append(" | encodedValuesSize=").append(encodedValuesSize)
                append(" | uncompressedPageSize=").append(uncompressedSize)
                append(" | compressedPageSize=").append(compressedSize)
                append(" | hasDictionary=").append(hasDictionary)
                append(" | fieldPrecision=").append(field.precision)
                append(" | fieldScale=").append(field.scale)
                append(" | fieldLength=").append(field.length)
                append(" | compressedHeadHex=").append(preview)
                append(" | problems=").append(problems.joinToString(";"))
            }
            System.err.println(diagnostic)
            throw IllegalStateException(diagnostic)
        }
    }


    private fun columnPathInSchema(field: DataField): List<String> {
        return if (field.repetition == Repetition.REPEATED && field.maxRepetitionLevel > 0 && field.maxDefinitionLevel >= 2) {
            listOf(field.name, "list", "element")
        } else {
            listOf(field.name)
        }
    }


    private fun checkNotClosed() {
        check(!isClosed) { "ParquetWriter is closed" }
    }

    private fun estimateRowGroupBytes(rowGroup: RowGroup): Long {
        var bytes = 0L
        for (i in 0 until rowGroup.columnCount) {
            val col = rowGroup.getColumn(i)
            val rows = col.size.toLong()
            val bytesPerRow: Long = when (col.field.dataType) {
                ParquetType.BOOLEAN            -> 1L
                ParquetType.INT32              -> 4L
                ParquetType.INT64, ParquetType.INT96 -> 8L
                ParquetType.FLOAT              -> 4L
                ParquetType.DOUBLE             -> 8L
                ParquetType.FIXED_LEN_BYTE_ARRAY -> (col.field.length ?: 16).toLong()
                ParquetType.BYTE_ARRAY         -> {
                    // Sample first non-null value for variable-length estimate
                    val sample = col.rawData.firstOrNull { it != null }
                    when (sample) {
                        is ByteArray -> sample.size.toLong() + 4L
                        is String    -> sample.length.toLong() * 2L
                        else         -> 64L // conservative fallback
                    }
                }
            }
            bytes += rows * bytesPerRow
        }
        return bytes
    }

    /**
     * Pure heap pressure check — returns true when free heap drops below the configured
     * safety threshold.  Used for both flush decisions and parallel compression fallback.
     *
     * Note: this samples the JVM's view of free memory at a single point in time.
     * Under high concurrency, multiple writers may all sample "enough free" simultaneously
     * and then allocate together.  The per-writer byte budget ([maxBufferedBytes]) is the
     * primary guard against that race; this check is a secondary safety net.
     */
    private fun isMemoryPressureHigh(): Boolean {
        val rt = Runtime.getRuntime()
        val freeBytes = rt.maxMemory() - (rt.totalMemory() - rt.freeMemory())
        return freeBytes < minFreeMemoryBytes
    }

    /**
     * Whether the writer should flush its buffered row groups due to memory pressure.
     * Returns false when nothing is buffered (nothing to flush).
     */
    private fun shouldFlushForMemory(): Boolean {
        return rowGroups.isNotEmpty() && isMemoryPressureHigh()
    }
    
    companion object {
        // Process-wide cap on concurrent JNI compression calls.
        // Each JNI call holds the GCLocker; unbounded concurrency across many writers
        // starves the GC, causing "Retried waiting for GCLocker too often" under load.
        private val compressionSemaphore = Semaphore(Runtime.getRuntime().availableProcessors())

        const val DEFAULT_ROW_GROUP_SIZE = 8 * 1024 * 1024 // 8 MB - reduced from 128 MB to prevent OOM
        const val DEFAULT_PAGE_SIZE = 256 * 1024 // 256 KB - reduced from 1 MB for better memory efficiency
        const val DEFAULT_BUFFER_SIZE = 64 * 1024 // 64 KB buffer for I/O operations
        const val DEFAULT_MAX_ROW_GROUPS_IN_MEMORY = 10 // Auto-flush after 10 row groups (~80 MB with default size)
        const val DEFAULT_MAX_ROWS_IN_MEMORY = 200_000 // ~25MB default dataset assuming ~8 bytes per cell
        const val DEFAULT_MAX_BUFFERED_BYTES = 32L * 1024 * 1024 // 32 MB per writer before forced flush — keeps concurrent writers from compounding heap

        // Low memory mode constants - more aggressive flushing for high-throughput scenarios
        const val LOW_MEMORY_MAX_ROW_GROUPS = 1 // Flush immediately after each row group
        const val LOW_MEMORY_MAX_ROWS = 10_000 // Flush after 10K rows
        const val LOW_MEMORY_MAX_BUFFERED_BYTES = 16L * 1024 * 1024 // 16 MB per writer in low-memory mode

        fun defaultMinFreeMemoryBytes(): Long = Runtime.getRuntime().maxMemory() * 3 / 10 // 30% of -Xmx; handles concurrent writers
        fun lowMemoryMinFreeBytes(): Long = Runtime.getRuntime().maxMemory() / 3 // 33% of -Xmx for aggressive flushing
        
        fun create(
            file: File,
            schema: ParquetSchema,
            compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
            enableDictionary: Boolean = false,
            minFreeMemoryBytes: Long = defaultMinFreeMemoryBytes()
        ): ParquetWriter = ParquetWriter(
            outputPath = file.absolutePath,
            schema = schema,
            compressionCodec = compressionCodec,
            enableDictionary = enableDictionary,
            minFreeMemoryBytes = minFreeMemoryBytes
        )
        
        fun create(
            path: String,
            schema: ParquetSchema,
            compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
            enableDictionary: Boolean = false,
            minFreeMemoryBytes: Long = defaultMinFreeMemoryBytes()
        ): ParquetWriter = ParquetWriter(
            outputPath = path,
            schema = schema,
            compressionCodec = compressionCodec,
            enableDictionary = enableDictionary,
            minFreeMemoryBytes = minFreeMemoryBytes
        )
        
        /**
         * Create a ParquetWriter optimized for low-memory, high-throughput scenarios.
         * This mode:
         * - Flushes row groups immediately (maxRowGroupsInMemory = 1)
         * - Uses smaller row batches (maxRowsInMemory = 10,000)
         * - Triggers memory pressure flush at 33% free heap instead of 20%
         * - Disables parallel compression to reduce peak memory
         * - Uses shared ArrayPool to reduce GC pressure
         */
        fun createLowMemory(
            file: File,
            schema: ParquetSchema,
            compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
            enableDictionary: Boolean = false,
            enableStrictWriteValidation: Boolean = false
        ): ParquetWriter = ParquetWriter(
            outputPath = file.absolutePath,
            schema = schema,
            compressionCodec = compressionCodec,
            enableDictionary = enableDictionary,
            enableParallelCompression = false,
            maxRowGroupsInMemory = LOW_MEMORY_MAX_ROW_GROUPS,
            maxRowsInMemory = LOW_MEMORY_MAX_ROWS,
            maxBufferedBytes = LOW_MEMORY_MAX_BUFFERED_BYTES,
            minFreeMemoryBytes = lowMemoryMinFreeBytes(),
            arrayPool = ArrayPool.shared,
            enableStrictWriteValidation = enableStrictWriteValidation
        )
        
        /**
         * Create a ParquetWriter optimized for low-memory, high-throughput scenarios.
         */
        fun createLowMemory(
            path: String,
            schema: ParquetSchema,
            compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
            enableDictionary: Boolean = false,
            enableStrictWriteValidation: Boolean = false
        ): ParquetWriter = ParquetWriter(
            outputPath = path,
            schema = schema,
            compressionCodec = compressionCodec,
            enableDictionary = enableDictionary,
            enableParallelCompression = false,
            maxRowGroupsInMemory = LOW_MEMORY_MAX_ROW_GROUPS,
            maxRowsInMemory = LOW_MEMORY_MAX_ROWS,
            maxBufferedBytes = LOW_MEMORY_MAX_BUFFERED_BYTES,
            minFreeMemoryBytes = lowMemoryMinFreeBytes(),
            arrayPool = ArrayPool.shared,
            enableStrictWriteValidation = enableStrictWriteValidation
        )
        
        /**
         * Create a ParquetWriter with automatic memory-adaptive settings.
         * This constructor analyzes **both** the current JVM heap pressure and the static
         * heap configuration + schema complexity to select optimal settings:
         *
         * Priority 1 — Runtime pressure override:
         *   If free heap is currently below 40% of maxMemory, or available processors < 4,
         *   low-memory mode is forced regardless of the static tier.  This handles the case
         *   where many concurrent writers have already consumed most of the heap by the time
         *   a new writer is created.
         *
         * Priority 2 — Static tier selection:
         *   - For heaps < 512MB or schemas with > 100 columns: aggressive low-memory settings
         *   - For heaps 512MB-2GB: balanced settings with moderate buffering
         *   - For heaps > 2GB: standard settings with larger buffers for throughput
         *
         * NOTE: Even after construction, the writer dynamically falls back to sequential
         * compression when heap pressure is detected at write time (see [writeRowGroupToFile]).
         *
         * This is the recommended constructor for production use where memory constraints vary.
         */
        fun createAuto(
            file: File,
            schema: ParquetSchema,
            compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
            enableDictionary: Boolean = false,
            enableStrictWriteValidation: Boolean = false
        ): ParquetWriter {
            val rt = Runtime.getRuntime()
            val maxHeapMb = rt.maxMemory() / (1024 * 1024)
            val columnCount = schema.fieldCount
            val freeRatio = (rt.maxMemory() - (rt.totalMemory() - rt.freeMemory())).toDouble() / rt.maxMemory()

            // Priority 1: if heap is already under pressure at construction time, force low-memory
            if (freeRatio < 0.40 || rt.availableProcessors() < 4) {
                return createLowMemory(file, schema, compressionCodec, enableDictionary, enableStrictWriteValidation)
            }
            
            // Priority 2: static tier selection based on maxMemory and schema width
            val estimatedBytesPerRow = columnCount * 50L // ~50 bytes per column average
            
            return when {
                // Low memory or many columns: aggressive flushing
                maxHeapMb < 512 || columnCount > 100 -> ParquetWriter(
                    outputPath = file.absolutePath,
                    schema = schema,
                    compressionCodec = compressionCodec,
                    enableDictionary = enableDictionary,
                    enableParallelCompression = false,
                    maxRowGroupsInMemory = 1,
                    maxRowsInMemory = (50 * 1024 * 1024 / estimatedBytesPerRow).toInt().coerceIn(1_000, 50_000),
                    minFreeMemoryBytes = rt.maxMemory() / 3, // 33% free
                    arrayPool = ArrayPool.shared,
                    enableStrictWriteValidation = enableStrictWriteValidation
                )
                // Medium memory: balanced settings
                maxHeapMb < 2048 -> ParquetWriter(
                    outputPath = file.absolutePath,
                    schema = schema,
                    compressionCodec = compressionCodec,
                    enableDictionary = enableDictionary,
                    enableParallelCompression = columnCount <= 50, // Only parallelize for simpler schemas
                    maxRowGroupsInMemory = 3,
                    maxRowsInMemory = (100 * 1024 * 1024 / estimatedBytesPerRow).toInt().coerceIn(10_000, 100_000),
                    minFreeMemoryBytes = rt.maxMemory() / 4, // 25% free
                    arrayPool = ArrayPool.shared,
                    enableStrictWriteValidation = enableStrictWriteValidation
                )
                // High memory: optimize for throughput
                else -> ParquetWriter(
                    outputPath = file.absolutePath,
                    schema = schema,
                    compressionCodec = compressionCodec,
                    enableDictionary = enableDictionary,
                    enableParallelCompression = true,
                    maxRowGroupsInMemory = DEFAULT_MAX_ROW_GROUPS_IN_MEMORY,
                    maxRowsInMemory = DEFAULT_MAX_ROWS_IN_MEMORY,
                    minFreeMemoryBytes = defaultMinFreeMemoryBytes(),
                    arrayPool = ArrayPool.shared,
                    enableStrictWriteValidation = enableStrictWriteValidation
                )
            }
        }
        
        /**
         * Create a ParquetWriter with automatic memory-adaptive settings.
         */
        fun createAuto(
            path: String,
            schema: ParquetSchema,
            compressionCodec: CompressionCodec = CompressionCodec.SNAPPY,
            enableDictionary: Boolean = false,
            enableStrictWriteValidation: Boolean = false
        ): ParquetWriter = createAuto(File(path), schema, compressionCodec, enableDictionary, enableStrictWriteValidation)
    }
}
