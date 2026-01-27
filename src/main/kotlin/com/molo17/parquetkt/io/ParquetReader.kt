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
import java.io.EOFException
import java.io.InputStream
import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.encoding.DictionaryDecoder
import com.molo17.parquetkt.encoding.PlainDecoder
import com.molo17.parquetkt.encoding.RleDecoder
import com.molo17.parquetkt.format.BinaryReader
import com.molo17.parquetkt.format.ParquetConstants
import com.molo17.parquetkt.schema.Encoding
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.SchemaConverter
import com.molo17.parquetkt.thrift.*
import java.io.ByteArrayInputStream
import java.io.Closeable
import java.io.File
import java.io.RandomAccessFile

class ParquetReader(
    private val inputPath: String
) : Closeable {
    
    private val file: RandomAccessFile = RandomAccessFile(inputPath, "r")
    private var isClosed = false
    private var _schema: ParquetSchema? = null
    private var _rowGroupCount: Int = 0
    private var _totalRowCount: Long = 0L
    
    val schema: ParquetSchema
        get() {
            if (_schema == null) {
                initialize()
            }
            return _schema!!
        }
    
    val rowGroupCount: Int
        get() {
            initialize()
            return _rowGroupCount
        }
    
    val totalRowCount: Long
        get() {
            initialize()
            return _totalRowCount
        }
    
    private fun initialize() {
        if (_schema != null) return
        
        // 1. Verify magic number "PAR1" at start
        file.seek(0)
        val startMagic = ByteArray(ParquetConstants.MAGIC_LENGTH)
        file.readFully(startMagic)
        require(startMagic.contentEquals(ParquetConstants.MAGIC_BYTES)) {
            "Invalid Parquet file: missing magic number at start"
        }
        
        // 2. Read footer from end of file
        val fileLength = file.length()
        file.seek(fileLength - ParquetConstants.MAGIC_LENGTH - ParquetConstants.FOOTER_LENGTH_SIZE)
        
        val footerLengthBytes = ByteArray(ParquetConstants.FOOTER_LENGTH_SIZE)
        file.readFully(footerLengthBytes)
        val reader = BinaryReader(ByteArrayInputStream(footerLengthBytes))
        val footerLength = reader.readInt32()
        
        // 3. Verify magic number at end
        val endMagic = ByteArray(ParquetConstants.MAGIC_LENGTH)
        file.readFully(endMagic)
        require(endMagic.contentEquals(ParquetConstants.MAGIC_BYTES)) {
            "Invalid Parquet file: missing magic number at end"
        }
        
        // 4. Read and parse file metadata
        val metadataOffset = fileLength - ParquetConstants.MAGIC_LENGTH - 
                            ParquetConstants.FOOTER_LENGTH_SIZE - footerLength
        file.seek(metadataOffset)
        
        val metadataBytes = ByteArray(footerLength)
        file.readFully(metadataBytes)
        val fileMetadata = deserializeFileMetadata(metadataBytes)
        
        // 5. Store parsed information
        _schema = SchemaConverter.fromThriftSchema(fileMetadata.schema)
        _rowGroupCount = fileMetadata.rowGroups.size
        _totalRowCount = fileMetadata.numRows
        this.fileMetadata = fileMetadata
    }
    
    fun readRowGroup(index: Int): RowGroup {
        checkNotClosed()
        initialize()
        
        require(index in 0 until rowGroupCount) {
            "Row group index $index out of bounds [0, $rowGroupCount)"
        }
        
        val columns = readRowGroupColumns(index)
        return RowGroup(schema, columns)
    }
    
    fun readAllRowGroups(): List<RowGroup> {
        checkNotClosed()
        initialize()
        
        return (0 until rowGroupCount).map { readRowGroup(it) }
    }
    
    fun readColumn(rowGroupIndex: Int, columnName: String): DataColumn<*> {
        checkNotClosed()
        val rowGroup = readRowGroup(rowGroupIndex)
        return rowGroup.getColumn(columnName) 
            ?: throw IllegalArgumentException("Column $columnName not found")
    }
    
    fun readAllRows(): Sequence<Map<String, Any?>> = sequence {
        checkNotClosed()
        initialize()
        
        for (i in 0 until rowGroupCount) {
            val rowGroup = readRowGroup(i)
            for (j in 0 until rowGroup.rowCount) {
                yield(rowGroup.getRow(j))
            }
        }
    }
    
    override fun close() {
        if (isClosed) return
        
        try {
            file.close()
        } finally {
            isClosed = true
        }
    }
    
    private var fileMetadata: FileMetaData? = null
    
    private fun readRowGroupColumns(index: Int): List<DataColumn<*>> {
        initialize()
        val metadata = fileMetadata ?: throw IllegalStateException("File metadata not loaded")
        val rowGroupMeta = metadata.rowGroups[index]
        
        val columns = mutableListOf<DataColumn<*>>()
        
        for (i in 0 until schema.fieldCount) {
            val field = schema.getField(i)
            val columnChunk = rowGroupMeta.columns[i]
            val column = readColumnChunk(columnChunk, field)
            columns.add(column)
        }
        
        return columns
    }
    
    private fun readColumnChunk(columnChunk: ColumnChunk, field: com.molo17.parquetkt.schema.DataField): DataColumn<*> {
        val metadata = columnChunk.metaData
        
        // Check if dictionary encoding is used
        val hasDictionary = metadata.dictionaryPageOffset != null
        var dictionaryData: ByteArray? = null
        
        if (hasDictionary) {
            // Read dictionary page first
            file.seek(metadata.dictionaryPageOffset!!)
            val dictPageHeader = deserializePageHeaderFromStream(file)
            val dictCompressedData = ByteArray(dictPageHeader.compressedPageSize)
            file.readFully(dictCompressedData)
            
            // Dictionary pages are typically uncompressed, but check if compression was used
            dictionaryData = if (dictPageHeader.compressedPageSize == dictPageHeader.uncompressedPageSize) {
                dictCompressedData
            } else {
                val compressor = CompressionCodecFactory.getCompressor(metadata.codec)
                compressor.decompress(dictCompressedData, dictPageHeader.uncompressedPageSize)
            }
        }
        
        // Seek to data page
        file.seek(metadata.dataPageOffset)
        
        // Read page header directly from file stream
        val pageHeader = deserializePageHeaderFromStream(file)
        
        // Read compressed data
        val compressedData = ByteArray(pageHeader.compressedPageSize)
        file.readFully(compressedData)
        
        // Decompress
        val compressor = CompressionCodecFactory.getCompressor(metadata.codec)
        val uncompressedData = compressor.decompress(compressedData, pageHeader.uncompressedPageSize)
        
        // Parse definition levels and data from uncompressed page
        // Track position manually to know where value data starts
        var position = 0
        val reader = BinaryReader(ByteArrayInputStream(uncompressedData))
        
        // Read repetition levels if present (for nested types)
        var repetitionLevels: IntArray? = null
        
        if (field.maxRepetitionLevel > 0) {
            // Read repetition level length
            val repLevelLength = reader.readInt32()
            position += 4
            
            // Read and decode repetition levels
            val repLevelBytes = reader.readBytes(repLevelLength)
            position += repLevelLength
            
            repetitionLevels = com.molo17.parquetkt.encoding.LevelEncoder.decodeLevels(
                repLevelBytes, 
                metadata.numValues.toInt()
            ).toIntArray()
        }
        
        // Read definition levels if nullable or has nested structure
        var definitionLevels: IntArray? = null
        var nonNullCount = metadata.numValues.toInt()
        
        if (field.maxDefinitionLevel > 0 || field.isNullable) {
            // Read definition level length
            val defLevelLength = reader.readInt32()
            position += 4
            
            // Read definition level data
            val defLevelBytes = reader.readBytes(defLevelLength)
            position += defLevelLength
            
            // Try LevelEncoder first (for nested types), fall back to RLE
            definitionLevels = try {
                com.molo17.parquetkt.encoding.LevelEncoder.decodeLevels(
                    defLevelBytes,
                    metadata.numValues.toInt()
                ).toIntArray()
            } catch (e: Exception) {
                // Fall back to RLE decoder for backward compatibility
                val rleDecoder = RleDecoder(1)
                rleDecoder.decode(defLevelBytes, metadata.numValues.toInt())
            }
            
            // Count non-null values
            // For list columns (with repetition levels), definition level at max means value is present
            // For simple nullable columns, definition level > 0 means value is present
            nonNullCount = if (repetitionLevels != null && field.maxDefinitionLevel > 1) {
                // For lists, use the actual max definition level from the data
                // This handles cases where the schema's maxDefLevel doesn't match the actual data
                val actualMaxDefLevel = definitionLevels?.maxOrNull() ?: field.maxDefinitionLevel
                definitionLevels?.count { it == actualMaxDefLevel } ?: metadata.numValues.toInt()
            } else {
                // For simple nullable fields, count any non-zero definition level
                definitionLevels?.count { it > 0 } ?: metadata.numValues.toInt()
            }
        }
        
        // Decode values from remaining data (only non-null values are encoded)
        val valueData = uncompressedData.copyOfRange(position, uncompressedData.size)
        
        // Check if data is dictionary-encoded
        val encoding = pageHeader.dataPageHeader?.encoding ?: Encoding.PLAIN
        val values = if (encoding == Encoding.RLE_DICTIONARY && dictionaryData != null) {
            // Dictionary-encoded data
            val dictDecoder = DictionaryDecoder(field.dataType, dictionaryData)
            dictDecoder.decode(valueData, nonNullCount)
        } else {
            // Plain-encoded data
            val decoder = PlainDecoder(field.dataType)
            decoder.decode(valueData, nonNullCount)
        }
        
        // Reconstruct with nulls if needed
        // For list columns (with repetition levels), keep flattened values as-is
        // The reconstruction will happen later using NestedDataReconstructor
        val finalData = if (definitionLevels != null && repetitionLevels == null) {
            reconstructWithNulls(values, definitionLevels)
        } else {
            values
        }
        
        @Suppress("UNCHECKED_CAST")
        return DataColumn(
            field = field,
            data = finalData as Array<Any?>,
            definitionLevels = definitionLevels,
            repetitionLevels = repetitionLevels
        ) as DataColumn<*>
    }
    
    private fun reconstructWithNulls(values: Array<*>, definitionLevels: IntArray): Array<Any?> {
        val result = arrayOfNulls<Any>(definitionLevels.size)
        var valueIndex = 0
        
        for (i in definitionLevels.indices) {
            if (definitionLevels[i] > 0) {
                result[i] = values[valueIndex++]
            }
        }
        
        return result
    }
    
    private fun deserializeFileMetadata(bytes: ByteArray): FileMetaData {
        return ThriftDeserializer.deserializeFileMetadata(bytes)
    }
    
    private fun deserializePageHeaderFromStream(file: RandomAccessFile): PageHeader {
        // Read PageHeader using a wrapper that reads from RandomAccessFile
        val fileInputStream = object : InputStream() {
            override fun read(): Int {
                return try {
                    file.readByte().toInt() and 0xFF
                } catch (e: EOFException) {
                    -1
                }
            }
        }
        val reader = BinaryReader(fileInputStream)
        
        var type = PageType.DATA_PAGE
        var uncompressedSize = 0
        var compressedSize = 0
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) {
                reader.readInt32Zigzag()
            } else {
                lastFieldId + fieldDelta
            }
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> {
                    val thriftValue = reader.readInt32Zigzag()
                    type = PageType.values().find { it.value == thriftValue } ?: PageType.DATA_PAGE
                }
                2 -> uncompressedSize = reader.readInt32Zigzag()
                3 -> compressedSize = reader.readInt32Zigzag()
                else -> ThriftDeserializer.skipField(reader, fieldType)
            }
        }
        
        return PageHeader(
            type = type,
            uncompressedPageSize = uncompressedSize,
            compressedPageSize = compressedSize
        )
    }
    
    private fun checkNotClosed() {
        check(!isClosed) { "ParquetReader is closed" }
    }
    
    companion object {
        fun open(file: File): ParquetReader {
            return ParquetReader(file.absolutePath)
        }
        
        fun open(path: String): ParquetReader {
            return ParquetReader(path)
        }
    }
}
