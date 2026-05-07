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


package com.molo17.parquetkt.thrift

import com.molo17.parquetkt.format.BinaryReader
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.Encoding
import com.molo17.parquetkt.schema.ParquetType
import java.io.ByteArrayInputStream

object ThriftDeserializer {
    
    fun deserializeFileMetadata(bytes: ByteArray): FileMetaData {
        val reader = BinaryReader(ByteArrayInputStream(bytes))
        
        var version = 1
        var schema = emptyList<SchemaElement>()
        var numRows = 0L
        var rowGroups = emptyList<RowGroup>()
        var createdBy: String? = null
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break // Stop field
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> version = reader.readInt32Zigzag() // version
                2 -> schema = readSchemaList(reader) // schema
                3 -> numRows = reader.readInt64Zigzag() // num_rows
                4 -> rowGroups = readRowGroupList(reader) // row_groups
                6 -> createdBy = readString(reader) // created_by
                else -> skipField(reader, fieldType)
            }
        }
        
        return FileMetaData(
            version = version,
            schema = schema,
            numRows = numRows,
            rowGroups = rowGroups,
            createdBy = createdBy
        )
    }
    
    private fun readSchemaList(reader: BinaryReader): List<SchemaElement> {
        val (_, size) = readListBegin(reader)
        val list = mutableListOf<SchemaElement>()
        
        repeat(size) {
            list.add(readSchemaElement(reader))
        }
        
        return list
    }
    
    private fun readSchemaElement(reader: BinaryReader): SchemaElement {
        var type: ParquetType? = null
        var name = ""
        var numChildren: Int? = null
        var repetitionType: FieldRepetitionType? = null
        var convertedType: ConvertedType? = null
        var logicalType: LogicalTypeAnnotation? = null
        var scale: Int? = null
        var precision: Int? = null
        
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
                    type = ParquetType.values().find { it.thriftValue == thriftValue }
                }
                3 -> {
                    val ordinal = reader.readInt32Zigzag()
                    repetitionType = FieldRepetitionType.values().getOrNull(ordinal)
                }
                4 -> name = readString(reader)
                5 -> numChildren = reader.readInt32Zigzag()
                6 -> {
                    val ordinal = reader.readInt32Zigzag()
                    convertedType = ConvertedType.values().getOrNull(ordinal)
                }
                7 -> scale = reader.readInt32Zigzag()
                8 -> precision = reader.readInt32Zigzag()
                10 -> logicalType = readLogicalType(reader)
                else -> skipField(reader, fieldType)
            }
        }
        
        return SchemaElement(
            type = type,
            name = name,
            numChildren = numChildren,
            repetitionType = repetitionType,
            convertedType = convertedType,
            scale = scale,
            precision = precision,
            logicalType = logicalType
        )
    }
    
    private fun readLogicalType(reader: BinaryReader): LogicalTypeAnnotation? {
        var result: LogicalTypeAnnotation? = null
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            result = when (fieldId) {
                1 -> { skipField(reader, fieldType); LogicalTypeAnnotation.String }
                2 -> { skipField(reader, fieldType); LogicalTypeAnnotation.Map }
                3 -> { skipField(reader, fieldType); LogicalTypeAnnotation.List }
                4 -> { skipField(reader, fieldType); LogicalTypeAnnotation.Enum }
                5 -> readDecimalType(reader)
                6 -> { skipField(reader, fieldType); LogicalTypeAnnotation.Date }
                7 -> readTimeType(reader)
                8 -> readTimestampType(reader)
                10 -> readIntegerType(reader)
                12 -> { skipField(reader, fieldType); LogicalTypeAnnotation.Json }
                13 -> { skipField(reader, fieldType); LogicalTypeAnnotation.Bson }
                14 -> { skipField(reader, fieldType); LogicalTypeAnnotation.Uuid }
                else -> { skipField(reader, fieldType); null }
            }
        }
        
        return result
    }
    
    private fun readDecimalType(reader: BinaryReader): LogicalTypeAnnotation.Decimal {
        var scale = 0
        var precision = 0
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> scale = reader.readInt32Zigzag()
                2 -> precision = reader.readInt32Zigzag()
                else -> skipField(reader, fieldType)
            }
        }
        
        return LogicalTypeAnnotation.Decimal(precision, scale)
    }
    
    private fun readTimeType(reader: BinaryReader): LogicalTypeAnnotation.Time {
        var isAdjustedToUTC = true
        var unit = TimeUnit.MILLIS
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> isAdjustedToUTC = fieldType == 1 // TRUE
                2 -> unit = readTimeUnit(reader)
                else -> skipField(reader, fieldType)
            }
        }
        
        return LogicalTypeAnnotation.Time(isAdjustedToUTC, unit)
    }
    
    private fun readTimestampType(reader: BinaryReader): LogicalTypeAnnotation.Timestamp {
        var isAdjustedToUTC = true
        var unit = TimeUnit.MILLIS
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> isAdjustedToUTC = fieldType == 1 // TRUE
                2 -> unit = readTimeUnit(reader)
                else -> skipField(reader, fieldType)
            }
        }
        
        return LogicalTypeAnnotation.Timestamp(isAdjustedToUTC, unit)
    }
    
    private fun readIntegerType(reader: BinaryReader): LogicalTypeAnnotation.Integer {
        var bitWidth = 32
        var isSigned = true
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> bitWidth = reader.readByte().toInt()
                2 -> isSigned = fieldType == 1 // TRUE
                else -> skipField(reader, fieldType)
            }
        }
        
        return LogicalTypeAnnotation.Integer(bitWidth, isSigned)
    }
    
    private fun readTimeUnit(reader: BinaryReader): TimeUnit {
        var result = TimeUnit.MILLIS
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            result = when (fieldId) {
                1 -> { skipField(reader, fieldType); TimeUnit.MILLIS }
                2 -> { skipField(reader, fieldType); TimeUnit.MICROS }
                3 -> { skipField(reader, fieldType); TimeUnit.NANOS }
                else -> { skipField(reader, fieldType); TimeUnit.MILLIS }
            }
        }
        
        return result
    }
    
    private fun readRowGroupList(reader: BinaryReader): List<RowGroup> {
        val (elementType, size) = readListBegin(reader)
        val list = mutableListOf<RowGroup>()
        
        repeat(size) {
            list.add(readRowGroup(reader))
        }
        
        return list
    }
    
    private fun readRowGroup(reader: BinaryReader): RowGroup {
        var columns = emptyList<ColumnChunk>()
        var totalByteSize = 0L
        var numRows = 0L
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> columns = readColumnChunkList(reader)
                2 -> totalByteSize = reader.readInt64Zigzag()
                3 -> numRows = reader.readInt64Zigzag()
                else -> skipField(reader, fieldType)
            }
        }
        
        return RowGroup(
            columns = columns,
            totalByteSize = totalByteSize,
            numRows = numRows
        )
    }
    
    private fun readColumnChunkList(reader: BinaryReader): List<ColumnChunk> {
        val (elementType, size) = readListBegin(reader)
        val list = mutableListOf<ColumnChunk>()
        
        repeat(size) {
            list.add(readColumnChunk(reader))
        }
        
        return list
    }
    
    private fun readColumnChunk(reader: BinaryReader): ColumnChunk {
        var fileOffset = 0L
        var metaData: ColumnMetaData? = null
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                2 -> fileOffset = reader.readInt64Zigzag()
                3 -> metaData = readColumnMetaData(reader)
                else -> skipField(reader, fieldType)
            }
        }
        
        return ColumnChunk(
            fileOffset = fileOffset,
            metaData = metaData ?: throw IllegalStateException("ColumnMetaData is required")
        )
    }
    
    private fun readColumnMetaData(reader: BinaryReader): ColumnMetaData {
        var type = ParquetType.INT32
        var encodings = emptyList<Encoding>()
        var pathInSchema = emptyList<String>()
        var codec = CompressionCodec.UNCOMPRESSED
        var numValues = 0L
        var totalUncompressedSize = 0L
        var totalCompressedSize = 0L
        var dataPageOffset = 0L
        var dictionaryPageOffset: Long? = null
        var statistics: Statistics? = null
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> {
                    val thriftValue = reader.readInt32Zigzag()
                    type = ParquetType.values().find { it.thriftValue == thriftValue } ?: ParquetType.INT32
                }
                2 -> encodings = readEncodingList(reader)
                3 -> pathInSchema = readStringList(reader)
                4 -> {
                    val thriftValue = reader.readInt32Zigzag()
                    codec = CompressionCodec.values().find { it.thriftValue == thriftValue } ?: CompressionCodec.UNCOMPRESSED
                }
                5 -> numValues = reader.readInt64Zigzag()
                6 -> totalUncompressedSize = reader.readInt64Zigzag()
                7 -> totalCompressedSize = reader.readInt64Zigzag()
                8 -> statistics = readStatistics(reader)
                9 -> dataPageOffset = reader.readInt64Zigzag()
                11 -> dictionaryPageOffset = reader.readInt64Zigzag()
                else -> skipField(reader, fieldType)
            }
        }
        
        return ColumnMetaData(
            type = type,
            encodings = encodings,
            pathInSchema = pathInSchema,
            codec = codec,
            numValues = numValues,
            totalUncompressedSize = totalUncompressedSize,
            totalCompressedSize = totalCompressedSize,
            dataPageOffset = dataPageOffset,
            dictionaryPageOffset = dictionaryPageOffset,
            statistics = statistics
        )
    }
    
    private fun readStatistics(reader: BinaryReader): Statistics {
        var max: ByteArray? = null
        var min: ByteArray? = null
        var nullCount: Long? = null
        var distinctCount: Long? = null
        var maxValue: ByteArray? = null
        var minValue: ByteArray? = null
        
        var lastFieldId = 0
        while (true) {
            val fieldHeader = reader.readByte().toInt() and 0xFF
            if (fieldHeader == 0) break
            
            val fieldDelta = (fieldHeader shr 4) and 0x0F
            val fieldType = fieldHeader and 0x0F
            val fieldId = if (fieldDelta == 0) reader.readInt32Zigzag() else lastFieldId + fieldDelta
            lastFieldId = fieldId
            
            when (fieldId) {
                1 -> max = readBinary(reader)
                2 -> min = readBinary(reader)
                3 -> nullCount = reader.readInt64Zigzag()
                4 -> distinctCount = reader.readInt64Zigzag()
                5 -> maxValue = readBinary(reader)
                6 -> minValue = readBinary(reader)
                else -> skipField(reader, fieldType)
            }
        }
        
        return Statistics(
            max = max,
            min = min,
            nullCount = nullCount,
            distinctCount = distinctCount,
            maxValue = maxValue,
            minValue = minValue
        )
    }
    
    private fun readBinary(reader: BinaryReader): ByteArray {
        val length = reader.readVarInt()
        return reader.readBytes(length)
    }
    
    private fun readEncodingList(reader: BinaryReader): List<Encoding> {
        val (elementType, size) = readListBegin(reader)
        return List(size) {
            val thriftValue = reader.readInt32Zigzag()
            Encoding.values().find { it.thriftValue == thriftValue } ?: Encoding.PLAIN
        }
    }
    
    private fun readStringList(reader: BinaryReader): List<String> {
        val (elementType, size) = readListBegin(reader)
        return List(size) { readString(reader) }
    }
    
    private fun readListBegin(reader: BinaryReader): Pair<Int, Int> {
        val header = reader.readByte().toInt() and 0xFF
        val size = header shr 4
        val elementType = header and 0x0F
        
        return if (size == 15) {
            // Size is encoded separately as varint
            elementType to reader.readVarInt()
        } else {
            elementType to size
        }
    }
    
    private fun readString(reader: BinaryReader): String {
        val length = reader.readVarInt()
        val bytes = reader.readBytes(length)
        return String(bytes, Charsets.UTF_8)
    }
    
    internal fun skipField(reader: BinaryReader, fieldType: Int) {
        when (fieldType) {
            1, 2 -> {} // BOOL_TRUE, BOOL_FALSE - no data
            3 -> reader.readByte() // BYTE
            4 -> reader.readBytes(2) // I16
            5 -> reader.readInt32Zigzag() // I32
            6 -> reader.readInt64Zigzag() // I64
            7 -> reader.readDouble() // DOUBLE
            8 -> { // STRING/BINARY
                val length = reader.readVarInt()
                reader.readBytes(length)
            }
            9, 10 -> { // LIST, SET
                val (elementType, size) = readListBegin(reader)
                repeat(size) { skipField(reader, elementType) }
            }
            12 -> { // STRUCT
                var lastFieldId = 0
                while (true) {
                    val fieldHeader = reader.readByte().toInt() and 0xFF
                    if (fieldHeader == 0) break
                    val fieldDelta = (fieldHeader shr 4) and 0x0F
                    val nestedFieldType = fieldHeader and 0x0F
                    if (fieldDelta == 0) {
                        reader.readInt32Zigzag()
                    } else {
                        lastFieldId += fieldDelta
                    }
                    skipField(reader, nestedFieldType)
                }
            }
        }
    }
}
