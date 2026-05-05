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

import com.molo17.parquetkt.format.BinaryWriter
import java.io.ByteArrayOutputStream

/**
 * Simplified Thrift Compact Protocol serializer for Parquet metadata.
 * This implements just enough of the Thrift format to write valid Parquet files.
 */
object ThriftSerializer {
    
    fun serializeFileMetadata(metadata: FileMetaData): ByteArray {
        val output = ByteArrayOutputStream()
        val writer = BinaryWriter(output)
        
        var lastFieldId = 0
        
        // Field 1: version (i32)
        writeFieldBegin(writer, 1, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(metadata.version))
        lastFieldId = 1
        
        // Field 2: schema (list of SchemaElement)
        writeFieldBegin(writer, 2, lastFieldId, ThriftType.LIST)
        writeListBegin(writer, ThriftType.STRUCT, metadata.schema.size)
        metadata.schema.forEach { element ->
            serializeSchemaElement(writer, element)
        }
        lastFieldId = 2
        
        // Field 3: num_rows (i64)
        writeFieldBegin(writer, 3, lastFieldId, ThriftType.I64)
        writer.writeVarLong(zigzagEncode(metadata.numRows))
        lastFieldId = 3
        
        // Field 4: row_groups (list of RowGroup)
        writeFieldBegin(writer, 4, lastFieldId, ThriftType.LIST)
        writeListBegin(writer, ThriftType.STRUCT, metadata.rowGroups.size)
        metadata.rowGroups.forEach { rowGroup ->
            serializeRowGroup(writer, rowGroup)
        }
        lastFieldId = 4
        
        // Field 6: created_by (string) - optional
        if (metadata.createdBy != null) {
            writeFieldBegin(writer, 6, lastFieldId, ThriftType.STRING)
            writeString(writer, metadata.createdBy)
        }
        
        // Stop field
        writer.writeByte(0)
        
        writer.flush()
        return output.toByteArray()
    }
    
    private fun serializeSchemaElement(writer: BinaryWriter, element: SchemaElement) {
        var lastFieldId = 0
        
        // Field 1: type (i32) - optional
        if (element.type != null) {
            writeFieldBegin(writer, 1, lastFieldId, ThriftType.I32)
            writer.writeVarInt(zigzagEncode(element.type.ordinal))
            lastFieldId = 1
        }
        
        // Field 2: type_length (i32) - optional
        if (element.typeLength != null) {
            writeFieldBegin(writer, 2, lastFieldId, ThriftType.I32)
            writer.writeVarInt(zigzagEncode(element.typeLength))
            lastFieldId = 2
        }
        
        // Field 3: repetition_type (i32) - optional
        if (element.repetitionType != null) {
            writeFieldBegin(writer, 3, lastFieldId, ThriftType.I32)
            writer.writeVarInt(zigzagEncode(element.repetitionType.ordinal))
            lastFieldId = 3
        }
        
        // Field 4: name (string) - required
        writeFieldBegin(writer, 4, lastFieldId, ThriftType.STRING)
        writeString(writer, element.name)
        lastFieldId = 4
        
        // Field 5: num_children (i32) - optional
        if (element.numChildren != null) {
            writeFieldBegin(writer, 5, lastFieldId, ThriftType.I32)
            writer.writeVarInt(zigzagEncode(element.numChildren))
            lastFieldId = 5
        }
        
        // Field 6: converted_type (i32) - optional (deprecated but still supported)
        if (element.convertedType != null) {
            writeFieldBegin(writer, 6, lastFieldId, ThriftType.I32)
            writer.writeVarInt(zigzagEncode(element.convertedType.ordinal))
            lastFieldId = 6
        }
        
        // Field 7: scale (i32) - optional
        if (element.scale != null) {
            writeFieldBegin(writer, 7, lastFieldId, ThriftType.I32)
            writer.writeVarInt(zigzagEncode(element.scale))
            lastFieldId = 7
        }
        
        // Field 8: precision (i32) - optional
        if (element.precision != null) {
            writeFieldBegin(writer, 8, lastFieldId, ThriftType.I32)
            writer.writeVarInt(zigzagEncode(element.precision))
            lastFieldId = 8
        }
        
        // Field 10: logicalType (struct) - optional (modern format)
        if (element.logicalType != null) {
            writeFieldBegin(writer, 10, lastFieldId, ThriftType.STRUCT)
            serializeLogicalType(writer, element.logicalType)
            lastFieldId = 10
        }
        
        // Stop field
        writer.writeByte(0)
    }
    
    private fun serializeRowGroup(writer: BinaryWriter, rowGroup: RowGroup) {
        var lastFieldId = 0
        
        // Field 1: columns (list of ColumnChunk)
        writeFieldBegin(writer, 1, lastFieldId, ThriftType.LIST)
        writeListBegin(writer, ThriftType.STRUCT, rowGroup.columns.size)
        rowGroup.columns.forEach { column ->
            serializeColumnChunk(writer, column)
        }
        lastFieldId = 1
        
        // Field 2: total_byte_size (i64)
        writeFieldBegin(writer, 2, lastFieldId, ThriftType.I64)
        writer.writeVarLong(zigzagEncode(rowGroup.totalByteSize))
        lastFieldId = 2
        
        // Field 3: num_rows (i64)
        writeFieldBegin(writer, 3, lastFieldId, ThriftType.I64)
        writer.writeVarLong(zigzagEncode(rowGroup.numRows))
        
        // Stop field
        writer.writeByte(0)
    }
    
    private fun serializeColumnChunk(writer: BinaryWriter, chunk: ColumnChunk) {
        var lastFieldId = 0
        
        // Field 2: file_offset (i64)
        writeFieldBegin(writer, 2, lastFieldId, ThriftType.I64)
        writer.writeVarLong(zigzagEncode(chunk.fileOffset))
        lastFieldId = 2
        
        // Field 3: meta_data (ColumnMetaData)
        writeFieldBegin(writer, 3, lastFieldId, ThriftType.STRUCT)
        serializeColumnMetaData(writer, chunk.metaData)
        
        // Stop field
        writer.writeByte(0)
    }
    
    private fun serializeColumnMetaData(writer: BinaryWriter, metadata: ColumnMetaData) {
        var lastFieldId = 0
        
        // Field 1: type (i32)
        writeFieldBegin(writer, 1, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(metadata.type.thriftValue))
        lastFieldId = 1
        
        // Field 2: encodings (list of i32)
        writeFieldBegin(writer, 2, lastFieldId, ThriftType.LIST)
        writeListBegin(writer, ThriftType.I32, metadata.encodings.size)
        metadata.encodings.forEach { encoding ->
            writer.writeVarInt(zigzagEncode(encoding.thriftValue))
        }
        lastFieldId = 2
        
        // Field 3: path_in_schema (list of string)
        writeFieldBegin(writer, 3, lastFieldId, ThriftType.LIST)
        writeListBegin(writer, ThriftType.STRING, metadata.pathInSchema.size)
        metadata.pathInSchema.forEach { path ->
            writeString(writer, path)
        }
        lastFieldId = 3
        
        // Field 4: codec (i32)
        writeFieldBegin(writer, 4, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(metadata.codec.thriftValue))
        lastFieldId = 4
        
        // Field 5: num_values (i64)
        writeFieldBegin(writer, 5, lastFieldId, ThriftType.I64)
        writer.writeVarLong(zigzagEncode(metadata.numValues))
        lastFieldId = 5
        
        // Field 6: total_uncompressed_size (i64)
        writeFieldBegin(writer, 6, lastFieldId, ThriftType.I64)
        writer.writeVarLong(zigzagEncode(metadata.totalUncompressedSize))
        lastFieldId = 6
        
        // Field 7: total_compressed_size (i64)
        writeFieldBegin(writer, 7, lastFieldId, ThriftType.I64)
        writer.writeVarLong(zigzagEncode(metadata.totalCompressedSize))
        lastFieldId = 7
        
        // Field 8: statistics (struct) - optional
        if (metadata.statistics != null) {
            writeFieldBegin(writer, 8, lastFieldId, ThriftType.STRUCT)
            serializeStatistics(writer, metadata.statistics)
            lastFieldId = 8
        }
        
        // Field 9: data_page_offset (i64)
        writeFieldBegin(writer, 9, lastFieldId, ThriftType.I64)
        writer.writeVarLong(zigzagEncode(metadata.dataPageOffset))
        
        // Stop field
        writer.writeByte(0)
    }
    
    private fun serializeStatistics(writer: BinaryWriter, stats: Statistics) {
        var lastFieldId = 0
        
        // Field 1: max (binary) - optional
        if (stats.max != null) {
            writeFieldBegin(writer, 1, lastFieldId, ThriftType.STRING)
            writeBinary(writer, stats.max)
            lastFieldId = 1
        }
        
        // Field 2: min (binary) - optional
        if (stats.min != null) {
            writeFieldBegin(writer, 2, lastFieldId, ThriftType.STRING)
            writeBinary(writer, stats.min)
            lastFieldId = 2
        }
        
        // Field 3: null_count (i64) - optional
        if (stats.nullCount != null) {
            writeFieldBegin(writer, 3, lastFieldId, ThriftType.I64)
            writer.writeVarLong(zigzagEncode(stats.nullCount))
            lastFieldId = 3
        }
        
        // Field 4: distinct_count (i64) - optional
        if (stats.distinctCount != null) {
            writeFieldBegin(writer, 4, lastFieldId, ThriftType.I64)
            writer.writeVarLong(zigzagEncode(stats.distinctCount))
            lastFieldId = 4
        }
        
        // Field 5: max_value (binary) - optional
        if (stats.maxValue != null) {
            writeFieldBegin(writer, 5, lastFieldId, ThriftType.STRING)
            writeBinary(writer, stats.maxValue)
            lastFieldId = 5
        }
        
        // Field 6: min_value (binary) - optional
        if (stats.minValue != null) {
            writeFieldBegin(writer, 6, lastFieldId, ThriftType.STRING)
            writeBinary(writer, stats.minValue)
            lastFieldId = 6
        }
        
        // Stop field
        writer.writeByte(0)
    }
    
    private fun writeBinary(writer: BinaryWriter, data: ByteArray) {
        writer.writeVarInt(data.size)
        writer.writeBytes(data)
    }
    
    private fun writeFieldBegin(writer: BinaryWriter, fieldId: Int, lastFieldId: Int, type: ThriftType) {
        val delta = fieldId - lastFieldId
        
        if (delta > 0 && delta <= 15) {
            // Small delta: encode in single byte as (delta << 4) | type
            val header = (delta shl 4) or type.value
            writer.writeByte(header.toByte())
        } else {
            // Large delta or negative: use zigzag encoding
            // Write type in lower 4 bits with 0 delta marker
            writer.writeByte(type.value.toByte())
            // Write field ID as zigzag varint
            writer.writeVarInt(zigzagEncode(fieldId))
        }
    }
    
    private fun zigzagEncode(n: Int): Int {
        return (n shl 1) xor (n shr 31)
    }
    
    private fun zigzagEncode(n: Long): Long {
        return (n shl 1) xor (n shr 63)
    }
    
    private fun writeListBegin(writer: BinaryWriter, elementType: ThriftType, size: Int) {
        if (size < 15) {
            // Size fits in 4 bits
            val header = (size shl 4) or elementType.value
            writer.writeByte(header.toByte())
        } else {
            // Size doesn't fit, use 0xF0 | type, then write size as varint
            val header = 0xF0 or elementType.value
            writer.writeByte(header.toByte())
            writer.writeVarInt(size)
        }
    }
    
    private fun writeString(writer: BinaryWriter, value: String) {
        val bytes = value.toByteArray(Charsets.UTF_8)
        writer.writeVarInt(bytes.size)
        writer.writeBytes(bytes)
    }
    
    fun serializePageHeader(header: PageHeader): ByteArray {
        val output = ByteArrayOutputStream()
        val writer = BinaryWriter(output)
        
        var lastFieldId = 0
        
        // Field 1: type (i32) - required
        writeFieldBegin(writer, 1, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.type.value))
        lastFieldId = 1
        
        // Field 2: uncompressed_page_size (i32) - required
        writeFieldBegin(writer, 2, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.uncompressedPageSize))
        lastFieldId = 2
        
        // Field 3: compressed_page_size (i32) - required
        writeFieldBegin(writer, 3, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.compressedPageSize))
        lastFieldId = 3
        
        // Field 5: data_page_header (DataPageHeader) - optional, set for DATA_PAGE type
        if (header.dataPageHeader != null) {
            writeFieldBegin(writer, 5, lastFieldId, ThriftType.STRUCT)
            serializeDataPageHeader(writer, header.dataPageHeader)
            lastFieldId = 5
        }
        
        // Field 7: data_page_header_v2 (DataPageHeaderV2) - optional, set for DATA_PAGE_V2 type
        if (header.dataPageHeaderV2 != null) {
            writeFieldBegin(writer, 7, lastFieldId, ThriftType.STRUCT)
            serializeDataPageHeaderV2(writer, header.dataPageHeaderV2)
        }
        
        // Stop field
        writer.writeByte(0)
        
        writer.flush()
        return output.toByteArray()
    }
    
    private fun serializeDataPageHeader(writer: BinaryWriter, header: DataPageHeader) {
        var lastFieldId = 0
        
        // Field 1: num_values (i32) - required
        writeFieldBegin(writer, 1, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.numValues))
        lastFieldId = 1
        
        // Field 2: encoding (i32) - required
        writeFieldBegin(writer, 2, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.encoding.thriftValue))
        lastFieldId = 2
        
        // Field 3: definition_level_encoding (i32) - required
        writeFieldBegin(writer, 3, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.definitionLevelEncoding.thriftValue))
        lastFieldId = 3
        
        // Field 4: repetition_level_encoding (i32) - required
        writeFieldBegin(writer, 4, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.repetitionLevelEncoding.thriftValue))
        
        // Stop field
        writer.writeByte(0)
    }
    
    private fun serializeDataPageHeaderV2(writer: BinaryWriter, header: DataPageHeaderV2) {
        var lastFieldId = 0
        
        // Field 1: num_values (i32) - required
        writeFieldBegin(writer, 1, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.numValues))
        lastFieldId = 1
        
        // Field 2: num_nulls (i32) - required
        writeFieldBegin(writer, 2, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.numNulls))
        lastFieldId = 2
        
        // Field 3: num_rows (i32) - required
        writeFieldBegin(writer, 3, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.numRows))
        lastFieldId = 3
        
        // Field 4: encoding (i32) - required
        writeFieldBegin(writer, 4, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.encoding.thriftValue))
        lastFieldId = 4
        
        // Field 5: definition_levels_byte_length (i32) - required
        writeFieldBegin(writer, 5, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.definitionLevelsByteLength))
        lastFieldId = 5
        
        // Field 6: repetition_levels_byte_length (i32) - required
        writeFieldBegin(writer, 6, lastFieldId, ThriftType.I32)
        writer.writeVarInt(zigzagEncode(header.repetitionLevelsByteLength))
        lastFieldId = 6
        
        // Field 7: is_compressed (bool) - optional, default true
        if (!header.isCompressed) {
            writeFieldBegin(writer, 7, lastFieldId, ThriftType.FALSE)
        }
        
        // Stop field
        writer.writeByte(0)
    }
    
    private fun serializeLogicalType(writer: BinaryWriter, logicalType: LogicalTypeAnnotation) {
        var lastFieldId = 0
        
        when (logicalType) {
            is LogicalTypeAnnotation.String -> {
                // Field 1: STRING (struct with no fields)
                writeFieldBegin(writer, 1, lastFieldId, ThriftType.STRUCT)
                writer.writeByte(0) // Empty struct
            }
            is LogicalTypeAnnotation.Map -> {
                // Field 2: MAP (struct with no fields)
                writeFieldBegin(writer, 2, lastFieldId, ThriftType.STRUCT)
                writer.writeByte(0) // Empty struct
            }
            is LogicalTypeAnnotation.List -> {
                // Field 3: LIST (struct with no fields)
                writeFieldBegin(writer, 3, lastFieldId, ThriftType.STRUCT)
                writer.writeByte(0) // Empty struct
            }
            is LogicalTypeAnnotation.Enum -> {
                // Field 4: ENUM (struct with no fields)
                writeFieldBegin(writer, 4, lastFieldId, ThriftType.STRUCT)
                writer.writeByte(0) // Empty struct
            }
            is LogicalTypeAnnotation.Decimal -> {
                // Field 5: DECIMAL (struct with scale and precision)
                writeFieldBegin(writer, 5, lastFieldId, ThriftType.STRUCT)
                var decimalLastFieldId = 0
                // Field 1: scale (i32)
                writeFieldBegin(writer, 1, decimalLastFieldId, ThriftType.I32)
                writer.writeVarInt(zigzagEncode(logicalType.scale))
                decimalLastFieldId = 1
                // Field 2: precision (i32)
                writeFieldBegin(writer, 2, decimalLastFieldId, ThriftType.I32)
                writer.writeVarInt(zigzagEncode(logicalType.precision))
                writer.writeByte(0) // End of DECIMAL struct
            }
            is LogicalTypeAnnotation.Date -> {
                // Field 6: DATE (struct with no fields)
                writeFieldBegin(writer, 6, lastFieldId, ThriftType.STRUCT)
                writer.writeByte(0) // Empty struct
            }
            is LogicalTypeAnnotation.Time -> {
                // Field 7: TIME (struct with isAdjustedToUTC and unit)
                writeFieldBegin(writer, 7, lastFieldId, ThriftType.STRUCT)
                var timeLastFieldId = 0
                // Field 1: isAdjustedToUTC (bool)
                writeFieldBegin(writer, 1, timeLastFieldId, if (logicalType.isAdjustedToUTC) ThriftType.TRUE else ThriftType.FALSE)
                timeLastFieldId = 1
                // Field 2: unit (struct)
                writeFieldBegin(writer, 2, timeLastFieldId, ThriftType.STRUCT)
                serializeTimeUnit(writer, logicalType.unit)
                writer.writeByte(0) // End of TIME struct
            }
            is LogicalTypeAnnotation.Timestamp -> {
                // Field 8: TIMESTAMP (struct with isAdjustedToUTC and unit)
                writeFieldBegin(writer, 8, lastFieldId, ThriftType.STRUCT)
                var timestampLastFieldId = 0
                // Field 1: isAdjustedToUTC (bool)
                writeFieldBegin(writer, 1, timestampLastFieldId, if (logicalType.isAdjustedToUTC) ThriftType.TRUE else ThriftType.FALSE)
                timestampLastFieldId = 1
                // Field 2: unit (struct)
                writeFieldBegin(writer, 2, timestampLastFieldId, ThriftType.STRUCT)
                serializeTimeUnit(writer, logicalType.unit)
                writer.writeByte(0) // End of TIMESTAMP struct
            }
            is LogicalTypeAnnotation.Integer -> {
                // Field 10: INTEGER (struct with bitWidth and isSigned)
                writeFieldBegin(writer, 10, lastFieldId, ThriftType.STRUCT)
                var intLastFieldId = 0
                // Field 1: bitWidth (i8)
                writeFieldBegin(writer, 1, intLastFieldId, ThriftType.BYTE)
                writer.writeByte(logicalType.bitWidth.toByte())
                intLastFieldId = 1
                // Field 2: isSigned (bool)
                writeFieldBegin(writer, 2, intLastFieldId, if (logicalType.isSigned) ThriftType.TRUE else ThriftType.FALSE)
                writer.writeByte(0) // End of INTEGER struct
            }
            is LogicalTypeAnnotation.Json -> {
                // Field 12: JSON (struct with no fields)
                writeFieldBegin(writer, 12, lastFieldId, ThriftType.STRUCT)
                writer.writeByte(0) // Empty struct
            }
            is LogicalTypeAnnotation.Bson -> {
                // Field 13: BSON (struct with no fields)
                writeFieldBegin(writer, 13, lastFieldId, ThriftType.STRUCT)
                writer.writeByte(0) // Empty struct
            }
            is LogicalTypeAnnotation.Uuid -> {
                // Field 14: UUID (struct with no fields)
                writeFieldBegin(writer, 14, lastFieldId, ThriftType.STRUCT)
                writer.writeByte(0) // Empty struct
            }
        }
        
        // Stop field for LogicalType union
        writer.writeByte(0)
    }
    
    private fun serializeTimeUnit(writer: BinaryWriter, unit: TimeUnit) {
        when (unit) {
            TimeUnit.MILLIS -> {
                // Field 1: MILLIS (struct with no fields)
                writeFieldBegin(writer, 1, 0, ThriftType.STRUCT)
                writer.writeByte(0)
            }
            TimeUnit.MICROS -> {
                // Field 2: MICROS (struct with no fields)
                writeFieldBegin(writer, 2, 0, ThriftType.STRUCT)
                writer.writeByte(0)
            }
            TimeUnit.NANOS -> {
                // Field 3: NANOS (struct with no fields)
                writeFieldBegin(writer, 3, 0, ThriftType.STRUCT)
                writer.writeByte(0)
            }
        }
        // Stop field for TimeUnit union
        writer.writeByte(0)
    }
    
    private enum class ThriftType(val value: Int) {
        STOP(0),
        TRUE(1),
        FALSE(2),
        BYTE(3),
        I16(4),
        I32(5),
        I64(6),
        DOUBLE(7),
        BINARY(8),
        LIST(9),
        SET(10),
        MAP(11),
        STRUCT(12),
        STRING(8) // Same as BINARY in compact protocol
    }
}
