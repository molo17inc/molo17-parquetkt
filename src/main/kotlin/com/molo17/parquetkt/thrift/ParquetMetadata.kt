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

import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.Encoding
import com.molo17.parquetkt.schema.ParquetType

data class FileMetaData(
    val version: Int,
    val schema: List<SchemaElement>,
    val numRows: Long,
    val rowGroups: List<RowGroup>,
    val keyValueMetadata: Map<String, String>? = null,
    val createdBy: String? = null
)

data class SchemaElement(
    val type: ParquetType? = null,
    val typeLength: Int? = null,
    val repetitionType: FieldRepetitionType? = null,
    val name: String,
    val numChildren: Int? = null,
    val convertedType: ConvertedType? = null,
    val scale: Int? = null,
    val precision: Int? = null,
    val fieldId: Int? = null
)

data class RowGroup(
    val columns: List<ColumnChunk>,
    val totalByteSize: Long,
    val numRows: Long,
    val fileOffset: Long? = null
)

data class ColumnChunk(
    val filePath: String? = null,
    val fileOffset: Long,
    val metaData: ColumnMetaData,
    val offsetIndexOffset: Long? = null,
    val offsetIndexLength: Int? = null,
    val columnIndexOffset: Long? = null,
    val columnIndexLength: Int? = null
)

data class ColumnMetaData(
    val type: ParquetType,
    val encodings: List<Encoding>,
    val pathInSchema: List<String>,
    val codec: CompressionCodec,
    val numValues: Long,
    val totalUncompressedSize: Long,
    val totalCompressedSize: Long,
    val dataPageOffset: Long,
    val indexPageOffset: Long? = null,
    val dictionaryPageOffset: Long? = null,
    val statistics: Statistics? = null
)

data class Statistics(
    val max: ByteArray? = null,
    val min: ByteArray? = null,
    val nullCount: Long? = null,
    val distinctCount: Long? = null,
    val maxValue: ByteArray? = null,
    val minValue: ByteArray? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as Statistics
        if (max != null) {
            if (other.max == null) return false
            if (!max.contentEquals(other.max)) return false
        } else if (other.max != null) return false
        if (min != null) {
            if (other.min == null) return false
            if (!min.contentEquals(other.min)) return false
        } else if (other.min != null) return false
        if (nullCount != other.nullCount) return false
        if (distinctCount != other.distinctCount) return false
        return true
    }

    override fun hashCode(): Int {
        var result = max?.contentHashCode() ?: 0
        result = 31 * result + (min?.contentHashCode() ?: 0)
        result = 31 * result + (nullCount?.hashCode() ?: 0)
        result = 31 * result + (distinctCount?.hashCode() ?: 0)
        return result
    }
}

data class PageHeader(
    val type: PageType,
    val uncompressedPageSize: Int,
    val compressedPageSize: Int,
    val crc: Int? = null,
    val dataPageHeader: DataPageHeader? = null,
    val dataPageHeaderV2: DataPageHeaderV2? = null,
    val dictionaryPageHeader: DictionaryPageHeader? = null
)

data class DataPageHeader(
    val numValues: Int,
    val encoding: Encoding,
    val definitionLevelEncoding: Encoding,
    val repetitionLevelEncoding: Encoding,
    val statistics: Statistics? = null
)

data class DataPageHeaderV2(
    val numValues: Int,
    val numNulls: Int,
    val numRows: Int,
    val encoding: Encoding,
    val definitionLevelsByteLength: Int,
    val repetitionLevelsByteLength: Int,
    val isCompressed: Boolean = true,
    val statistics: Statistics? = null
)

data class DictionaryPageHeader(
    val numValues: Int,
    val encoding: Encoding,
    val isSorted: Boolean? = null
)

enum class PageType(val value: Int) {
    DATA_PAGE(0),
    INDEX_PAGE(1),
    DICTIONARY_PAGE(2),
    DATA_PAGE_V2(3)
}

enum class FieldRepetitionType {
    REQUIRED,
    OPTIONAL,
    REPEATED
}

enum class ConvertedType {
    UTF8,
    MAP,
    MAP_KEY_VALUE,
    LIST,
    ENUM,
    DECIMAL,
    DATE,
    TIME_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    UINT_8,
    UINT_16,
    UINT_32,
    UINT_64,
    INT_8,
    INT_16,
    INT_32,
    INT_64,
    JSON,
    BSON,
    INTERVAL
}
