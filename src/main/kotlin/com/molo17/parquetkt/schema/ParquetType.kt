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


package com.molo17.parquetkt.schema

enum class ParquetType(val thriftValue: Int) {
    BOOLEAN(0),
    INT32(1),
    INT64(2),
    INT96(3),
    FLOAT(4),
    DOUBLE(5),
    BYTE_ARRAY(6),
    FIXED_LEN_BYTE_ARRAY(7);
    
    companion object {
        fun fromString(type: String): ParquetType {
            return when (type.uppercase()) {
                "BOOLEAN" -> BOOLEAN
                "INT32" -> INT32
                "INT64" -> INT64
                "INT96" -> INT96
                "FLOAT" -> FLOAT
                "DOUBLE" -> DOUBLE
                "BYTE_ARRAY" -> BYTE_ARRAY
                "FIXED_LEN_BYTE_ARRAY" -> FIXED_LEN_BYTE_ARRAY
                else -> throw IllegalArgumentException("Unknown Parquet type: $type")
            }
        }
    }
}

enum class LogicalType {
    NONE,
    STRING,
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
    UUID,
    INTERVAL
}

enum class Repetition {
    REQUIRED,
    OPTIONAL,
    REPEATED
}

enum class CompressionCodec(val thriftValue: Int) {
    UNCOMPRESSED(0),
    SNAPPY(1),
    GZIP(2),
    LZO(3),
    BROTLI(4),
    LZ4(5),
    ZSTD(6)
}

enum class Encoding(val thriftValue: Int) {
    PLAIN(0),
    PLAIN_DICTIONARY(1),
    RLE(2),
    BIT_PACKED(3),
    DELTA_BINARY_PACKED(4),
    DELTA_LENGTH_BYTE_ARRAY(5),
    DELTA_BYTE_ARRAY(6),
    RLE_DICTIONARY(7)
}
