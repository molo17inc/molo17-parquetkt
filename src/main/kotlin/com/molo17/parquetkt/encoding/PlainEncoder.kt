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


package com.molo17.parquetkt.encoding

import com.molo17.parquetkt.format.BinaryWriter
import com.molo17.parquetkt.schema.ParquetType
import java.io.ByteArrayOutputStream

class PlainEncoder(private val type: ParquetType, private val typeLength: Int? = null) {
    
    fun encode(values: Array<Any>): ByteArray {
        // Pre-allocate buffer with estimated size
        val estimatedSize = estimateSize(values.size)
        val output = ByteArrayOutputStream(estimatedSize)
        val writer = BinaryWriter(output)
        
        when (type) {
            ParquetType.BOOLEAN -> encodeBooleans(values, writer)
            ParquetType.INT32 -> encodeInt32s(values, writer)
            ParquetType.INT64 -> encodeInt64s(values, writer)
            ParquetType.INT96 -> encodeInt96s(values, writer)
            ParquetType.FLOAT -> encodeFloats(values, writer)
            ParquetType.DOUBLE -> encodeDoubles(values, writer)
            ParquetType.BYTE_ARRAY -> encodeByteArrays(values, writer)
            ParquetType.FIXED_LEN_BYTE_ARRAY -> encodeFixedLenByteArrays(values, writer)
        }
        
        writer.flush()
        return output.toByteArray()
    }
    
    /**
     * Memory-efficient encoding directly to an output stream.
     * Avoids creating intermediate ByteArrayOutputStream when writing to file.
     */
    fun encodeTo(values: Array<Any>, output: ByteArrayOutputStream) {
        val writer = BinaryWriter(output)
        
        when (type) {
            ParquetType.BOOLEAN -> encodeBooleans(values, writer)
            ParquetType.INT32 -> encodeInt32s(values, writer)
            ParquetType.INT64 -> encodeInt64s(values, writer)
            ParquetType.INT96 -> encodeInt96s(values, writer)
            ParquetType.FLOAT -> encodeFloats(values, writer)
            ParquetType.DOUBLE -> encodeDoubles(values, writer)
            ParquetType.BYTE_ARRAY -> encodeByteArrays(values, writer)
            ParquetType.FIXED_LEN_BYTE_ARRAY -> encodeFixedLenByteArrays(values, writer)
        }
        
        writer.flush()
    }
    
    /**
     * Estimate the encoded size for pre-allocation.
     */
    fun estimateSize(valueCount: Int): Int {
        return when (type) {
            ParquetType.BOOLEAN -> (valueCount + 7) / 8
            ParquetType.INT32, ParquetType.FLOAT -> valueCount * 4
            ParquetType.INT64, ParquetType.DOUBLE -> valueCount * 8
            ParquetType.INT96 -> valueCount * 12
            else -> valueCount * 20 // Estimate for byte arrays
        }
    }
    
    private fun encodeBooleans(values: Array<Any>, writer: BinaryWriter) {
        var currentByte = 0
        var bitIndex = 0
        
        for (value in values) {
            if (value as Boolean) {
                currentByte = currentByte or (1 shl bitIndex)
            }
            bitIndex++
            
            if (bitIndex == 8) {
                writer.writeByte(currentByte.toByte())
                currentByte = 0
                bitIndex = 0
            }
        }
        
        if (bitIndex > 0) {
            writer.writeByte(currentByte.toByte())
        }
    }
    
    private fun encodeInt32s(values: Array<Any>, writer: BinaryWriter) {
        for (value in values) {
            val intValue = when (value) {
                is Int -> value
                is java.time.LocalDate -> {
                    // Convert LocalDate to days since Unix epoch (1970-01-01)
                    value.toEpochDay().toInt()
                }
                else -> value as Int
            }
            writer.writeInt32(intValue)
        }
    }
    
    private fun encodeInt64s(values: Array<Any>, writer: BinaryWriter) {
        for (value in values) {
            val longValue = when (value) {
                is Long -> value
                is java.time.LocalDateTime -> {
                    // Convert LocalDateTime to microseconds since Unix epoch (1970-01-01T00:00:00)
                    val epochSecond = value.atZone(java.time.ZoneOffset.UTC).toEpochSecond()
                    val nanos = value.nano
                    epochSecond * 1_000_000L + nanos / 1000L
                }
                else -> value as Long
            }
            writer.writeInt64(longValue)
        }
    }
    
    private fun encodeInt96s(values: Array<Any>, writer: BinaryWriter) {
        for (value in values) {
            writer.writeInt96(value as ByteArray)
        }
    }
    
    private fun encodeFloats(values: Array<Any>, writer: BinaryWriter) {
        for (value in values) {
            writer.writeFloat(value as Float)
        }
    }
    
    private fun encodeDoubles(values: Array<Any>, writer: BinaryWriter) {
        for (value in values) {
            writer.writeDouble(value as Double)
        }
    }
    
    private fun encodeByteArrays(values: Array<Any>, writer: BinaryWriter) {
        for (value in values) {
            val bytes = when (value) {
                is String -> value.toByteArray(Charsets.UTF_8)
                is ByteArray -> value
                else -> throw IllegalArgumentException("Expected String or ByteArray, got ${value::class.simpleName}")
            }
            writer.writeInt32(bytes.size)
            writer.writeBytes(bytes)
        }
    }
    
    private fun encodeFixedLenByteArrays(values: Array<Any>, writer: BinaryWriter) {
        if (typeLength == null) {
            for (value in values) {
                writer.writeBytes(value as ByteArray)
            }
            return
        }
        val length = typeLength
        for (value in values) {
            val bytes = value as ByteArray
            if (bytes.size == length) {
                writer.writeBytes(bytes)
            } else if (bytes.size < length) {
                // Pad with sign extension (or zeros if positive) to match the required length
                val paddingByte = if (bytes.isNotEmpty() && bytes[0] < 0) 0xFF.toByte() else 0x00.toByte()
                val padded = ByteArray(length) { paddingByte }
                System.arraycopy(bytes, 0, padded, length - bytes.size, bytes.size)
                writer.writeBytes(padded)
            } else {
                throw IllegalArgumentException("Provided ByteArray of size ${bytes.size} exceeds FIXED_LEN_BYTE_ARRAY length of $length")
            }
        }
    }
}
