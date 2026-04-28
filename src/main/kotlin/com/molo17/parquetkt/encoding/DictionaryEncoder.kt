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

class DictionaryEncoder(private val type: ParquetType, private val typeLength: Int? = null) {
    private val dictionary = mutableMapOf<Any, Int>()
    private val indices = mutableListOf<Int>()
    private var nextIndex = 0
    
    fun add(value: Any) {
        val index = dictionary.getOrPut(value) {
            val currentIndex = nextIndex
            nextIndex++
            currentIndex
        }
        indices.add(index)
    }
    
    fun addAll(values: Array<Any>) {
        for (value in values) {
            add(value)
        }
    }
    
    fun shouldUseDictionary(threshold: Double = 0.5): Boolean {
        if (indices.isEmpty()) return false
        val compressionRatio = dictionary.size.toDouble() / indices.size
        return compressionRatio < threshold
    }
    
    fun encodeDictionary(): ByteArray {
        val output = ByteArrayOutputStream()
        val writer = BinaryWriter(output)
        
        val sortedEntries = dictionary.entries.sortedBy { it.value }
        
        when (type) {
            ParquetType.BYTE_ARRAY -> {
                for (entry in sortedEntries) {
                    val bytes = when (val value = entry.key) {
                        is String -> value.toByteArray(Charsets.UTF_8)
                        is ByteArray -> value
                        else -> throw IllegalArgumentException("Expected String or ByteArray")
                    }
                    writer.writeInt32(bytes.size)
                    writer.writeBytes(bytes)
                }
            }
            ParquetType.FIXED_LEN_BYTE_ARRAY -> {
                if (typeLength == null) {
                    for (entry in sortedEntries) {
                        writer.writeBytes(entry.key as ByteArray)
                    }
                } else {
                    val length = typeLength
                    for (entry in sortedEntries) {
                        val bytes = entry.key as ByteArray
                        if (bytes.size == length) {
                            writer.writeBytes(bytes)
                        } else if (bytes.size < length) {
                            val paddingByte = if (bytes.isNotEmpty() && bytes[0] < 0) 0xFF.toByte() else 0x00.toByte()
                            val padded = ByteArray(length) { paddingByte }
                            System.arraycopy(bytes, 0, padded, length - bytes.size, bytes.size)
                            writer.writeBytes(padded)
                        } else {
                            throw IllegalArgumentException("Provided ByteArray size ${bytes.size} exceeds FIXED_LEN_BYTE_ARRAY length of $length")
                        }
                    }
                }
            }
            else -> throw UnsupportedOperationException("Dictionary encoding only supported for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY")
        }
        
        writer.flush()
        return output.toByteArray()
    }
    
    fun encodeIndices(): ByteArray {
        val output = ByteArrayOutputStream()
        val writer = BinaryWriter(output)
        
        val bitWidth = calculateBitWidth(dictionary.size)
        
        writer.writeByte(bitWidth.toByte())
        
        val rleEncoder = RleEncoder(bitWidth)
        val encodedIndices = rleEncoder.encode(indices.toIntArray())
        writer.writeBytes(encodedIndices)
        
        writer.flush()
        return output.toByteArray()
    }
    
    fun getDictionarySize(): Int = dictionary.size
    
    fun getValueCount(): Int = indices.size
    
    fun clear() {
        dictionary.clear()
        indices.clear()
        nextIndex = 0
    }
    
    private fun calculateBitWidth(maxValue: Int): Int {
        if (maxValue == 0) return 1
        return 32 - Integer.numberOfLeadingZeros(maxValue - 1)
    }
    
    companion object {
        fun canUseDictionary(type: ParquetType): Boolean {
            return type == ParquetType.BYTE_ARRAY || type == ParquetType.FIXED_LEN_BYTE_ARRAY
        }
    }
}
