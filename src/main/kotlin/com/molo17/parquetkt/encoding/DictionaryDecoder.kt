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

import com.molo17.parquetkt.format.BinaryReader
import com.molo17.parquetkt.schema.ParquetType
import java.io.ByteArrayInputStream

class DictionaryDecoder(
    private val type: ParquetType,
    private val dictionaryData: ByteArray
) {
    private val dictionary: Array<Any> by lazy {
        decodeDictionary()
    }
    
    fun decode(indicesData: ByteArray, count: Int): Array<Any> {
        val indices = decodeIndices(indicesData, count)
        return Array(count) { i ->
            dictionary[indices[i]]
        }
    }
    
    private fun decodeDictionary(): Array<Any> {
        val input = ByteArrayInputStream(dictionaryData)
        val reader = BinaryReader(input)
        val values = mutableListOf<Any>()
        
        when (type) {
            ParquetType.BYTE_ARRAY -> {
                while (input.available() > 0) {
                    val length = reader.readInt32()
                    val bytes = reader.readBytes(length)
                    values.add(bytes)
                }
            }
            ParquetType.FIXED_LEN_BYTE_ARRAY -> {
                val itemLength = dictionaryData.size / (dictionaryData.size / 4) // Estimate
                while (input.available() > 0) {
                    val bytes = reader.readBytes(itemLength)
                    values.add(bytes)
                }
            }
            else -> throw UnsupportedOperationException("Dictionary encoding only supported for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY")
        }
        
        return values.toTypedArray()
    }
    
    private fun decodeIndices(indicesData: ByteArray, count: Int): IntArray {
        val input = ByteArrayInputStream(indicesData)
        val reader = BinaryReader(input)
        
        val bitWidth = reader.readByte().toInt() and 0xFF
        
        val remainingData = ByteArray(indicesData.size - 1)
        input.read(remainingData)
        
        val rleDecoder = RleDecoder(bitWidth)
        return rleDecoder.decode(remainingData, count)
    }
    
    companion object {
        fun canUseDictionary(type: ParquetType): Boolean {
            return type == ParquetType.BYTE_ARRAY || type == ParquetType.FIXED_LEN_BYTE_ARRAY
        }
    }
}
