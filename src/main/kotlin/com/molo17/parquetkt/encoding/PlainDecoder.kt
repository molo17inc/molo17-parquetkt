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

class PlainDecoder(private val type: ParquetType) {
    
    fun decode(data: ByteArray, count: Int): Array<*> {
        val input = ByteArrayInputStream(data)
        val reader = BinaryReader(input)
        
        return when (type) {
            ParquetType.BOOLEAN -> decodeBooleans(reader, count)
            ParquetType.INT32 -> decodeInt32s(reader, count)
            ParquetType.INT64 -> decodeInt64s(reader, count)
            ParquetType.INT96 -> decodeInt96s(reader, count)
            ParquetType.FLOAT -> decodeFloats(reader, count)
            ParquetType.DOUBLE -> decodeDoubles(reader, count)
            ParquetType.BYTE_ARRAY -> decodeByteArrays(reader, count)
            ParquetType.FIXED_LEN_BYTE_ARRAY -> decodeFixedLenByteArrays(reader, count, data.size / count)
        }
    }
    
    private fun decodeBooleans(reader: BinaryReader, count: Int): Array<Boolean> {
        val result = mutableListOf<Boolean>()
        var remaining = count
        
        while (remaining > 0) {
            val byte = reader.readByte().toInt() and 0xFF
            val bitsToRead = minOf(8, remaining)
            
            for (i in 0 until bitsToRead) {
                result.add((byte and (1 shl i)) != 0)
            }
            
            remaining -= bitsToRead
        }
        
        return result.toTypedArray()
    }
    
    private fun decodeInt32s(reader: BinaryReader, count: Int): Array<Int> {
        return Array(count) { reader.readInt32() }
    }
    
    private fun decodeInt64s(reader: BinaryReader, count: Int): Array<Long> {
        return Array(count) { reader.readInt64() }
    }
    
    private fun decodeInt96s(reader: BinaryReader, count: Int): Array<ByteArray> {
        return Array(count) { reader.readInt96() }
    }
    
    private fun decodeFloats(reader: BinaryReader, count: Int): Array<Float> {
        return Array(count) { reader.readFloat() }
    }
    
    private fun decodeDoubles(reader: BinaryReader, count: Int): Array<Double> {
        return Array(count) { reader.readDouble() }
    }
    
    private fun decodeByteArrays(reader: BinaryReader, count: Int): Array<ByteArray> {
        return Array(count) {
            val length = reader.readInt32()
            reader.readBytes(length)
        }
    }
    
    private fun decodeFixedLenByteArrays(reader: BinaryReader, count: Int, length: Int): Array<ByteArray> {
        return Array(count) { reader.readBytes(length) }
    }
}
