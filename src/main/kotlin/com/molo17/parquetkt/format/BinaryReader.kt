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


package com.molo17.parquetkt.format

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

class BinaryReader(private val input: InputStream) {
    
    fun readByte(): Byte {
        val value = input.read()
        if (value == -1) throw IllegalStateException("Unexpected end of stream")
        return value.toByte()
    }
    
    fun readBytes(count: Int): ByteArray {
        val buffer = ByteArray(count)
        var offset = 0
        while (offset < count) {
            val read = input.read(buffer, offset, count - offset)
            if (read == -1) throw IllegalStateException("Unexpected end of stream")
            offset += read
        }
        return buffer
    }
    
    fun readInt32(): Int {
        // For data values: fixed 4-byte little-endian
        val bytes = readBytes(4)
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).int
    }
    
    fun readInt64(): Long {
        // For data values: fixed 8-byte little-endian
        val bytes = readBytes(8)
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).long
    }
    
    fun readInt32Zigzag(): Int {
        // For Thrift Compact Protocol: zigzag-encoded varint
        val zigzag = readVarInt()
        return (zigzag ushr 1) xor -(zigzag and 1)
    }
    
    fun readInt64Zigzag(): Long {
        // For Thrift Compact Protocol: zigzag-encoded varlong
        val zigzag = readVarLong()
        return (zigzag ushr 1) xor -(zigzag and 1)
    }
    
    fun readFloat(): Float {
        val bytes = readBytes(4)
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).float
    }
    
    fun readDouble(): Double {
        val bytes = readBytes(8)
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).double
    }
    
    fun readBoolean(): Boolean {
        return readByte() != 0.toByte()
    }
    
    fun readInt96(): ByteArray {
        return readBytes(12)
    }
    
    fun readVarInt(): Int {
        var result = 0
        var shift = 0
        var byte: Int
        
        do {
            byte = readByte().toInt() and 0xFF
            result = result or ((byte and 0x7F) shl shift)
            shift += 7
        } while ((byte and 0x80) != 0)
        
        return result
    }
    
    fun readVarLong(): Long {
        var result = 0L
        var shift = 0
        var byte: Int
        
        do {
            byte = readByte().toInt() and 0xFF
            result = result or ((byte.toLong() and 0x7F) shl shift)
            shift += 7
        } while ((byte and 0x80) != 0)
        
        return result
    }
    
    fun skip(count: Long): Long {
        return input.skip(count)
    }
}
