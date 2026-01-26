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
import java.io.ByteArrayInputStream

class RleDecoder(private val bitWidth: Int) {
    
    fun decode(data: ByteArray, count: Int): IntArray {
        if (data.isEmpty() || count == 0) return IntArray(0)
        
        val input = ByteArrayInputStream(data)
        val reader = BinaryReader(input)
        val result = mutableListOf<Int>()
        
        while (result.size < count) {
            val header = reader.readVarInt()
            val isRle = (header and 1) == 0
            
            if (isRle) {
                val runLength = header ushr 1
                val value = readValue(reader)
                repeat(runLength) { result.add(value) }
            } else {
                val numGroups = header ushr 1
                for (group in 0 until numGroups) {
                    val groupValues = readBitPackedGroup(reader)
                    result.addAll(groupValues)
                    if (result.size >= count) break
                }
            }
        }
        
        return result.take(count).toIntArray()
    }
    
    private fun readValue(reader: BinaryReader): Int {
        val bytes = (bitWidth + 7) / 8
        var value = 0
        
        for (i in 0 until bytes) {
            val byte = reader.readByte().toInt() and 0xFF
            value = value or (byte shl (i * 8))
        }
        
        return value and ((1 shl bitWidth) - 1)
    }
    
    private fun readBitPackedGroup(reader: BinaryReader): List<Int> {
        val values = mutableListOf<Int>()
        val totalBits = bitWidth * 8
        val totalBytes = (totalBits + 7) / 8
        
        var buffer = 0L
        var bitsInBuffer = 0
        
        for (i in 0 until totalBytes) {
            val byte = reader.readByte().toLong() and 0xFF
            buffer = buffer or (byte shl bitsInBuffer)
            bitsInBuffer += 8
        }
        
        repeat(8) {
            val value = (buffer and ((1L shl bitWidth) - 1)).toInt()
            values.add(value)
            buffer = buffer ushr bitWidth
        }
        
        return values
    }
}
