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
import java.io.ByteArrayOutputStream

class RleEncoder(private val bitWidth: Int) {
    
    fun encode(values: IntArray): ByteArray {
        if (values.isEmpty()) return ByteArray(0)
        
        val output = ByteArrayOutputStream()
        val writer = BinaryWriter(output)
        
        var i = 0
        while (i < values.size) {
            val runLength = getRunLength(values, i)
            
            if (runLength >= 8) {
                // RLE run
                writeRleRun(writer, values[i], runLength)
                i += runLength
            } else {
                // Bit-packed run
                val bitPackedLength = getBitPackedLength(values, i)
                writeBitPackedRun(writer, values, i, bitPackedLength)
                i += bitPackedLength
            }
        }
        
        writer.flush()
        return output.toByteArray()
    }
    
    private fun getRunLength(values: IntArray, start: Int): Int {
        val value = values[start]
        var length = 1
        
        while (start + length < values.size && values[start + length] == value) {
            length++
        }
        
        return length
    }
    
    private fun getBitPackedLength(values: IntArray, start: Int): Int {
        var length = 1
        var lastValue = values[start]
        var runLength = 1
        
        while (start + length < values.size) {
            val currentValue = values[start + length]
            
            if (currentValue == lastValue) {
                runLength++
                if (runLength >= 8) {
                    return length - runLength + 1
                }
            } else {
                runLength = 1
                lastValue = currentValue
            }
            
            length++
        }
        
        return length
    }
    
    private fun writeRleRun(writer: BinaryWriter, value: Int, length: Int) {
        val header = (length shl 1) or 0
        writer.writeVarInt(header)
        writeValue(writer, value)
    }
    
    private fun writeBitPackedRun(writer: BinaryWriter, values: IntArray, start: Int, length: Int) {
        val numGroups = (length + 7) / 8
        val header = (numGroups shl 1) or 1
        writer.writeVarInt(header)
        
        for (group in 0 until numGroups) {
            val groupStart = start + (group * 8)
            val groupEnd = minOf(groupStart + 8, start + length)
            writeBitPackedGroup(writer, values, groupStart, groupEnd)
        }
    }
    
    private fun writeBitPackedGroup(writer: BinaryWriter, values: IntArray, start: Int, end: Int) {
        var buffer = 0L
        var bitsWritten = 0
        
        for (i in start until end) {
            buffer = buffer or (values[i].toLong() shl bitsWritten)
            bitsWritten += bitWidth
            
            while (bitsWritten >= 8) {
                writer.writeByte((buffer and 0xFF).toByte())
                buffer = buffer ushr 8
                bitsWritten -= 8
            }
        }
        
        if (bitsWritten > 0) {
            writer.writeByte((buffer and 0xFF).toByte())
        }
    }
    
    private fun writeValue(writer: BinaryWriter, value: Int) {
        val bytes = (bitWidth + 7) / 8
        for (i in 0 until bytes) {
            writer.writeByte(((value ushr (i * 8)) and 0xFF).toByte())
        }
    }
}
