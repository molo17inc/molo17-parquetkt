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

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

class BinaryWriter(private val output: OutputStream) {
    private val buffer = ByteArray(8192) // 8KB buffer for batching writes
    private var bufferPos = 0
    
    fun writeByte(value: Byte) {
        if (bufferPos >= buffer.size) {
            flushBuffer()
        }
        buffer[bufferPos++] = value
    }
    
    fun writeBytes(bytes: ByteArray) {
        if (bytes.size > buffer.size - bufferPos) {
            flushBuffer()
            if (bytes.size > buffer.size) {
                output.write(bytes)
                return
            }
        }
        System.arraycopy(bytes, 0, buffer, bufferPos, bytes.size)
        bufferPos += bytes.size
    }
    
    private fun flushBuffer() {
        if (bufferPos > 0) {
            output.write(buffer, 0, bufferPos)
            bufferPos = 0
        }
    }
    
    private val tempBuffer = ByteArray(8)
    
    fun writeInt32(value: Int) {
        tempBuffer[0] = value.toByte()
        tempBuffer[1] = (value shr 8).toByte()
        tempBuffer[2] = (value shr 16).toByte()
        tempBuffer[3] = (value shr 24).toByte()
        writeBytes(tempBuffer.copyOfRange(0, 4))
    }
    
    fun writeInt64(value: Long) {
        tempBuffer[0] = value.toByte()
        tempBuffer[1] = (value shr 8).toByte()
        tempBuffer[2] = (value shr 16).toByte()
        tempBuffer[3] = (value shr 24).toByte()
        tempBuffer[4] = (value shr 32).toByte()
        tempBuffer[5] = (value shr 40).toByte()
        tempBuffer[6] = (value shr 48).toByte()
        tempBuffer[7] = (value shr 56).toByte()
        writeBytes(tempBuffer)
    }
    
    fun writeFloat(value: Float) {
        writeInt32(java.lang.Float.floatToRawIntBits(value))
    }
    
    fun writeDouble(value: Double) {
        writeInt64(java.lang.Double.doubleToRawLongBits(value))
    }
    
    fun writeBoolean(value: Boolean) {
        writeByte(if (value) 1 else 0)
    }
    
    fun writeInt96(bytes: ByteArray) {
        require(bytes.size == 12) { "INT96 must be 12 bytes" }
        writeBytes(bytes)
    }
    
    fun writeVarInt(value: Int) {
        var v = value
        while ((v and 0x7F.inv()) != 0) {
            writeByte(((v and 0x7F) or 0x80).toByte())
            v = v ushr 7
        }
        writeByte((v and 0x7F).toByte())
    }
    
    fun writeVarLong(value: Long) {
        var v = value
        while ((v and 0x7FL.inv()) != 0L) {
            writeByte(((v and 0x7F) or 0x80).toByte())
            v = v ushr 7
        }
        writeByte((v and 0x7F).toByte())
    }
    
    fun flush() {
        flushBuffer()
        output.flush()
    }
}
