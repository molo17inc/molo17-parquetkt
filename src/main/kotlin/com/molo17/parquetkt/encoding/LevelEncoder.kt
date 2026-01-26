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
import com.molo17.parquetkt.format.BinaryReader
import java.io.ByteArrayOutputStream

/**
 * Encodes and decodes repetition and definition levels using RLE/Bit-packing hybrid encoding.
 * This is the standard encoding for levels in Parquet.
 * 
 * For now, we use a simple implementation that stores levels as varints.
 * Full RLE/Bit-packing can be added later for optimization.
 */
object LevelEncoder {
    
    /**
     * Encode levels as varints (simple implementation).
     * Format: [count as varint] [level1 as varint] [level2 as varint] ...
     */
    fun encodeLevels(levels: List<Int>): ByteArray {
        if (levels.isEmpty()) {
            return ByteArray(0)
        }
        
        val output = ByteArrayOutputStream()
        val writer = BinaryWriter(output)
        
        // Write count
        writer.writeVarInt(levels.size)
        
        // Write each level as varint
        for (level in levels) {
            writer.writeVarInt(level)
        }
        
        writer.flush()
        return output.toByteArray()
    }
    
    /**
     * Decode levels from varint encoding.
     */
    fun decodeLevels(data: ByteArray, count: Int): List<Int> {
        if (data.isEmpty() || count == 0) {
            return emptyList()
        }
        
        val reader = BinaryReader(data.inputStream())
        val levels = mutableListOf<Int>()
        
        // Read count (verify it matches expected)
        val actualCount = reader.readVarInt()
        require(actualCount == count) { 
            "Level count mismatch: expected $count, got $actualCount" 
        }
        
        // Read levels
        repeat(count) {
            levels.add(reader.readVarInt())
        }
        
        return levels
    }
    
    /**
     * Calculate the maximum bit width needed to store the given max level.
     * Used for determining encoding efficiency.
     */
    fun getMaxBitWidth(maxLevel: Int): Int {
        if (maxLevel == 0) return 0
        var width = 0
        var value = maxLevel
        while (value > 0) {
            width++
            value = value shr 1
        }
        return width
    }
}
