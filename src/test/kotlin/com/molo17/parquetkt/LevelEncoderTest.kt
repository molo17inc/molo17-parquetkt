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


package com.molo17.parquetkt

import com.molo17.parquetkt.encoding.LevelEncoder
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class LevelEncoderTest {
    
    @Test
    fun `test encode and decode empty levels`() {
        val levels = emptyList<Int>()
        val encoded = LevelEncoder.encodeLevels(levels)
        
        assertEquals(0, encoded.size)
    }
    
    @Test
    fun `test encode and decode single level`() {
        val levels = listOf(0)
        val encoded = LevelEncoder.encodeLevels(levels)
        val decoded = LevelEncoder.decodeLevels(encoded, 1)
        
        assertEquals(levels, decoded)
    }
    
    @Test
    fun `test encode and decode multiple levels`() {
        val levels = listOf(0, 1, 1, 0, 2, 2, 2)
        val encoded = LevelEncoder.encodeLevels(levels)
        val decoded = LevelEncoder.decodeLevels(encoded, levels.size)
        
        assertEquals(levels, decoded)
    }
    
    @Test
    fun `test encode and decode repetition levels for lists`() {
        // Typical repetition levels for 3 lists: [[a,b,c], [d], [e,f]]
        val repLevels = listOf(0, 1, 1, 0, 0, 1)
        val encoded = LevelEncoder.encodeLevels(repLevels)
        val decoded = LevelEncoder.decodeLevels(encoded, repLevels.size)
        
        assertEquals(repLevels, decoded)
    }
    
    @Test
    fun `test encode and decode definition levels`() {
        // Definition levels with nulls: [value, value, null, value]
        val defLevels = listOf(2, 2, 1, 2)
        val encoded = LevelEncoder.encodeLevels(defLevels)
        val decoded = LevelEncoder.decodeLevels(encoded, defLevels.size)
        
        assertEquals(defLevels, decoded)
    }
    
    @Test
    fun `test get max bit width`() {
        assertEquals(0, LevelEncoder.getMaxBitWidth(0))
        assertEquals(1, LevelEncoder.getMaxBitWidth(1))
        assertEquals(2, LevelEncoder.getMaxBitWidth(2))
        assertEquals(2, LevelEncoder.getMaxBitWidth(3))
        assertEquals(3, LevelEncoder.getMaxBitWidth(4))
        assertEquals(3, LevelEncoder.getMaxBitWidth(7))
        assertEquals(4, LevelEncoder.getMaxBitWidth(8))
    }
    
    @Test
    fun `test round trip with large level values`() {
        val levels = listOf(0, 5, 10, 15, 20, 25, 30)
        val encoded = LevelEncoder.encodeLevels(levels)
        val decoded = LevelEncoder.decodeLevels(encoded, levels.size)
        
        assertEquals(levels, decoded)
    }
}
