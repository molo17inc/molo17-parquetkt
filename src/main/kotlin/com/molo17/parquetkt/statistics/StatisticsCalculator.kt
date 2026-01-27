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

package com.molo17.parquetkt.statistics

import com.molo17.parquetkt.format.BinaryWriter
import com.molo17.parquetkt.schema.ParquetType
import com.molo17.parquetkt.thrift.Statistics
import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Calculates statistics (min, max, null count) for column data.
 * Statistics are used for query optimization and predicate pushdown.
 */
class StatisticsCalculator(private val type: ParquetType) {
    
    private var nullCount = 0L
    private var minValue: Any? = null
    private var maxValue: Any? = null
    private var hasValues = false
    
    /**
     * Add a value to the statistics calculation.
     * Null values increment the null count.
     */
    fun add(value: Any?) {
        if (value == null) {
            nullCount++
            return
        }
        
        hasValues = true
        
        when (type) {
            ParquetType.BOOLEAN -> updateBooleanStats(value as Boolean)
            ParquetType.INT32 -> updateInt32Stats(value)
            ParquetType.INT64 -> updateInt64Stats(value)
            ParquetType.FLOAT -> updateFloatStats(value as Float)
            ParquetType.DOUBLE -> updateDoubleStats(value as Double)
            ParquetType.BYTE_ARRAY -> updateByteArrayStats(value)
            ParquetType.FIXED_LEN_BYTE_ARRAY -> updateByteArrayStats(value)
            ParquetType.INT96 -> {} // INT96 statistics not commonly used
        }
    }
    
    /**
     * Add multiple values at once.
     */
    fun addAll(values: Array<Any?>) {
        values.forEach { add(it) }
    }
    
    /**
     * Build the Statistics object with calculated min/max/null_count.
     */
    fun build(): Statistics? {
        if (!hasValues && nullCount == 0L) {
            return null
        }
        
        val minBytes = minValue?.let { encodeValue(it) }
        val maxBytes = maxValue?.let { encodeValue(it) }
        
        return Statistics(
            max = maxBytes,
            min = minBytes,
            nullCount = nullCount,
            distinctCount = null  // Distinct count calculation is expensive, skip for now
        )
    }
    
    private fun updateBooleanStats(value: Boolean) {
        val intValue = if (value) 1 else 0
        if (minValue == null || intValue < (minValue as Int)) {
            minValue = intValue
        }
        if (maxValue == null || intValue > (maxValue as Int)) {
            maxValue = intValue
        }
    }
    
    private fun updateInt32Stats(value: Any) {
        val intValue = when (value) {
            is Int -> value
            is LocalDate -> value.toEpochDay().toInt()
            else -> value as Int
        }
        
        if (minValue == null || intValue < (minValue as Int)) {
            minValue = intValue
        }
        if (maxValue == null || intValue > (maxValue as Int)) {
            maxValue = intValue
        }
    }
    
    private fun updateInt64Stats(value: Any) {
        val longValue = when (value) {
            is Long -> value
            is LocalDateTime -> {
                val epochSecond = value.atZone(ZoneOffset.UTC).toEpochSecond()
                val nanos = value.nano
                epochSecond * 1_000_000L + nanos / 1000L
            }
            else -> value as Long
        }
        
        if (minValue == null || longValue < (minValue as Long)) {
            minValue = longValue
        }
        if (maxValue == null || longValue > (maxValue as Long)) {
            maxValue = longValue
        }
    }
    
    private fun updateFloatStats(value: Float) {
        if (minValue == null || value < (minValue as Float)) {
            minValue = value
        }
        if (maxValue == null || value > (maxValue as Float)) {
            maxValue = value
        }
    }
    
    private fun updateDoubleStats(value: Double) {
        if (minValue == null || value < (minValue as Double)) {
            minValue = value
        }
        if (maxValue == null || value > (maxValue as Double)) {
            maxValue = value
        }
    }
    
    private fun updateByteArrayStats(value: Any) {
        val bytes = when (value) {
            is String -> value.toByteArray(Charsets.UTF_8)
            is ByteArray -> value
            else -> return
        }
        
        if (minValue == null || compareByteArrays(bytes, minValue as ByteArray) < 0) {
            minValue = bytes
        }
        if (maxValue == null || compareByteArrays(bytes, maxValue as ByteArray) > 0) {
            maxValue = bytes
        }
    }
    
    private fun compareByteArrays(a: ByteArray, b: ByteArray): Int {
        val minLength = minOf(a.size, b.size)
        for (i in 0 until minLength) {
            val diff = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
            if (diff != 0) return diff
        }
        return a.size - b.size
    }
    
    private fun encodeValue(value: Any): ByteArray {
        val output = ByteArrayOutputStream()
        val writer = BinaryWriter(output)
        
        when (type) {
            ParquetType.BOOLEAN -> {
                writer.writeByte(if ((value as Int) == 1) 1 else 0)
            }
            ParquetType.INT32 -> {
                writer.writeInt32(value as Int)
            }
            ParquetType.INT64 -> {
                writer.writeInt64(value as Long)
            }
            ParquetType.FLOAT -> {
                writer.writeFloat(value as Float)
            }
            ParquetType.DOUBLE -> {
                writer.writeDouble(value as Double)
            }
            ParquetType.BYTE_ARRAY, ParquetType.FIXED_LEN_BYTE_ARRAY -> {
                val bytes = value as ByteArray
                writer.writeBytes(bytes)
            }
            else -> {}
        }
        
        writer.flush()
        return output.toByteArray()
    }
    
    companion object {
        /**
         * Calculate statistics for a column of data.
         */
        fun calculate(type: ParquetType, values: Array<Any?>): Statistics? {
            val calculator = StatisticsCalculator(type)
            calculator.addAll(values)
            return calculator.build()
        }
    }
}
