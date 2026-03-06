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


package com.molo17.parquetkt.data

import com.molo17.parquetkt.schema.DataField

class DataColumn<T>(
    val field: DataField,
    val data: Array<T?>,
    val definitionLevels: IntArray? = null,
    val repetitionLevels: IntArray? = null
) {
    @Suppress("UNCHECKED_CAST")
    internal val rawData: Array<Any?> get() = data as Array<Any?>
    // Cache the defined data to avoid recomputing on every access
    private val _definedData: Array<Any> by lazy {
        @Suppress("UNCHECKED_CAST")
        val nonNullItems = ArrayList<Any>(data.size)
        for (item in data) {
            if (item != null) {
                nonNullItems.add(item as Any)
            }
        }
        nonNullItems.toArray(arrayOfNulls<Any>(nonNullItems.size)) as Array<Any>
    }
    
    val definedData: Array<Any>
        get() = _definedData
    
    val size: Int
        get() = data.size
    
    val hasNulls: Boolean
        get() = data.any { it == null }
    
    fun get(index: Int): T? {
        return data[index]
    }
    
    fun getDefinitionLevel(index: Int): Int {
        return definitionLevels?.get(index) ?: 0
    }
    
    fun getRepetitionLevel(index: Int): Int {
        return repetitionLevels?.get(index) ?: 0
    }
    
    override fun toString(): String {
        return "DataColumn(field=${field.name}, size=$size, hasNulls=$hasNulls)"
    }
    
    companion object {
        fun <T> create(
            field: DataField,
            data: List<T?>,
            definitionLevels: IntArray? = null,
            repetitionLevels: IntArray? = null
        ): DataColumn<T> {
            @Suppress("UNCHECKED_CAST")
            val array = arrayOfNulls<Any>(data.size)
            data.forEachIndexed { index, value -> array[index] = value }
            return DataColumn(
                field = field,
                data = array as Array<T?>,
                definitionLevels = definitionLevels,
                repetitionLevels = repetitionLevels
            )
        }
        
        fun <T> createRequired(
            field: DataField,
            data: List<T>
        ): DataColumn<T> {
            @Suppress("UNCHECKED_CAST")
            val array = arrayOfNulls<Any>(data.size)
            data.forEachIndexed { index, value -> array[index] = value }
            return DataColumn(
                field = field,
                data = array as Array<T?>
            )
        }
    }
}
