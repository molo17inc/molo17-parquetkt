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

import com.molo17.parquetkt.schema.NestedField

/**
 * Represents a column of nested data with repetition and definition levels.
 * Used for storing lists, maps, and nested structures in columnar format.
 */
data class NestedDataColumn<T>(
    val field: NestedField,
    val values: List<T>,
    val definitionLevels: List<Int>,
    val repetitionLevels: List<Int>,
    val maxDefinitionLevel: Int,
    val maxRepetitionLevel: Int
) {
    val size: Int
        get() = definitionLevels.size
    
    /**
     * Get the actual (non-null) values count.
     */
    val valueCount: Int
        get() = values.size
    
    companion object {
        /**
         * Create a nested data column from raw nested data.
         */
        fun <T> create(
            field: NestedField,
            data: List<Any?>,
            maxDefLevel: Int,
            maxRepLevel: Int
        ): NestedDataColumn<T> {
            val (values, defLevels, repLevels) = com.molo17.parquetkt.encoding.LevelCalculator.flattenNestedData<T>(
                data = data,
                field = field,
                maxDefLevel = maxDefLevel,
                maxRepLevel = maxRepLevel
            )
            
            return NestedDataColumn(
                field = field,
                values = values,
                definitionLevels = defLevels,
                repetitionLevels = repLevels,
                maxDefinitionLevel = maxDefLevel,
                maxRepetitionLevel = maxRepLevel
            )
        }
        
        /**
         * Create a nested data column for a list field.
         */
        fun <T> createForList(
            field: NestedField.Group,
            data: List<List<T>?>
        ): NestedDataColumn<T> {
            require(field.logicalType == com.molo17.parquetkt.schema.LogicalType.LIST) {
                "Field must be a LIST type"
            }
            
            val maxDefLevel = com.molo17.parquetkt.encoding.LevelCalculator.calculateMaxDefinitionLevel(field)
            val maxRepLevel = com.molo17.parquetkt.encoding.LevelCalculator.calculateMaxRepetitionLevel(field)
            
            return create(field, data, maxDefLevel, maxRepLevel)
        }
    }
}

/**
 * Reconstructs nested data from a NestedDataColumn.
 */
object NestedDataReconstructor {
    
    /**
     * Reconstruct a list of lists from flattened columnar data with levels.
     */
    fun <T> reconstructLists(
        values: List<T>,
        definitionLevels: List<Int>,
        repetitionLevels: List<Int>,
        maxDefinitionLevel: Int,
        nullable: Boolean
    ): List<List<T>?> {
        if (definitionLevels.isEmpty()) {
            return emptyList()
        }
        
        val result = mutableListOf<List<T>?>()
        var currentList = mutableListOf<T>()
        var valueIndex = 0
        var isEmptyList = false
        
        for (i in definitionLevels.indices) {
            val defLevel = definitionLevels[i]
            val repLevel = repetitionLevels[i]
            
            // New record starts when repLevel == 0 (except for the first element)
            if (repLevel == 0 && i > 0) {
                // Finalize previous list
                when {
                    isEmptyList -> result.add(emptyList())
                    currentList.isNotEmpty() -> result.add(currentList.toList())
                }
                currentList = mutableListOf()
                isEmptyList = false
            }
            
            // Process current element
            when {
                // Null list
                defLevel == 0 && nullable -> {
                    if (repLevel == 0) {
                        result.add(null)
                    }
                }
                // Empty list marker
                defLevel == 1 -> {
                    isEmptyList = true
                }
                // Actual value
                defLevel == maxDefinitionLevel -> {
                    isEmptyList = false
                    if (valueIndex < values.size) {
                        currentList.add(values[valueIndex])
                        valueIndex++
                    }
                }
            }
        }
        
        // Add the final list
        when {
            isEmptyList -> result.add(emptyList())
            currentList.isNotEmpty() -> result.add(currentList.toList())
        }
        
        return result
    }
}
