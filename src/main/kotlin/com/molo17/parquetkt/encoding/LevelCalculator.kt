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

import com.molo17.parquetkt.schema.NestedField
import com.molo17.parquetkt.schema.Repetition

/**
 * Calculates repetition and definition levels for nested Parquet structures.
 * 
 * Repetition levels track where new list elements start.
 * Definition levels track which optional fields are null.
 */
object LevelCalculator {
    
    /**
     * Calculate the maximum definition level for a field path.
     * This is the number of optional fields in the path from root to leaf.
     */
    fun calculateMaxDefinitionLevel(field: NestedField, parentLevel: Int = 0): Int {
        var level = parentLevel
        
        // Add 1 if this field is optional
        if (field.repetition == Repetition.OPTIONAL) {
            level++
        }
        
        // For groups, recursively calculate for children
        if (field is NestedField.Group) {
            // For lists and maps, the structure adds additional levels
            when {
                field.logicalType == com.molo17.parquetkt.schema.LogicalType.LIST -> {
                    // List structure: list (optional) -> list (repeated) -> element (optional)
                    // Each level can add to definition level
                    return level + 2 // repeated group + optional element
                }
                field.logicalType == com.molo17.parquetkt.schema.LogicalType.MAP -> {
                    // Map structure: map (optional) -> key_value (repeated) -> key/value
                    return level + 2 // repeated group + optional value
                }
                else -> {
                    // Regular struct - just count optional children
                    return field.children.maxOfOrNull { child ->
                        calculateMaxDefinitionLevel(child, level)
                    } ?: level
                }
            }
        }
        
        return level
    }
    
    /**
     * Calculate the maximum repetition level for a field path.
     * This is the number of repeated fields in the path from root to leaf.
     */
    fun calculateMaxRepetitionLevel(field: NestedField, parentLevel: Int = 0): Int {
        var level = parentLevel
        
        // Add 1 if this field is repeated
        if (field.repetition == Repetition.REPEATED) {
            level++
        }
        
        // For groups, recursively calculate for children
        if (field is NestedField.Group) {
            return field.children.maxOfOrNull { child ->
                calculateMaxRepetitionLevel(child, level)
            } ?: level
        }
        
        return level
    }
    
    /**
     * Flatten nested data into columnar format with repetition and definition levels.
     * Returns a triple of (values, definitionLevels, repetitionLevels).
     */
    fun <T> flattenNestedData(
        data: List<Any?>,
        field: NestedField,
        maxDefLevel: Int = calculateMaxDefinitionLevel(field),
        maxRepLevel: Int = calculateMaxRepetitionLevel(field)
    ): Triple<List<T>, List<Int>, List<Int>> {
        val values = mutableListOf<T>()
        val defLevels = mutableListOf<Int>()
        val repLevels = mutableListOf<Int>()
        
        data.forEachIndexed { index, item ->
            flattenValue(
                value = item,
                field = field,
                currentDefLevel = 0,
                currentRepLevel = 0, // Always 0 for new top-level records
                maxDefLevel = maxDefLevel,
                values = values,
                defLevels = defLevels,
                repLevels = repLevels
            )
        }
        
        return Triple(values, defLevels, repLevels)
    }
    
    @Suppress("UNCHECKED_CAST")
    private fun <T> flattenValue(
        value: Any?,
        field: NestedField,
        currentDefLevel: Int,
        currentRepLevel: Int,
        maxDefLevel: Int,
        values: MutableList<T>,
        defLevels: MutableList<Int>,
        repLevels: MutableList<Int>
    ) {
        when (field) {
            is NestedField.Primitive -> {
                if (value == null) {
                    // Null value - write definition level but no value
                    defLevels.add(currentDefLevel)
                    repLevels.add(currentRepLevel)
                } else {
                    // Non-null value - write value and max definition level
                    values.add(value as T)
                    defLevels.add(maxDefLevel)
                    repLevels.add(currentRepLevel)
                }
            }
            is NestedField.Group -> {
                when {
                    field.logicalType == com.molo17.parquetkt.schema.LogicalType.LIST -> {
                        flattenList(value, field, currentDefLevel, currentRepLevel, maxDefLevel, values, defLevels, repLevels)
                    }
                    field.logicalType == com.molo17.parquetkt.schema.LogicalType.MAP -> {
                        flattenMap(value, field, currentDefLevel, currentRepLevel, maxDefLevel, values, defLevels, repLevels)
                    }
                    else -> {
                        flattenStruct(value, field, currentDefLevel, currentRepLevel, maxDefLevel, values, defLevels, repLevels)
                    }
                }
            }
        }
    }
    
    @Suppress("UNCHECKED_CAST")
    private fun <T> flattenList(
        value: Any?,
        field: NestedField.Group,
        currentDefLevel: Int,
        currentRepLevel: Int,
        maxDefLevel: Int,
        values: MutableList<T>,
        defLevels: MutableList<Int>,
        repLevels: MutableList<Int>
    ) {
        if (value == null) {
            // Null list
            defLevels.add(currentDefLevel)
            repLevels.add(currentRepLevel)
            return
        }
        
        val list = value as? List<*> ?: return
        
        if (list.isEmpty()) {
            // Empty list - definition level indicates list is defined but empty
            defLevels.add(currentDefLevel + 1)
            repLevels.add(currentRepLevel)
            return
        }
        
        // Get the element field (list -> list -> element)
        val listGroup = field.children[0] as NestedField.Group
        val elementField = listGroup.children[0]
        
        list.forEachIndexed { index, element ->
            flattenValue(
                value = element,
                field = elementField,
                currentDefLevel = currentDefLevel + 2,
                currentRepLevel = if (index == 0) currentRepLevel else currentRepLevel + 1,
                maxDefLevel = maxDefLevel,
                values = values,
                defLevels = defLevels,
                repLevels = repLevels
            )
        }
    }
    
    @Suppress("UNCHECKED_CAST")
    private fun <T> flattenMap(
        value: Any?,
        field: NestedField.Group,
        currentDefLevel: Int,
        currentRepLevel: Int,
        maxDefLevel: Int,
        values: MutableList<T>,
        defLevels: MutableList<Int>,
        repLevels: MutableList<Int>
    ) {
        if (value == null) {
            defLevels.add(currentDefLevel)
            repLevels.add(currentRepLevel)
            return
        }
        
        val map = value as? Map<*, *> ?: return
        
        if (map.isEmpty()) {
            defLevels.add(currentDefLevel + 1)
            repLevels.add(currentRepLevel)
            return
        }
        
        // Get key and value fields (map -> key_value -> key, value)
        val keyValueGroup = field.children[0] as NestedField.Group
        val keyField = keyValueGroup.children[0]
        val valueField = keyValueGroup.children[1]
        
        map.entries.forEachIndexed { index, entry ->
            // Flatten key
            flattenValue(
                value = entry.key,
                field = keyField,
                currentDefLevel = currentDefLevel + 2,
                currentRepLevel = if (index == 0) currentRepLevel else currentRepLevel + 1,
                maxDefLevel = maxDefLevel,
                values = values,
                defLevels = defLevels,
                repLevels = repLevels
            )
            
            // Flatten value
            flattenValue(
                value = entry.value,
                field = valueField,
                currentDefLevel = currentDefLevel + 2,
                currentRepLevel = if (index == 0) currentRepLevel else currentRepLevel + 1,
                maxDefLevel = maxDefLevel,
                values = values,
                defLevels = defLevels,
                repLevels = repLevels
            )
        }
    }
    
    private fun <T> flattenStruct(
        value: Any?,
        field: NestedField.Group,
        currentDefLevel: Int,
        currentRepLevel: Int,
        maxDefLevel: Int,
        values: MutableList<T>,
        defLevels: MutableList<Int>,
        repLevels: MutableList<Int>
    ) {
        if (value == null) {
            defLevels.add(currentDefLevel)
            repLevels.add(currentRepLevel)
            return
        }
        
        // For structs, we need to flatten each field
        // This requires reflection to access the struct's properties
        // For now, this is a simplified implementation
        field.children.forEach { childField ->
            flattenValue(
                value = null, // TODO: Extract actual field value from struct
                field = childField,
                currentDefLevel = if (field.repetition == Repetition.OPTIONAL) currentDefLevel + 1 else currentDefLevel,
                currentRepLevel = currentRepLevel,
                maxDefLevel = maxDefLevel,
                values = values,
                defLevels = defLevels,
                repLevels = repLevels
            )
        }
    }
}
