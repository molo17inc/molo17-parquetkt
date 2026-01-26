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


package com.molo17.parquetkt.schema

/**
 * Represents a field that can contain nested structures (groups).
 * This enables support for structs, lists, and maps in Parquet.
 */
sealed class NestedField(
    open val name: String,
    open val repetition: Repetition,
    open val maxDefinitionLevel: Int,
    open val maxRepetitionLevel: Int
) {
    /**
     * A primitive field (leaf node in the schema tree).
     */
    data class Primitive(
        override val name: String,
        val dataType: ParquetType,
        val logicalType: LogicalType = LogicalType.NONE,
        override val repetition: Repetition = Repetition.REQUIRED,
        override val maxDefinitionLevel: Int = 0,
        override val maxRepetitionLevel: Int = 0,
        val length: Int? = null,
        val precision: Int? = null,
        val scale: Int? = null
    ) : NestedField(name, repetition, maxDefinitionLevel, maxRepetitionLevel) {
        
        val isNullable: Boolean get() = repetition == Repetition.OPTIONAL
        val isRepeated: Boolean get() = repetition == Repetition.REPEATED
        val isRequired: Boolean get() = repetition == Repetition.REQUIRED
        
        fun toDataField(): DataField {
            return DataField(
                name = name,
                dataType = dataType,
                logicalType = logicalType,
                repetition = repetition,
                maxDefinitionLevel = maxDefinitionLevel,
                maxRepetitionLevel = maxRepetitionLevel,
                length = length,
                precision = precision,
                scale = scale
            )
        }
    }
    
    /**
     * A group field (struct/nested structure).
     * Contains child fields that form a nested structure.
     */
    data class Group(
        override val name: String,
        val children: List<NestedField>,
        override val repetition: Repetition = Repetition.REQUIRED,
        override val maxDefinitionLevel: Int = 0,
        override val maxRepetitionLevel: Int = 0,
        val logicalType: LogicalType = LogicalType.NONE
    ) : NestedField(name, repetition, maxDefinitionLevel, maxRepetitionLevel) {
        
        val isNullable: Boolean get() = repetition == Repetition.OPTIONAL
        val isRepeated: Boolean get() = repetition == Repetition.REPEATED
        val isRequired: Boolean get() = repetition == Repetition.REQUIRED
        
        /**
         * Get all leaf (primitive) fields in this group and its descendants.
         */
        fun getLeafFields(): List<Primitive> {
            return children.flatMap { child ->
                when (child) {
                    is Primitive -> listOf(child)
                    is Group -> child.getLeafFields()
                }
            }
        }
    }
    
    companion object {
        /**
         * Create a list field with proper 3-level Parquet structure.
         * Structure: list (group) -> list (repeated group) -> element
         */
        fun createList(
            name: String,
            elementField: NestedField,
            nullable: Boolean = false,
            elementNullable: Boolean = true
        ): Group {
            // Adjust element repetition and definition levels
            val adjustedElement = when (elementField) {
                is Primitive -> elementField.copy(
                    name = "element",
                    repetition = if (elementNullable) Repetition.OPTIONAL else Repetition.REQUIRED,
                    maxDefinitionLevel = if (elementNullable) 3 else 2,
                    maxRepetitionLevel = 1
                )
                is Group -> elementField.copy(
                    name = "element",
                    repetition = if (elementNullable) Repetition.OPTIONAL else Repetition.REQUIRED,
                    maxDefinitionLevel = if (elementNullable) 3 else 2,
                    maxRepetitionLevel = 1
                )
            }
            
            // Create the repeated "list" group
            val listGroup = Group(
                name = "list",
                children = listOf(adjustedElement),
                repetition = Repetition.REPEATED,
                maxDefinitionLevel = 2,
                maxRepetitionLevel = 1
            )
            
            // Create the outer list group
            return Group(
                name = name,
                children = listOf(listGroup),
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED,
                maxDefinitionLevel = if (nullable) 1 else 0,
                maxRepetitionLevel = 0,
                logicalType = LogicalType.LIST
            )
        }
        
        /**
         * Create a map field with proper Parquet structure.
         * Structure: map (group) -> key_value (repeated group) -> key, value
         */
        fun createMap(
            name: String,
            keyField: Primitive,
            valueField: NestedField,
            nullable: Boolean = false,
            valueNullable: Boolean = true
        ): Group {
            // Keys are always required in Parquet maps
            val adjustedKey = keyField.copy(
                name = "key",
                repetition = Repetition.REQUIRED,
                maxDefinitionLevel = 2,
                maxRepetitionLevel = 1
            )
            
            // Adjust value field
            val adjustedValue = when (valueField) {
                is Primitive -> valueField.copy(
                    name = "value",
                    repetition = if (valueNullable) Repetition.OPTIONAL else Repetition.REQUIRED,
                    maxDefinitionLevel = if (valueNullable) 3 else 2,
                    maxRepetitionLevel = 1
                )
                is Group -> valueField.copy(
                    name = "value",
                    repetition = if (valueNullable) Repetition.OPTIONAL else Repetition.REQUIRED,
                    maxDefinitionLevel = if (valueNullable) 3 else 2,
                    maxRepetitionLevel = 1
                )
            }
            
            // Create the repeated "key_value" group
            val keyValueGroup = Group(
                name = "key_value",
                children = listOf(adjustedKey, adjustedValue),
                repetition = Repetition.REPEATED,
                maxDefinitionLevel = 2,
                maxRepetitionLevel = 1
            )
            
            // Create the outer map group
            return Group(
                name = name,
                children = listOf(keyValueGroup),
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED,
                maxDefinitionLevel = if (nullable) 1 else 0,
                maxRepetitionLevel = 0,
                logicalType = LogicalType.MAP
            )
        }
        
        /**
         * Create a struct field (nested data class).
         */
        fun createStruct(
            name: String,
            fields: List<NestedField>,
            nullable: Boolean = false
        ): Group {
            return Group(
                name = name,
                children = fields,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED,
                maxDefinitionLevel = if (nullable) 1 else 0,
                maxRepetitionLevel = 0
            )
        }
    }
}
