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


package com.molo17.parquetkt.serialization

import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.LogicalType
import com.molo17.parquetkt.schema.NestedField
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.ParquetType
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaType

object SchemaReflector {
    
    fun <T : Any> reflectSchema(clazz: KClass<T>): ParquetSchema {
        val nestedFields = mutableListOf<NestedField>()
        var hasNestedStructures = false
        
        clazz.memberProperties.forEach { property ->
            val name = property.name
            val type = property.returnType
            val nullable = type.isMarkedNullable
            
            val field = reflectField(name, type, nullable)
            when (field) {
                is DataField -> {
                    // Convert DataField to NestedField.Primitive
                    nestedFields.add(NestedField.Primitive(
                        name = field.name,
                        dataType = field.dataType,
                        logicalType = field.logicalType,
                        repetition = field.repetition,
                        maxDefinitionLevel = field.maxDefinitionLevel,
                        maxRepetitionLevel = field.maxRepetitionLevel
                    ))
                }
                is NestedField -> {
                    nestedFields.add(field)
                    if (field is NestedField.Group && field.logicalType != LogicalType.LIST && field.logicalType != LogicalType.MAP) {
                        hasNestedStructures = true
                    }
                }
            }
        }
        
        // Use createNested to properly handle nested structures
        return ParquetSchema.createNested(nestedFields)
    }
    
    inline fun <reified T : Any> reflectSchema(): ParquetSchema {
        return reflectSchema(T::class)
    }
    
    private fun reflectField(name: String, type: KType, nullable: Boolean): Any {
        val javaType = type.javaType
        
        return when (javaType.typeName) {
            "boolean", "java.lang.Boolean" -> DataField.boolean(name, nullable)
            "int", "java.lang.Integer" -> DataField.int32(name, nullable)
            "long", "java.lang.Long" -> DataField.int64(name, nullable)
            "float", "java.lang.Float" -> DataField.float(name, nullable)
            "double", "java.lang.Double" -> DataField.double(name, nullable)
            "java.lang.String" -> DataField.string(name, nullable)
            "byte[]", "[B" -> DataField.byteArray(name, nullable)
            LocalDate::class.java.name -> DataField.date(name, nullable)
            LocalDateTime::class.java.name -> DataField.timestamp(name, nullable)
            else -> {
                when {
                    // Handle List<T>
                    javaType.typeName.startsWith("java.util.List") || 
                    javaType.typeName.startsWith("kotlin.collections.List") -> {
                        reflectListField(name, type, nullable)
                    }
                    // Handle Map<K, V>
                    javaType.typeName.startsWith("java.util.Map") ||
                    javaType.typeName.startsWith("kotlin.collections.Map") -> {
                        reflectMapField(name, type, nullable)
                    }
                    // Handle nested data classes (Structs)
                    else -> {
                        val classifier = type.classifier as? KClass<*>
                        if (classifier != null && classifier.isData) {
                            reflectStructField(name, classifier, nullable)
                        } else {
                            throw IllegalArgumentException("Unsupported type: ${javaType.typeName}")
                        }
                    }
                }
            }
        }
    }
    
    private fun reflectListField(name: String, type: KType, nullable: Boolean): DataField {
        // Extract element type from List<T>
        val elementType = type.arguments.firstOrNull()?.type
        val (dataType, logicalType) = when (elementType?.javaType?.typeName) {
            "java.lang.String" -> ParquetType.BYTE_ARRAY to LogicalType.STRING
            "int", "java.lang.Integer" -> ParquetType.INT32 to LogicalType.NONE
            "long", "java.lang.Long" -> ParquetType.INT64 to LogicalType.NONE
            "float", "java.lang.Float" -> ParquetType.FLOAT to LogicalType.NONE
            "double", "java.lang.Double" -> ParquetType.DOUBLE to LogicalType.NONE
            "boolean", "java.lang.Boolean" -> ParquetType.BOOLEAN to LogicalType.NONE
            else -> ParquetType.BYTE_ARRAY to LogicalType.STRING // Default to String
        }
        
        // For lists, set maxRepetitionLevel=1 and maxDefinitionLevel based on nullability
        // For nullable list: Level 0=null list, 1=empty list, 2=null element, 3=element present
        // For non-nullable list: Level 0=empty list, 1=element present
        return DataField(
            name = name,
            dataType = dataType,
            logicalType = logicalType,
            repetition = com.molo17.parquetkt.schema.Repetition.REPEATED,
            maxRepetitionLevel = 1,
            maxDefinitionLevel = if (nullable) 3 else 1
        )
    }
    
    private fun reflectStructField(name: String, clazz: KClass<*>, nullable: Boolean): NestedField.Group {
        // Reflect nested fields from the data class properties
        val nestedFields = clazz.memberProperties.mapNotNull { property ->
            val fieldName = property.name
            val fieldType = property.returnType
            val fieldNullable = fieldType.isMarkedNullable
            val field = reflectField(fieldName, fieldType, fieldNullable)
            // Convert to NestedField
            when (field) {
                is DataField -> NestedField.Primitive(
                    name = fieldName,
                    dataType = field.dataType,
                    logicalType = field.logicalType,
                    repetition = field.repetition,
                    maxDefinitionLevel = field.maxDefinitionLevel,
                    maxRepetitionLevel = field.maxRepetitionLevel
                )
                is NestedField -> field
                else -> null
            }
        }
        
        return NestedField.Group(
            name = name,
            children = nestedFields,
            repetition = if (nullable) com.molo17.parquetkt.schema.Repetition.OPTIONAL 
                        else com.molo17.parquetkt.schema.Repetition.REQUIRED,
            maxDefinitionLevel = if (nullable) 1 else 0,
            maxRepetitionLevel = 0
        )
    }
    
    private fun reflectMapField(name: String, type: KType, nullable: Boolean): NestedField.Group {
        // Maps are stored as List<Struct<key, value>> in Parquet
        // Extract key and value types from Map<K, V>
        val keyType = type.arguments.getOrNull(0)?.type
        val valueType = type.arguments.getOrNull(1)?.type
        
        if (keyType == null || valueType == null) {
            throw IllegalArgumentException("Map type must have key and value type arguments")
        }
        
        // Create fields for key and value
        val keyFieldAny = reflectField("key", keyType, false)
        val valueFieldAny = reflectField("value", valueType, valueType.isMarkedNullable)
        
        val keyField = when (keyFieldAny) {
            is DataField -> NestedField.Primitive(
                name = "key",
                dataType = keyFieldAny.dataType,
                logicalType = keyFieldAny.logicalType,
                repetition = com.molo17.parquetkt.schema.Repetition.REQUIRED
            )
            else -> throw IllegalArgumentException("Map keys must be primitive types")
        }
        
        val valueField = when (valueFieldAny) {
            is DataField -> NestedField.Primitive(
                name = "value",
                dataType = valueFieldAny.dataType,
                logicalType = valueFieldAny.logicalType,
                repetition = if (valueType.isMarkedNullable) com.molo17.parquetkt.schema.Repetition.OPTIONAL
                            else com.molo17.parquetkt.schema.Repetition.REQUIRED
            )
            else -> throw IllegalArgumentException("Map values must be primitive types")
        }
        
        // Create the key-value struct (repeated)
        val keyValueStruct = NestedField.Group(
            name = "key_value",
            children = listOf(keyField, valueField),
            repetition = com.molo17.parquetkt.schema.Repetition.REPEATED,
            maxDefinitionLevel = 1,
            maxRepetitionLevel = 1,
            logicalType = LogicalType.NONE
        )
        
        // Create the outer map group
        return NestedField.Group(
            name = name,
            children = listOf(keyValueStruct),
            repetition = if (nullable) com.molo17.parquetkt.schema.Repetition.OPTIONAL
                        else com.molo17.parquetkt.schema.Repetition.REQUIRED,
            maxDefinitionLevel = if (nullable) 1 else 0,
            maxRepetitionLevel = 0,
            logicalType = LogicalType.MAP
        )
    }
}
