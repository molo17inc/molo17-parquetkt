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

import com.molo17.parquetkt.schema.*
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaType
import kotlin.reflect.jvm.jvmErasure

/**
 * Extended schema reflector that supports nested types (Lists, Maps, nested data classes).
 * Generates proper 3-level list structures and nested groups as per Parquet specification.
 */
object NestedSchemaReflector {
    
    fun <T : Any> reflectNestedSchema(clazz: KClass<T>): ParquetSchema {
        val fields = clazz.memberProperties.map { property ->
            val name = property.name
            val type = property.returnType
            val nullable = type.isMarkedNullable
            
            reflectNestedField(name, type, nullable)
        }
        
        return ParquetSchema.createNested(fields)
    }
    
    inline fun <reified T : Any> reflectNestedSchema(): ParquetSchema {
        return reflectNestedSchema(T::class)
    }
    
    private fun reflectNestedField(name: String, type: KType, nullable: Boolean): NestedField {
        val classifier = type.jvmErasure
        val javaType = type.javaType
        
        // Check for List types
        if (classifier == List::class) {
            return reflectListField(name, type, nullable)
        }
        
        // Check for Map types
        if (classifier == Map::class) {
            return reflectMapField(name, type, nullable)
        }
        
        // Check for primitive types
        val primitiveField = tryReflectPrimitive(name, javaType.typeName, nullable)
        if (primitiveField != null) {
            return primitiveField
        }
        
        // Check if it's a data class (nested struct)
        if (classifier.isData) {
            return reflectStructField(name, classifier, nullable)
        }
        
        throw IllegalArgumentException("Unsupported type: ${javaType.typeName}")
    }
    
    private fun reflectListField(name: String, type: KType, nullable: Boolean): NestedField {
        // Get the element type from List<T>
        val elementType = type.arguments.firstOrNull()?.type
            ?: throw IllegalArgumentException("List type must have element type parameter")
        
        val elementNullable = elementType.isMarkedNullable
        val elementField = reflectNestedField("element", elementType, elementNullable)
        
        return NestedField.createList(name, elementField, nullable, elementNullable)
    }
    
    private fun reflectMapField(name: String, type: KType, nullable: Boolean): NestedField {
        // Get key and value types from Map<K, V>
        val typeArgs = type.arguments
        if (typeArgs.size != 2) {
            throw IllegalArgumentException("Map type must have key and value type parameters")
        }
        
        val keyType = typeArgs[0].type
            ?: throw IllegalArgumentException("Map key type cannot be null")
        val valueType = typeArgs[1].type
            ?: throw IllegalArgumentException("Map value type cannot be null")
        
        // Keys must be primitive and non-nullable in Parquet
        val keyField = reflectNestedField("key", keyType, false)
        if (keyField !is NestedField.Primitive) {
            throw IllegalArgumentException("Map keys must be primitive types")
        }
        
        val valueNullable = valueType.isMarkedNullable
        val valueField = reflectNestedField("value", valueType, valueNullable)
        
        return NestedField.createMap(name, keyField, valueField, nullable, valueNullable)
    }
    
    private fun reflectStructField(name: String, clazz: KClass<*>, nullable: Boolean): NestedField {
        // Recursively reflect the nested data class
        val childFields = clazz.memberProperties.map { property ->
            val propName = property.name
            val propType = property.returnType
            val propNullable = propType.isMarkedNullable
            
            reflectNestedField(propName, propType, propNullable)
        }
        
        return NestedField.createStruct(name, childFields, nullable)
    }
    
    private fun tryReflectPrimitive(name: String, typeName: String, nullable: Boolean): NestedField.Primitive? {
        return when (typeName) {
            "boolean", "java.lang.Boolean" -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.BOOLEAN,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            "int", "java.lang.Integer" -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.INT32,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            "long", "java.lang.Long" -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.INT64,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            "float", "java.lang.Float" -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.FLOAT,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            "double", "java.lang.Double" -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.DOUBLE,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            "java.lang.String" -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.BYTE_ARRAY,
                logicalType = LogicalType.STRING,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            "byte[]", "[B" -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.BYTE_ARRAY,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            LocalDate::class.java.name -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.INT32,
                logicalType = LogicalType.DATE,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            LocalDateTime::class.java.name -> NestedField.Primitive(
                name = name,
                dataType = ParquetType.INT64,
                logicalType = LogicalType.TIMESTAMP_MILLIS,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
            else -> null
        }
    }
}
