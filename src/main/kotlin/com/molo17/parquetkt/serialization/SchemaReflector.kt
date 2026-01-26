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
        val fields = clazz.memberProperties.map { property ->
            val name = property.name
            val type = property.returnType
            val nullable = type.isMarkedNullable
            
            reflectField(name, type, nullable)
        }
        
        return ParquetSchema.create(fields)
    }
    
    inline fun <reified T : Any> reflectSchema(): ParquetSchema {
        return reflectSchema(T::class)
    }
    
    private fun reflectField(name: String, type: KType, nullable: Boolean): DataField {
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
                if (javaType.typeName.startsWith("java.util.List") || 
                    javaType.typeName.startsWith("kotlin.collections.List")) {
                    DataField(
                        name = name,
                        dataType = ParquetType.BYTE_ARRAY,
                        logicalType = LogicalType.STRING,
                        repetition = com.molo17.parquetkt.schema.Repetition.REPEATED
                    )
                } else {
                    throw IllegalArgumentException("Unsupported type: ${javaType.typeName}")
                }
            }
        }
    }
}
