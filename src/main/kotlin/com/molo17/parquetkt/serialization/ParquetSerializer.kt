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

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible

class ParquetSerializer<T : Any>(
    private val clazz: KClass<T>
) {
    private val properties: List<KProperty1<T, *>> = clazz.memberProperties.toList()
    
    fun serialize(data: List<T>, schema: ParquetSchema): RowGroup {
        val columns = schema.fields.map { field ->
            serializeColumn(data, field)
        }
        return RowGroup(schema, columns)
    }
    
    private fun serializeColumn(data: List<T>, field: DataField): DataColumn<*> {
        val property = properties.find { it.name == field.name }
            ?: throw IllegalArgumentException("Property ${field.name} not found in class ${clazz.simpleName}")
        
        property.isAccessible = true
        
        val values = data.map { obj ->
            property.get(obj)
        }
        
        return DataColumn.create(field, values)
    }
    
    companion object {
        inline fun <reified T : Any> create(): ParquetSerializer<T> {
            return ParquetSerializer(T::class)
        }
    }
}

class ParquetDeserializer<T : Any>(
    private val clazz: KClass<T>
) {
    fun deserialize(rowGroup: RowGroup): List<T> {
        val constructor = clazz.constructors.first()
        val parameters = constructor.parameters
        
        return (0 until rowGroup.rowCount).map { rowIndex ->
            val row = rowGroup.getRow(rowIndex)
            
            val args = parameters.map { param ->
                val value = row[param.name]
                when {
                    value == null -> if (param.type.isMarkedNullable) null 
                        else throw IllegalStateException("Non-nullable parameter ${param.name} is null")
                    // Convert ByteArray to String for String parameters
                    value is ByteArray && param.type.classifier == String::class -> 
                        String(value, Charsets.UTF_8)
                    else -> value
                }
            }.toTypedArray()
            
            constructor.call(*args)
        }
    }
    
    fun deserializeSequence(rowGroups: List<RowGroup>): Sequence<T> = sequence {
        rowGroups.forEach { rowGroup ->
            yieldAll(deserialize(rowGroup))
        }
    }
    
    companion object {
        inline fun <reified T : Any> create(): ParquetDeserializer<T> {
            return ParquetDeserializer(T::class)
        }
    }
}
