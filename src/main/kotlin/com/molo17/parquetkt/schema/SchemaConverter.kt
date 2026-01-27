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

import com.molo17.parquetkt.thrift.ConvertedType
import com.molo17.parquetkt.thrift.FieldRepetitionType
import com.molo17.parquetkt.thrift.SchemaElement

object SchemaConverter {
    
    fun toThriftSchema(schema: ParquetSchema): List<SchemaElement> {
        val elements = mutableListOf<SchemaElement>()
        
        // Root element
        elements.add(SchemaElement(
            name = "schema",
            numChildren = schema.fieldCount,
            repetitionType = null,
            type = null
        ))
        
        // Add field elements
        schema.fields.forEach { field ->
            elements.add(toThriftSchemaElement(field))
        }
        
        return elements
    }
    
    fun fromThriftSchema(elements: List<SchemaElement>): ParquetSchema {
        require(elements.isNotEmpty()) { "Schema elements cannot be empty" }
        require(elements[0].name == "schema") { "First element must be root schema" }
        
        val fields = elements.drop(1).map { fromThriftSchemaElement(it) }
        return ParquetSchema.create(fields)
    }
    
    private fun toThriftSchemaElement(field: DataField): SchemaElement {
        return SchemaElement(
            type = field.dataType,
            typeLength = field.length,
            repetitionType = when (field.repetition) {
                Repetition.REQUIRED -> FieldRepetitionType.REQUIRED
                Repetition.OPTIONAL -> FieldRepetitionType.OPTIONAL
                Repetition.REPEATED -> FieldRepetitionType.REPEATED
            },
            name = field.name,
            numChildren = null,
            convertedType = toConvertedType(field.logicalType),
            scale = field.scale,
            precision = field.precision,
            fieldId = null,
            logicalType = toLogicalTypeAnnotation(field.logicalType, field.precision, field.scale)
        )
    }
    
    private fun fromThriftSchemaElement(element: SchemaElement): DataField {
        val repetition = when (element.repetitionType) {
            FieldRepetitionType.REQUIRED -> Repetition.REQUIRED
            FieldRepetitionType.OPTIONAL -> Repetition.OPTIONAL
            FieldRepetitionType.REPEATED -> Repetition.REPEATED
            null -> Repetition.REQUIRED
        }
        
        // Calculate max levels based on repetition type
        // For REPEATED fields, we assume they're lists with the standard 3-level structure
        val maxRepetitionLevel = if (repetition == Repetition.REPEATED) 1 else 0
        val maxDefinitionLevel = when (repetition) {
            Repetition.OPTIONAL -> 1
            Repetition.REPEATED -> 2 // For lists: 0=null list, 1=empty list, 2=element present
            Repetition.REQUIRED -> 0
        }
        
        return DataField(
            name = element.name,
            dataType = element.type ?: throw IllegalArgumentException("Type is required"),
            logicalType = fromConvertedType(element.convertedType),
            repetition = repetition,
            maxRepetitionLevel = maxRepetitionLevel,
            maxDefinitionLevel = maxDefinitionLevel,
            length = element.typeLength,
            precision = element.precision,
            scale = element.scale
        )
    }
    
    private fun toConvertedType(logicalType: LogicalType): ConvertedType? {
        return when (logicalType) {
            LogicalType.NONE -> null
            LogicalType.STRING -> ConvertedType.UTF8
            LogicalType.ENUM -> ConvertedType.ENUM
            LogicalType.DECIMAL -> ConvertedType.DECIMAL
            LogicalType.DATE -> ConvertedType.DATE
            LogicalType.TIME_MILLIS -> ConvertedType.TIME_MILLIS
            LogicalType.TIME_MICROS -> ConvertedType.TIME_MICROS
            LogicalType.TIMESTAMP_MILLIS -> ConvertedType.TIMESTAMP_MILLIS
            LogicalType.TIMESTAMP_MICROS -> ConvertedType.TIMESTAMP_MICROS
            LogicalType.UINT_8 -> ConvertedType.UINT_8
            LogicalType.UINT_16 -> ConvertedType.UINT_16
            LogicalType.UINT_32 -> ConvertedType.UINT_32
            LogicalType.UINT_64 -> ConvertedType.UINT_64
            LogicalType.INT_8 -> ConvertedType.INT_8
            LogicalType.INT_16 -> ConvertedType.INT_16
            LogicalType.LIST -> ConvertedType.LIST
            LogicalType.MAP -> ConvertedType.MAP
            LogicalType.INT_32 -> ConvertedType.INT_32
            LogicalType.INT_64 -> ConvertedType.INT_64
            LogicalType.JSON -> ConvertedType.JSON
            LogicalType.BSON -> ConvertedType.BSON
            LogicalType.UUID -> null
            LogicalType.INTERVAL -> ConvertedType.INTERVAL
        }
    }
    
    private fun fromConvertedType(convertedType: ConvertedType?): LogicalType {
        return when (convertedType) {
            null -> LogicalType.NONE
            ConvertedType.UTF8 -> LogicalType.STRING
            ConvertedType.ENUM -> LogicalType.ENUM
            ConvertedType.DECIMAL -> LogicalType.DECIMAL
            ConvertedType.DATE -> LogicalType.DATE
            ConvertedType.TIME_MILLIS -> LogicalType.TIME_MILLIS
            ConvertedType.TIME_MICROS -> LogicalType.TIME_MICROS
            ConvertedType.TIMESTAMP_MILLIS -> LogicalType.TIMESTAMP_MILLIS
            ConvertedType.TIMESTAMP_MICROS -> LogicalType.TIMESTAMP_MICROS
            ConvertedType.UINT_8 -> LogicalType.UINT_8
            ConvertedType.UINT_16 -> LogicalType.UINT_16
            ConvertedType.UINT_32 -> LogicalType.UINT_32
            ConvertedType.UINT_64 -> LogicalType.UINT_64
            ConvertedType.INT_8 -> LogicalType.INT_8
            ConvertedType.INT_16 -> LogicalType.INT_16
            ConvertedType.INT_32 -> LogicalType.INT_32
            ConvertedType.INT_64 -> LogicalType.INT_64
            ConvertedType.JSON -> LogicalType.JSON
            ConvertedType.BSON -> LogicalType.BSON
            ConvertedType.INTERVAL -> LogicalType.INTERVAL
            ConvertedType.MAP, ConvertedType.MAP_KEY_VALUE, ConvertedType.LIST -> LogicalType.NONE
        }
    }
    
    private fun toLogicalTypeAnnotation(logicalType: LogicalType, precision: Int?, scale: Int?): com.molo17.parquetkt.thrift.LogicalTypeAnnotation? {
        return when (logicalType) {
            LogicalType.NONE -> null
            LogicalType.STRING -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.String
            LogicalType.ENUM -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Enum
            LogicalType.DATE -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Date
            LogicalType.JSON -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Json
            LogicalType.BSON -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Bson
            LogicalType.UUID -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Uuid
            LogicalType.DECIMAL -> {
                if (precision != null && scale != null) {
                    com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Decimal(precision, scale)
                } else null
            }
            LogicalType.TIME_MILLIS -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Time(
                isAdjustedToUTC = true,
                unit = com.molo17.parquetkt.thrift.TimeUnit.MILLIS
            )
            LogicalType.TIME_MICROS -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Time(
                isAdjustedToUTC = true,
                unit = com.molo17.parquetkt.thrift.TimeUnit.MICROS
            )
            LogicalType.TIMESTAMP_MILLIS -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Timestamp(
                isAdjustedToUTC = true,
                unit = com.molo17.parquetkt.thrift.TimeUnit.MILLIS
            )
            LogicalType.TIMESTAMP_MICROS -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Timestamp(
                isAdjustedToUTC = true,
                unit = com.molo17.parquetkt.thrift.TimeUnit.MICROS
            )
            LogicalType.UINT_8 -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Integer(8, false)
            LogicalType.UINT_16 -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Integer(16, false)
            LogicalType.UINT_32 -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Integer(32, false)
            LogicalType.UINT_64 -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Integer(64, false)
            LogicalType.INT_8 -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Integer(8, true)
            LogicalType.INT_16 -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Integer(16, true)
            LogicalType.INT_32 -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Integer(32, true)
            LogicalType.INT_64 -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Integer(64, true)
            LogicalType.LIST -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.List
            LogicalType.MAP -> com.molo17.parquetkt.thrift.LogicalTypeAnnotation.Map
            LogicalType.INTERVAL -> null // INTERVAL doesn't have a modern LogicalType representation
        }
    }
}
