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

data class DataField(
    val name: String,
    val dataType: ParquetType,
    val logicalType: LogicalType = LogicalType.NONE,
    val repetition: Repetition = Repetition.REQUIRED,
    val maxDefinitionLevel: Int = 0,
    val maxRepetitionLevel: Int = 0,
    val length: Int? = null,
    val precision: Int? = null,
    val scale: Int? = null
) {
    val isNullable: Boolean
        get() = repetition == Repetition.OPTIONAL
    
    val isRepeated: Boolean
        get() = repetition == Repetition.REPEATED
    
    val isRequired: Boolean
        get() = repetition == Repetition.REQUIRED
    
    fun withNullable(nullable: Boolean): DataField {
        return copy(repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED)
    }
    
    fun withRepeated(): DataField {
        return copy(repetition = Repetition.REPEATED)
    }
    
    companion object {
        fun boolean(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.BOOLEAN,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        fun int32(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT32,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        fun int64(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT64,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        fun float(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.FLOAT,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        fun double(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.DOUBLE,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        fun string(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.BYTE_ARRAY,
                logicalType = LogicalType.STRING,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        fun byteArray(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.BYTE_ARRAY,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        fun decimal(name: String, precision: Int, scale: Int, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.FIXED_LEN_BYTE_ARRAY,
                logicalType = LogicalType.DECIMAL,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED,
                precision = precision,
                scale = scale
            )
        }
        
        fun date(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT32,
                logicalType = LogicalType.DATE,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        fun timestamp(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT64,
                logicalType = LogicalType.TIMESTAMP_MILLIS,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }

        fun timestampMicros(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT64,
                logicalType = LogicalType.TIMESTAMP_MICROS,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }

        fun localTimestampMicros(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT64,
                logicalType = LogicalType.TIMESTAMP_MICROS,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }

        fun time(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT64,
                logicalType = LogicalType.TIME_MICROS,
                repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
            )
        }
        
        /**
         * Create a list field (repeated values).
         * Note: Full nested list support with proper 3-level structure is planned.
         * This provides basic repeated field support.
         */
        fun list(name: String, elementType: ParquetType, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = elementType,
                repetition = Repetition.REPEATED,
                maxRepetitionLevel = 1
            )
        }
        
        /**
         * Create a repeated string field (list of strings).
         */
        fun stringList(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.BYTE_ARRAY,
                logicalType = LogicalType.STRING,
                repetition = Repetition.REPEATED,
                maxRepetitionLevel = 1
            )
        }
        
        /**
         * Create a repeated int32 field (list of integers).
         */
        fun int32List(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT32,
                repetition = Repetition.REPEATED,
                maxRepetitionLevel = 1
            )
        }
        
        /**
         * Create a repeated int64 field (list of longs).
         */
        fun int64List(name: String, nullable: Boolean = false): DataField {
            return DataField(
                name = name,
                dataType = ParquetType.INT64,
                repetition = Repetition.REPEATED,
                maxRepetitionLevel = 1
            )
        }
    }
}
