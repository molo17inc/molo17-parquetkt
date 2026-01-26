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


package com.molo17.parquetkt.core

import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.serialization.ParquetDeserializer
import com.molo17.parquetkt.serialization.ParquetSerializer
import com.molo17.parquetkt.serialization.SchemaReflector
import java.io.File

object ParquetFile {
    
    fun write(
        file: File,
        schema: ParquetSchema,
        rowGroups: List<RowGroup>,
        compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
    ) {
        ParquetWriter.create(file, schema, compressionCodec).use { writer ->
            rowGroups.forEach { writer.write(it) }
        }
    }
    
    fun write(
        path: String,
        schema: ParquetSchema,
        rowGroups: List<RowGroup>,
        compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
    ) {
        write(File(path), schema, rowGroups, compressionCodec)
    }
    
    inline fun <reified T : Any> writeObjects(
        file: File,
        data: List<T>,
        compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
    ) {
        val schema = SchemaReflector.reflectSchema<T>()
        val serializer = ParquetSerializer.create<T>()
        val rowGroup = serializer.serialize(data, schema)
        
        write(file, schema, listOf(rowGroup), compressionCodec)
    }
    
    inline fun <reified T : Any> writeObjects(
        path: String,
        data: List<T>,
        compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
    ) {
        writeObjects(File(path), data, compressionCodec)
    }
    
    fun read(file: File): List<RowGroup> {
        return ParquetReader.open(file).use { reader ->
            reader.readAllRowGroups()
        }
    }
    
    fun read(path: String): List<RowGroup> {
        return read(File(path))
    }
    
    inline fun <reified T : Any> readObjects(file: File): List<T> {
        return ParquetReader.open(file).use { reader ->
            val rowGroups = reader.readAllRowGroups()
            val deserializer = ParquetDeserializer.create<T>()
            deserializer.deserializeSequence(rowGroups).toList()
        }
    }
    
    inline fun <reified T : Any> readObjects(path: String): List<T> {
        return readObjects(File(path))
    }
    
    fun readSchema(file: File): ParquetSchema {
        return ParquetReader.open(file).use { reader ->
            reader.schema
        }
    }
    
    fun readSchema(path: String): ParquetSchema {
        return readSchema(File(path))
    }
    
    fun readRowsAsSequence(file: File): Sequence<Map<String, Any?>> {
        val reader = ParquetReader.open(file)
        return reader.readAllRows()
    }
    
    fun readRowsAsSequence(path: String): Sequence<Map<String, Any?>> {
        return readRowsAsSequence(File(path))
    }
}
