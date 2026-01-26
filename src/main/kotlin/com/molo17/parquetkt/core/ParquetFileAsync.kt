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
import com.molo17.parquetkt.io.ParquetReaderAsync
import com.molo17.parquetkt.io.ParquetWriterAsync
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.serialization.ParquetDeserializer
import com.molo17.parquetkt.serialization.ParquetSerializer
import com.molo17.parquetkt.serialization.SchemaReflector
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.io.File

/**
 * Async API for reading and writing Parquet files using Kotlin coroutines.
 * All operations are suspend functions that can be called from coroutine contexts.
 */
object ParquetFileAsync {
    
    /**
     * Write row groups to a Parquet file asynchronously.
     */
    suspend fun write(
        path: String,
        schema: ParquetSchema,
        rowGroups: List<RowGroup>,
        compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
    ) {
        write(File(path), schema, rowGroups, compressionCodec)
    }
    
    /**
     * Write row groups to a Parquet file asynchronously.
     */
    suspend fun write(
        file: File,
        schema: ParquetSchema,
        rowGroups: List<RowGroup>,
        compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
    ) {
        ParquetWriterAsync(file.absolutePath, schema, compressionCodec).use { writer ->
            rowGroups.forEach { rowGroup ->
                writer.write(rowGroup)
            }
        }
    }
    
    /**
     * Write objects to a Parquet file asynchronously.
     * Schema is automatically inferred from the object type.
     */
    suspend inline fun <reified T : Any> writeObjects(
        path: String,
        data: List<T>,
        compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
    ) {
        writeObjects(File(path), data, compressionCodec)
    }
    
    /**
     * Write objects to a Parquet file asynchronously.
     * Schema is automatically inferred from the object type.
     */
    suspend inline fun <reified T : Any> writeObjects(
        file: File,
        data: List<T>,
        compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
    ) {
        val schema = SchemaReflector.reflectSchema<T>()
        val serializer = ParquetSerializer.create<T>()
        val rowGroup = serializer.serialize(data, schema)
        
        write(file, schema, listOf(rowGroup), compressionCodec)
    }
    
    /**
     * Read all row groups from a Parquet file asynchronously.
     */
    suspend fun read(path: String): List<RowGroup> {
        return read(File(path))
    }
    
    /**
     * Read all row groups from a Parquet file asynchronously.
     */
    suspend fun read(file: File): List<RowGroup> {
        return ParquetReaderAsync.open(file.absolutePath).use { reader ->
            reader.readAllRowGroups()
        }
    }
    
    /**
     * Read objects from a Parquet file asynchronously.
     */
    suspend inline fun <reified T : Any> readObjects(path: String): List<T> {
        return readObjects(File(path))
    }
    
    /**
     * Read objects from a Parquet file asynchronously.
     */
    suspend inline fun <reified T : Any> readObjects(file: File): List<T> {
        val rowGroups = read(file)
        val deserializer = ParquetDeserializer.create<T>()
        
        return rowGroups.flatMap { rowGroup ->
            deserializer.deserialize(rowGroup)
        }
    }
    
    /**
     * Read objects from a Parquet file as a Flow for streaming processing.
     */
    inline fun <reified T : Any> readObjectsAsFlow(path: String): Flow<T> {
        return readObjectsAsFlow(File(path))
    }
    
    /**
     * Read objects from a Parquet file as a Flow for streaming processing.
     */
    inline fun <reified T : Any> readObjectsAsFlow(file: File): Flow<T> = flow {
        ParquetReaderAsync.open(file.absolutePath).use { reader ->
            val deserializer = ParquetDeserializer.create<T>()
            
            for (i in 0 until reader.rowGroupCount) {
                val rowGroup = reader.readRowGroup(i)
                val objects = deserializer.deserialize(rowGroup)
                objects.forEach { obj ->
                    emit(obj)
                }
            }
        }
    }
    
    /**
     * Read row groups from a Parquet file as a Flow for streaming processing.
     */
    fun readAsFlow(path: String): Flow<RowGroup> {
        return readAsFlow(File(path))
    }
    
    /**
     * Read row groups from a Parquet file as a Flow for streaming processing.
     */
    fun readAsFlow(file: File): Flow<RowGroup> = flow {
        ParquetReaderAsync.open(file.absolutePath).use { reader ->
            for (i in 0 until reader.rowGroupCount) {
                emit(reader.readRowGroup(i))
            }
        }
    }
    
    /**
     * Read the schema from a Parquet file asynchronously.
     */
    suspend fun readSchema(path: String): ParquetSchema {
        return readSchema(File(path))
    }
    
    /**
     * Read the schema from a Parquet file asynchronously.
     */
    suspend fun readSchema(file: File): ParquetSchema {
        return ParquetReaderAsync.open(file.absolutePath).use { reader ->
            reader.schema
        }
    }
}
