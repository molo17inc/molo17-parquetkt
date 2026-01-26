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


package com.molo17.parquetkt.io

import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.ParquetSchema
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.Closeable

/**
 * Async Parquet file reader that uses coroutines for non-blocking I/O operations.
 * All I/O operations are executed on the IO dispatcher.
 */
class ParquetReaderAsync private constructor(
    private val reader: ParquetReader
) : Closeable {
    
    val schema: ParquetSchema
        get() = reader.schema
    
    val rowGroupCount: Int
        get() = reader.rowGroupCount
    
    val totalRowCount: Long
        get() = reader.totalRowCount
    
    /**
     * Read a specific row group asynchronously.
     */
    suspend fun readRowGroup(index: Int): RowGroup = withContext(Dispatchers.IO) {
        reader.readRowGroup(index)
    }
    
    /**
     * Read all row groups asynchronously.
     */
    suspend fun readAllRowGroups(): List<RowGroup> = withContext(Dispatchers.IO) {
        reader.readAllRowGroups()
    }
    
    override fun close() {
        reader.close()
    }
    
    companion object {
        /**
         * Open a Parquet file for async reading.
         */
        suspend fun open(path: String): ParquetReaderAsync = withContext(Dispatchers.IO) {
            val reader = ParquetReader.open(path)
            ParquetReaderAsync(reader)
        }
    }
}
