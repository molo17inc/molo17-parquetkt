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
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.ParquetSchema
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.Closeable

/**
 * Async Parquet file writer that uses coroutines for non-blocking I/O operations.
 * All I/O operations are executed on the IO dispatcher.
 */
class ParquetWriterAsync(
    path: String,
    schema: ParquetSchema,
    compressionCodec: CompressionCodec = CompressionCodec.SNAPPY
) : Closeable {
    
    private val writer = ParquetWriter(path, schema, compressionCodec)
    
    /**
     * Write a row group to the file asynchronously.
     */
    suspend fun write(rowGroup: RowGroup) = withContext(Dispatchers.IO) {
        writer.write(rowGroup)
    }
    
    override fun close() {
        writer.close()
    }
}
