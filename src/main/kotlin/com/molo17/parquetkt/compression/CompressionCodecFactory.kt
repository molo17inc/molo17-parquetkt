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


package com.molo17.parquetkt.compression

import com.molo17.parquetkt.schema.CompressionCodec
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.xerial.snappy.Snappy
import com.github.luben.zstd.Zstd
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

interface Compressor {
    fun compress(data: ByteArray): ByteArray
    fun decompress(data: ByteArray, uncompressedSize: Int): ByteArray
}

object CompressionCodecFactory {
    
    fun getCompressor(codec: CompressionCodec): Compressor {
        return when (codec) {
            CompressionCodec.UNCOMPRESSED -> UncompressedCodec
            CompressionCodec.SNAPPY -> SnappyCodec
            CompressionCodec.GZIP -> GzipCodec
            CompressionCodec.ZSTD -> ZstdCodec
            else -> throw UnsupportedOperationException("Compression codec $codec not yet implemented")
        }
    }
}

object UncompressedCodec : Compressor {
    override fun compress(data: ByteArray): ByteArray = data
    override fun decompress(data: ByteArray, uncompressedSize: Int): ByteArray = data
}

object SnappyCodec : Compressor {
    override fun compress(data: ByteArray): ByteArray {
        return Snappy.compress(data)
    }
    
    override fun decompress(data: ByteArray, uncompressedSize: Int): ByteArray {
        return Snappy.uncompress(data)
    }
}

object GzipCodec : Compressor {
    override fun compress(data: ByteArray): ByteArray {
        val output = ByteArrayOutputStream()
        GzipCompressorOutputStream(output).use { gzip ->
            gzip.write(data)
        }
        return output.toByteArray()
    }
    
    override fun decompress(data: ByteArray, uncompressedSize: Int): ByteArray {
        val input = ByteArrayInputStream(data)
        val output = ByteArrayOutputStream(uncompressedSize)
        
        GzipCompressorInputStream(input).use { gzip ->
            gzip.copyTo(output)
        }
        
        return output.toByteArray()
    }
}

object ZstdCodec : Compressor {
    override fun compress(data: ByteArray): ByteArray {
        return Zstd.compress(data)
    }
    
    override fun decompress(data: ByteArray, uncompressedSize: Int): ByteArray {
        return Zstd.decompress(data, uncompressedSize)
    }
}
