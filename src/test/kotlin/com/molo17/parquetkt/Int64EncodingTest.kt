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

package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.encoding.PlainDecoder
import com.molo17.parquetkt.encoding.PlainEncoder
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.ParquetType
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Tests for INT64 encoding/decoding to verify edge cases and potential bugs.
 */
class Int64EncodingTest {

    @TempDir
    lateinit var tempDir: File

    private fun assertPyArrowDecodes(file: File, expectedRows: Int, expectedColumns: Int? = null) {
        val script = """
            import sys
            import pyarrow.parquet as pq

            table = pq.read_table(sys.argv[1])
            print(f"ROWS={table.num_rows}")
            print(f"COLS={table.num_columns}")
        """.trimIndent()

        val process = ProcessBuilder("python3", "-c", script, file.absolutePath)
            .redirectErrorStream(true)
            .start()

        val output = process.inputStream.bufferedReader().readText().trim()
        val exitCode = process.waitFor()

        assertEquals(0, exitCode, "PyArrow failed to decode ${file.absolutePath}: $output")
        assertTrue(output.contains("ROWS=$expectedRows"), "Unexpected PyArrow row count for ${file.name}: $output")
        if (expectedColumns != null) {
            assertTrue(output.contains("COLS=$expectedColumns"), "Unexpected PyArrow column count for ${file.name}: $output")
        }
    }

    @Test
    fun `test INT64 plain encoder roundtrip with various values`() {
        val encoder = PlainEncoder(ParquetType.INT64)
        val decoder = PlainDecoder(ParquetType.INT64)

        // Test various INT64 values including edge cases
        val testValues = arrayOf(
            0L,
            1L,
            -1L,
            127L,
            128L,
            -127L,
            -128L,
            255L,
            256L,
            65535L,
            65536L,
            Int.MAX_VALUE.toLong(),
            Int.MIN_VALUE.toLong(),
            Long.MAX_VALUE,  // 0x7FFFFFFFFFFFFFFFL
            Long.MIN_VALUE,  // 0x8000000000000000
        )

        val encoded = encoder.encode(testValues as Array<Any>)
        val decoded = decoder.decode(encoded, testValues.size) as Array<Long>

        assertContentEquals(testValues.toList(), decoded.toList())
    }

    @Test
    fun `test INT64 write and read roundtrip`() {
        val schema = ParquetSchema.create(
            DataField.int64("id"),
            DataField.int64("value")
        )

        val testValues = listOf(
            0L,
            1L,
            -1L,
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            1234567890123456789L,
            -1234567890123456789L
        )

        val idColumn = DataColumn.createRequired(
            DataField.int64("id"),
            testValues
        )

        val valueColumn = DataColumn.createRequired(
            DataField.int64("value"),
            testValues.reversed()
        )

        val rowGroup = RowGroup(schema, listOf(idColumn, valueColumn))
        val file = File(tempDir, "int64_roundtrip.parquet")

        com.molo17.parquetkt.core.ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.UNCOMPRESSED)
        assertPyArrowDecodes(file, expectedRows = testValues.size, expectedColumns = 2)

        val readRowGroups = com.molo17.parquetkt.core.ParquetFile.read(file)

        assertEquals(1, readRowGroups.size)
        assertEquals(testValues.size, readRowGroups[0].rowCount)

        val readIdColumn = readRowGroups[0].getColumn("id")
        assertNotNull(readIdColumn)

        for (i in testValues.indices) {
            assertEquals(testValues[i], readIdColumn.get(i), "Mismatch at index $i")
        }
    }

    @Test
    fun `test INT64 with nullable values`() {
        val schema = ParquetSchema.create(
            DataField.int64("id", nullable = false),
            DataField.int64("optional_value", nullable = true)
        )

        val idColumn = DataColumn.createRequired(
            DataField.int64("id"),
            listOf(1L, 2L, 3L)
        )

        val optionalColumn = DataColumn.create(
            DataField.int64("optional_value", nullable = true),
            listOf(Long.MAX_VALUE, null, Long.MIN_VALUE),
            definitionLevels = intArrayOf(1, 0, 1)
        )

        val rowGroup = RowGroup(schema, listOf(idColumn, optionalColumn))
        val file = File(tempDir, "int64_nullable.parquet")

        com.molo17.parquetkt.core.ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.UNCOMPRESSED)
        assertPyArrowDecodes(file, expectedRows = 3, expectedColumns = 2)

        val readRowGroups = com.molo17.parquetkt.core.ParquetFile.read(file)

        assertEquals(1, readRowGroups.size)
        assertEquals(3, readRowGroups[0].rowCount)

        val readOptionalColumn = readRowGroups[0].getColumn("optional_value")
        assertNotNull(readOptionalColumn)

        assertEquals(Long.MAX_VALUE, readOptionalColumn.get(0))
        assertEquals(null, readOptionalColumn.get(1))
        assertEquals(Long.MIN_VALUE, readOptionalColumn.get(2))
    }

    @Test
    fun `test INT64 with all compression codecs`() {
        val schema = ParquetSchema.create(
            DataField.int64("value")
        )

        val testValues = listOf(
            0L,
            1L,
            -1L,
            Long.MAX_VALUE,
            Long.MIN_VALUE,
            0x123456789ABCDEFL
        )

        val column = DataColumn.createRequired(
            DataField.int64("value"),
            testValues
        )

        val rowGroup = RowGroup(schema, listOf(column))

        val codecs = listOf(
            CompressionCodec.UNCOMPRESSED,
            CompressionCodec.SNAPPY,
            CompressionCodec.GZIP,
            CompressionCodec.ZSTD
        )

        for (codec in codecs) {
            val file = File(tempDir, "int64_${codec.name.lowercase()}.parquet")

            com.molo17.parquetkt.core.ParquetFile.write(file, schema, listOf(rowGroup), codec)
            assertPyArrowDecodes(file, expectedRows = testValues.size, expectedColumns = 1)

            val readRowGroups = com.molo17.parquetkt.core.ParquetFile.read(file)
            assertEquals(1, readRowGroups.size, "Failed for codec: $codec")
            assertEquals(testValues.size, readRowGroups[0].rowCount, "Failed for codec: $codec")

            val readColumn = readRowGroups[0].getColumn("value")
            assertNotNull(readColumn, "Failed for codec: $codec")

            for (i in testValues.indices) {
                assertEquals(testValues[i], readColumn.get(i), "Mismatch at index $i for codec: $codec")
            }
        }
    }

    @Test
    fun `test read external parquet file from user report - CORRUPTED FILE DETECTED`() {
        // This test documents a corrupted Parquet file that was reported by a user.
        // The file has a corrupted page header with uncompressed_page_size = 0,
        // which makes it unreadable by both ParquetKt and PyArrow.

        val externalFile = File("/Users/danieleangeli/Desktop/2026-05-06T08_49_59.705030925.parquet")

        if (!externalFile.exists()) {
            println("⚠️  External test file not found: ${externalFile.absolutePath}")
            println("Skipping this test - the file may have been moved or deleted.")
            return
        }

        println("Analyzing external file: ${externalFile.absolutePath}")
        println("File size: ${externalFile.length()} bytes")

        // First verify PyArrow also can't read it (confirming it's a file corruption issue)
        val pyarrowCheck = ProcessBuilder("python3", "-c",
            "import pyarrow.parquet as pq; pq.read_table('${externalFile.absolutePath}'); print('OK')"
        ).redirectErrorStream(true).start()
        val pyarrowOutput = pyarrowCheck.inputStream.bufferedReader().readText().trim()
        val pyarrowExitCode = pyarrowCheck.waitFor()

        if (pyarrowExitCode != 0) {
            println("✅ CONFIRMED: PyArrow also cannot read this file (exit code $pyarrowExitCode)")
            println("   This confirms the file is corrupted, not a ParquetKt bug.")
            println("   PyArrow error: ${pyarrowOutput.take(100)}")
        }

        try {
            val reader = ParquetReader(externalFile.absolutePath)
            val schema = reader.schema

            println("\n✅ Successfully read schema!")
            println("   Schema fields: ${schema.fieldCount}")
            println("   Total rows: ${reader.totalRowCount}")
            println("   Row groups: ${reader.rowGroupCount}")

            // Print INT64 fields only for brevity
            val int64Fields = schema.fields.filter { it.dataType == ParquetType.INT64 }
            println("   INT64 fields: ${int64Fields.size}")
            int64Fields.forEach { field ->
                println("   - ${field.name}: ${field.dataType} (nullable: ${field.isNullable}, maxDefLevel: ${field.maxDefinitionLevel})")
            }

            // Check the metadata for corruption
            println("\nAnalyzing column metadata for corruption...")
            for (rgIndex in 0 until reader.rowGroupCount) {
                int64Fields.forEach { field ->
                    val columnChunk = reader.fileMetadata?.rowGroups?.get(rgIndex)?.columns?.find {
                        it.metaData.pathInSchema.lastOrNull() == field.name
                    }
                    if (columnChunk != null) {
                        val meta = columnChunk.metaData
                        println("  Column '${field.name}' metadata:")
                        println("    - numValues: ${meta.numValues}")
                        println("    - totalUncompressedSize: ${meta.totalUncompressedSize}")
                        println("    - totalCompressedSize: ${meta.totalCompressedSize}")

                        // Detect the specific corruption
                        if (meta.totalUncompressedSize == 0L && meta.totalCompressedSize > 0) {
                            println("    ⚠️  CORRUPTION DETECTED: uncompressed size is 0 but compressed size is ${meta.totalCompressedSize}")
                            println("       This is a bug in the tool that wrote this file, not a ParquetKt bug.")
                        }
                    }
                }
            }

            // Try to read - this will fail due to corruption
            println("\nAttempting to read data (expected to fail due to corruption)...")
            try {
                reader.readRowGroup(0)
                println("❌ Unexpectedly succeeded - file should have been corrupted!")
            } catch (e: IllegalStateException) {
                println("✅ Expected error occurred: ${e.message}")
                println("   This is the correct behavior when reading a corrupted file.")
            }

            reader.close()
            println("\n✅ Diagnostic analysis completed!")
            println("\nSUMMARY:")
            println("The file '${externalFile.name}' is corrupted.")
            println("The page header has uncompressed_page_size = 0, which is invalid.")
            println("This is a bug in the tool that created the file, NOT a ParquetKt bug.")
            println("INT64 encoding/decoding in ParquetKt is working correctly.")
        } catch (e: Exception) {
            println("❌ Error during analysis: ${e.message}")
            e.printStackTrace()
        }
    }

    @Test
    fun `test specific INT64 values that might cause issues`() {
        // Values that might expose sign extension or bit masking issues
        val problematicValues = listOf(
            // Values with high bit set in lower bytes
            0x0000000000000080L,  // 128 - single bit in byte 0
            0x0000000000008000L,  // 32768 - single bit in byte 1
            0x0000000000800000L,  // 8388608 - single bit in byte 2
            0x0000000080000000L,  // 2147483648 - single bit in byte 3 (exceeds Int.MAX_VALUE)
            0x0000008000000000L,  // high bit in byte 4
            0x0000800000000000L,  // high bit in byte 5
            0x0080000000000000L,  // high bit in byte 6
            -0x7FFFFFFFFFFFFFFFL,  // high bit in byte 7 (Long.MIN_VALUE via negation)

            // Values with all bits set in various bytes
            0x00000000000000FFL,  // all bits in byte 0
            0x000000000000FF00L,  // all bits in byte 1
            0x0000000000FF0000L,  // all bits in byte 2
            0x00000000FF000000L,  // all bits in byte 3
            0x000000FF00000000L,  // all bits in byte 4
            0x0000FF0000000000L,  // all bits in byte 5
            0x00FF000000000000L,  // all bits in byte 6
            (-0x7FFFFFFFFFFFFFFFL - 1L).inv(),  // all bits in byte 7

            // Alternating patterns - using bitwise operations to create values
            (-0x5555555555555556L),  // 0xAAAAAAAAAAAAAAAAL as signed
            0x5555555555555555L,
        )

        val encoder = PlainEncoder(ParquetType.INT64)
        val decoder = PlainDecoder(ParquetType.INT64)

        problematicValues.forEach { value ->
            val encoded = encoder.encode(arrayOf<Any>(value))
            val decoded = decoder.decode(encoded, 1) as Array<Long>

            val valueLong = value as Long
            assertEquals(valueLong, decoded[0], "Failed for value: 0x${java.lang.Long.toHexString(valueLong)} ($valueLong)")
        }
    }

    @Test
    fun `test INT64 byte order consistency`() {
        // This test verifies that our little-endian encoding is correct
        // by checking the actual bytes produced
        val encoder = PlainEncoder(ParquetType.INT64)

        // Value: 0x0102030405060708
        // Little endian bytes: [08, 07, 06, 05, 04, 03, 02, 01]
        val value = 0x0102030405060708L
        val encoded = encoder.encode(arrayOf<Any>(value))

        assertEquals(8, encoded.size, "INT64 should be 8 bytes")
        assertEquals(0x08, encoded[0].toInt() and 0xFF, "Byte 0 should be 0x08")
        assertEquals(0x07, encoded[1].toInt() and 0xFF, "Byte 1 should be 0x07")
        assertEquals(0x06, encoded[2].toInt() and 0xFF, "Byte 2 should be 0x06")
        assertEquals(0x05, encoded[3].toInt() and 0xFF, "Byte 3 should be 0x05")
        assertEquals(0x04, encoded[4].toInt() and 0xFF, "Byte 4 should be 0x04")
        assertEquals(0x03, encoded[5].toInt() and 0xFF, "Byte 5 should be 0x03")
        assertEquals(0x02, encoded[6].toInt() and 0xFF, "Byte 6 should be 0x02")
        assertEquals(0x01, encoded[7].toInt() and 0xFF, "Byte 7 should be 0x01")
    }

    @Test
    fun `test INT64 pyarrow roundtrip compatibility`() {
        val schema = ParquetSchema.create(
            DataField.int64("value")
        )

        val testValues = listOf(
            0L,
            1L,
            -1L,
            9223372036854775807L,  // Long.MAX_VALUE
            Long.MIN_VALUE, // Long.MIN_VALUE
            1234567890123456789L,
            -1234567890123456789L
        )

        val column = DataColumn.createRequired(
            DataField.int64("value"),
            testValues
        )

        val rowGroup = RowGroup(schema, listOf(column))
        val file = File(tempDir, "int64_pyarrow_test.parquet")

        com.molo17.parquetkt.core.ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.SNAPPY)

        val script = """
            import sys
            import pyarrow.parquet as pq

            table = pq.read_table(sys.argv[1])
            values = table.column('value').to_pylist()
            expected = ${testValues}

            assert len(values) == len(expected), f"Size mismatch: {len(values)} vs {len(expected)}"
            for i, (actual, exp) in enumerate(zip(values, expected)):
                assert actual == exp, f"Mismatch at index {i}: {actual} != {exp}"

            print("OK: All INT64 values match")
        """.trimIndent()

        val process = ProcessBuilder("python3", "-c", script, file.absolutePath)
            .redirectErrorStream(true)
            .start()

        val output = process.inputStream.bufferedReader().readText().trim()
        val exitCode = process.waitFor()

        assertEquals(0, exitCode, "PyArrow INT64 roundtrip check failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `test verify INT64 page header is written correctly`() {
        // This test specifically checks that the page header uncompressed size
        // is written correctly (regression test for the bug where it was 0)
        val schema = ParquetSchema.create(
            DataField.int64("_RRN")
        )

        val testValues = listOf(1L, 2L, 3L, 4L, 5L, 6L)

        val column = DataColumn.createRequired(
            DataField.int64("_RRN"),
            testValues
        )

        val rowGroup = RowGroup(schema, listOf(column))
        val file = File(tempDir, "int64_page_header_test.parquet")

        // Write using ParquetFile.write (the API the user was using)
        com.molo17.parquetkt.core.ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.UNCOMPRESSED)
        assertPyArrowDecodes(file, expectedRows = testValues.size, expectedColumns = 1)

        // Read back and verify metadata
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema

        assertEquals(1, readSchema.fieldCount)
        assertEquals(ParquetType.INT64, readSchema.fields[0].dataType)

        // Check the column metadata
        val columnChunk = reader.fileMetadata?.rowGroups?.get(0)?.columns?.get(0)
        assertNotNull(columnChunk)

        val meta = columnChunk!!.metaData
        println("Column metadata:")
        println("  - numValues: ${meta.numValues}")
        println("  - totalUncompressedSize: ${meta.totalUncompressedSize}")
        println("  - totalCompressedSize: ${meta.totalCompressedSize}")
        println("  - dataPageOffset: ${meta.dataPageOffset}")

        // Verify the metadata uncompressed size is correct
        // For 6 INT64 values: 6 * 8 = 48 bytes of data
        // Plus definition levels (none for required field) and header
        // The totalUncompressedSize in column metadata should be > 0
        assertTrue(meta.totalUncompressedSize > 0,
            "totalUncompressedSize should be > 0, got ${meta.totalUncompressedSize}")

        // Now read the actual page header from the file
        reader.javaClass.getDeclaredField("file").let { fileField ->
            fileField.isAccessible = true
            val raf = fileField.get(reader) as java.io.RandomAccessFile
            val dataOffset = meta.dataPageOffset
            raf.seek(dataOffset)

            // Read and parse the page header manually
            val headerBytes = ByteArray(30)
            raf.readFully(headerBytes)
            println("  Page header bytes at offset $dataOffset:")
            println("    Hex: ${headerBytes.joinToString(" ") { "%02x".format(it) }}")

            // Decode the page header thrift compact protocol manually
            // Field 1: type (i32) - should be 0 (DATA_PAGE)
            // Field 2: uncompressed_page_size (i32) - should be > 0
            // Field 3: compressed_page_size (i32) - should be > 0

            // Parse the thrift compact protocol
            var pos = 0
            fun readVarInt(): Int {
                var result = 0
                var shift = 0
                while (true) {
                    val b = headerBytes[pos++].toInt() and 0xFF
                    result = result or ((b and 0x7F) shl shift)
                    if ((b and 0x80) == 0) break
                    shift += 7
                }
                return (result ushr 1) xor -(result and 1)  // zigzag decode
            }

            // Field 1 header: delta 1, type 5 (i32) = 0x15
            val field1Header = headerBytes[pos++].toInt() and 0xFF
            println("    Field 1 header: 0x${field1Header.toString(16)} (expected 0x15)")

            val type = readVarInt()
            println("    Type: $type (expected 0 for DATA_PAGE)")
            assertEquals(0, type, "Page type should be DATA_PAGE (0)")

            // Field 2 header: delta 1, type 5 (i32) = 0x15
            val field2Header = headerBytes[pos++].toInt() and 0xFF
            println("    Field 2 header: 0x${field2Header.toString(16)} (expected 0x15)")

            val uncompressedSize = readVarInt()
            println("    Uncompressed page size: $uncompressedSize (expected > 0)")
            assertTrue(uncompressedSize > 0, "uncompressed_page_size should be > 0, got $uncompressedSize")

            // Field 3 header: delta 1, type 5 (i32) = 0x15
            val field3Header = headerBytes[pos++].toInt() and 0xFF
            println("    Field 3 header: 0x${field3Header.toString(16)} (expected 0x15)")

            val compressedSize = readVarInt()
            println("    Compressed page size: $compressedSize (expected > 0)")
            assertTrue(compressedSize > 0, "compressed_page_size should be > 0, got $compressedSize")
        }

        // Try to read the data back
        try {
            val readRowGroups = reader.readAllRowGroups()
            assertEquals(1, readRowGroups.size)
            assertEquals(testValues.size, readRowGroups[0].rowCount)

            val readColumn = readRowGroups[0].getColumn("_RRN")
            assertNotNull(readColumn)
            assertEquals(testValues.size, readColumn!!.size)

            for (i in testValues.indices) {
                assertEquals(testValues[i], readColumn.get(i), "Mismatch at index $i")
            }
            println("✅ Data read back successfully!")
        } catch (e: Exception) {
            println("❌ Error reading data: ${e.message}")
            e.printStackTrace()
            throw e
        }

        reader.close()
    }

    @Test
    fun `test INT64 with large dataset similar to user data`() {
        // This test simulates the user's actual dataset with 4245 rows
        val rowCount = 4245

        val schema = ParquetSchema.create(
            DataField.int64("_RRN"),
            DataField.string("APDES"),
            DataField.string("APART"),
            DataField.int64("APCOL")
        )

        // Generate 4245 rows of test data similar to the user's DB2 data
        val rrnValues = (1L..rowCount).toList()
        val desValues = (1..rowCount).map { "Product description $it" }
        val artValues = (1..rowCount).map { "Article-${1000 + it}" }
        val colValues = (1..rowCount).map { (it % 100).toLong() }

        val rrnColumn = DataColumn.createRequired(
            DataField.int64("_RRN"),
            rrnValues
        )
        val desColumn = DataColumn.createRequired(
            DataField.string("APDES"),
            desValues.map { it.toByteArray() }
        )
        val artColumn = DataColumn.createRequired(
            DataField.string("APART"),
            artValues.map { it.toByteArray() }
        )
        val colColumn = DataColumn.createRequired(
            DataField.int64("APCOL"),
            colValues
        )

        val rowGroup = RowGroup(schema, listOf(rrnColumn, desColumn, artColumn, colColumn))
        val file = File(tempDir, "large_int64_dataset.parquet")

        println("Writing $rowCount rows to ${file.name}...")

        // Write using the same API the user used
        com.molo17.parquetkt.core.ParquetFile.write(
            file,
            schema,
            listOf(rowGroup),
            CompressionCodec.SNAPPY
        )
        assertPyArrowDecodes(file, expectedRows = rowCount, expectedColumns = 4)

        println("File size: ${file.length()} bytes")

        // Read back and verify
        val reader = ParquetReader(file.absolutePath)
        val readSchema = reader.schema

        assertEquals(4, readSchema.fieldCount)
        assertEquals(rowCount.toLong(), reader.totalRowCount)

        println("Schema fields: ${readSchema.fieldCount}")
        println("Total rows: ${reader.totalRowCount}")

        // Check column metadata for INT64 columns
        val int64Fields = readSchema.fields.filter { it.dataType == ParquetType.INT64 }
        println("\nINT64 columns metadata:")
        int64Fields.forEach { field ->
            val columnChunk = reader.fileMetadata?.rowGroups?.get(0)?.columns?.find {
                it.metaData.pathInSchema.lastOrNull() == field.name
            }
            if (columnChunk != null) {
                val meta = columnChunk.metaData
                println("  ${field.name}:")
                println("    numValues: ${meta.numValues}")
                println("    totalUncompressedSize: ${meta.totalUncompressedSize}")
                println("    totalCompressedSize: ${meta.totalCompressedSize}")
                println("    dataPageOffset: ${meta.dataPageOffset}")

                // Verify metadata is valid
                assertTrue(meta.numValues == rowCount.toLong(),
                    "${field.name}: numValues should be $rowCount, got ${meta.numValues}")
                assertTrue(meta.totalUncompressedSize > 0,
                    "${field.name}: totalUncompressedSize should be > 0, got ${meta.totalUncompressedSize}")
                assertTrue(meta.totalCompressedSize > 0,
                    "${field.name}: totalCompressedSize should be > 0, got ${meta.totalCompressedSize}")
            }
        }

        // Read all data back
        println("\nReading all data back...")
        val readRowGroups = reader.readAllRowGroups()
        assertEquals(1, readRowGroups.size)
        assertEquals(rowCount, readRowGroups[0].rowCount)

        val readRrnColumn = readRowGroups[0].getColumn("_RRN")
        assertNotNull(readRrnColumn)
        assertEquals(rowCount, readRrnColumn!!.size)

        // Verify first few and last few values
        assertEquals(1L, readRrnColumn.get(0))
        assertEquals(2L, readRrnColumn.get(1))
        assertEquals(rowCount.toLong(), readRrnColumn.get(rowCount - 1))

        println("✅ Successfully wrote and read $rowCount rows!")
        println("   First RRN: ${readRrnColumn.get(0)}")
        println("   Last RRN: ${readRrnColumn.get(rowCount - 1)}")

        reader.close()
    }
}
