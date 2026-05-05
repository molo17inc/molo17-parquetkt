package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.time.LocalDate

/**
 * Validates that files produced by this library are fully readable by PyArrow,
 * covering all supported types: primitives, strings, nullables, temporal types,
 * decimal, multiple row groups, and compression codecs.
 */
class PyArrowCompatibilityTest {

    private fun runPyArrow(script: String): Pair<Int, String> {
        val process = ProcessBuilder("python3", "-c", script)
            .redirectErrorStream(true)
            .start()
        val output = process.inputStream.bufferedReader().readText().trim()
        val exitCode = process.waitFor()
        return exitCode to output
    }

    private fun assertPyArrowReadable(file: File, message: String = "") {
        val (exitCode, output) = runPyArrow(
            "import pyarrow.parquet as pq; t = pq.read_table('${file.absolutePath}'); print(t.schema); print('ROWS:', t.num_rows)"
        )
        assertEquals(0, exitCode, "PyArrow failed to read file${if (message.isNotEmpty()) " ($message)" else ""}: $output")
    }

    @Test
    fun `all primitive types are readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "primitives.parquet")

        val schema = ParquetSchema.create(
            DataField.boolean("bool_col"),
            DataField.int32("int32_col"),
            DataField.int64("int64_col"),
            DataField.float("float_col"),
            DataField.double("double_col"),
            DataField.byteArray("bytes_col")
        )

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(true, false, true)),
            DataColumn(schema.fields[1], arrayOf(1, 2, 3)),
            DataColumn(schema.fields[2], arrayOf(100L, 200L, 300L)),
            DataColumn(schema.fields[3], arrayOf(1.5f, 2.5f, 3.5f)),
            DataColumn(schema.fields[4], arrayOf(10.5, 20.5, 30.5)),
            DataColumn(schema.fields[5], arrayOf("hello".toByteArray(), "world".toByteArray(), "test".toByteArray()))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 3, f"Expected 3 rows, got {t.num_rows}"
s = t.schema
assert s.field('bool_col').type == 'bool', f"Got {s.field('bool_col').type}"
assert s.field('int32_col').type == 'int32', f"Got {s.field('int32_col').type}"
assert s.field('int64_col').type == 'int64', f"Got {s.field('int64_col').type}"
assert s.field('float_col').type == 'float', f"Got {s.field('float_col').type}"
assert s.field('double_col').type == 'double', f"Got {s.field('double_col').type}"
print('OK: all primitive types verified')
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow primitive types test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `string logical type is readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "strings.parquet")

        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.string("name"),
            DataField.string("email")
        )

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(1, 2)),
            DataColumn(schema.fields[1], arrayOf("Alice", "Bob")),
            DataColumn(schema.fields[2], arrayOf("alice@test.com", "bob@test.com"))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 2
s = t.schema
assert str(s.field('name').type) == 'string' or str(s.field('name').type) == 'large_string', f"Got {s.field('name').type}"
print('OK: string type verified')
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow string test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `DATE logical type is readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "dates.parquet")

        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.date("birth_date")
        )

        val dateValue = LocalDate.of(2024, 1, 15).toEpochDay().toInt()

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(1)),
            DataColumn(schema.fields[1], arrayOf(dateValue))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 1
s = t.schema
assert 'date' in str(s.field('birth_date').type), f"Got {s.field('birth_date').type}"
print('OK: DATE type verified as ' + str(s.field('birth_date').type))
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow DATE test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `TIME logical type is readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "times.parquet")

        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.time("meeting_time")
        )

        val timeValue = 43200000000L // 12:00:00 in microseconds

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(1)),
            DataColumn(schema.fields[1], arrayOf(timeValue))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 1
s = t.schema
assert 'time' in str(s.field('meeting_time').type), f"Got {s.field('meeting_time').type}"
print('OK: TIME type verified as ' + str(s.field('meeting_time').type))
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow TIME test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `TIMESTAMP logical types are readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "timestamps.parquet")

        val schema = ParquetSchema.create(
            DataField.timestamp("created_at"),
            DataField.timestampMicros("updated_at")
        )

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(1705334400000L)),
            DataColumn(schema.fields[1], arrayOf(1705334400000000L))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 1
s = t.schema
assert 'timestamp' in str(s.field('created_at').type), f"Got {s.field('created_at').type}"
assert 'timestamp' in str(s.field('updated_at').type), f"Got {s.field('updated_at').type}"
print('OK: TIMESTAMP types verified: ' + str(s.field('created_at').type) + ', ' + str(s.field('updated_at').type))
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow TIMESTAMP test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `DECIMAL logical type is readable by PyArrow with correct precision and scale`(@TempDir tempDir: File) {
        val file = File(tempDir, "decimals.parquet")

        val schema = ParquetSchema.create(
            DataField.decimal("price", precision = 10, scale = 2)
        )

        val field = schema.fields[0]
        val requiredLength = field.length!!
        val decimalBytes = ByteArray(requiredLength) { 0 }
        decimalBytes[requiredLength - 1] = 0x39 // 12345 & 0xFF
        decimalBytes[requiredLength - 2] = 0x30 // (12345 >> 8) & 0xFF

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(DataColumn(field, arrayOf(decimalBytes))))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 1
s = t.schema
assert 'decimal128(10, 2)' in str(s.field('price').type), f"Got {s.field('price').type}"
print('OK: DECIMAL verified as ' + str(s.field('price').type))
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow DECIMAL test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `nullable columns are readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "nullables.parquet")

        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.string("name", nullable = true),
            DataField.int64("value", nullable = true)
        )

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(1, 2, 3)),
            DataColumn(schema.fields[1], arrayOf("Alice", null, "Charlie")),
            DataColumn(schema.fields[2], arrayOf(100L, null, 300L))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 3, f"Expected 3 rows, got {t.num_rows}"
name_col = t.column('name')
assert name_col[1].as_py() is None, f"Expected null at index 1, got {name_col[1]}"
assert name_col[0].as_py() == 'Alice', f"Got {name_col[0]}"
value_col = t.column('value')
assert value_col[1].as_py() is None, f"Expected null at index 1, got {value_col[1]}"
print('OK: nullable columns verified with correct null values')
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow nullable test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `multiple row groups are readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "multi_rg.parquet")

        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.string("data")
        )

        val writer = ParquetWriter(file.absolutePath, schema)
        for (g in 0 until 3) {
            val ids = (g * 10 + 1..(g + 1) * 10).map { it as Any? }.toTypedArray()
            val data = (g * 10 + 1..(g + 1) * 10).map { "row_$it" as Any? }.toTypedArray()
            writer.writeRowGroup(listOf(
                DataColumn(schema.fields[0], ids),
                DataColumn(schema.fields[1], data)
            ))
        }
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
f = pq.ParquetFile('${file.absolutePath}')
assert f.metadata.num_row_groups == 3, f"Expected 3 row groups, got {f.metadata.num_row_groups}"
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 30, f"Expected 30 rows, got {t.num_rows}"
print('OK: 3 row groups, 30 rows verified')
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow multi row group test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `SNAPPY compressed file is readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "snappy.parquet")

        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.string("text")
        )

        val writer = ParquetWriter(file.absolutePath, schema, CompressionCodec.SNAPPY)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(1, 2, 3, 4, 5)),
            DataColumn(schema.fields[1], arrayOf("alpha", "beta", "gamma", "delta", "epsilon"))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 5
print('OK: SNAPPY compressed file readable, 5 rows')
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow SNAPPY test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `GZIP compressed file is readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "gzip.parquet")

        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.double("value")
        )

        val writer = ParquetWriter(file.absolutePath, schema, CompressionCodec.GZIP)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(1, 2, 3)),
            DataColumn(schema.fields[1], arrayOf(1.1, 2.2, 3.3))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 3
print('OK: GZIP compressed file readable, 3 rows')
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow GZIP test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `ZSTD compressed file is readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "zstd.parquet")

        val schema = ParquetSchema.create(
            DataField.int32("id"),
            DataField.int64("big_value")
        )

        val writer = ParquetWriter(file.absolutePath, schema, CompressionCodec.ZSTD)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(1, 2, 3)),
            DataColumn(schema.fields[1], arrayOf(Long.MAX_VALUE, 0L, Long.MIN_VALUE))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 3
print('OK: ZSTD compressed file readable, 3 rows')
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow ZSTD test failed: $output")
        assertTrue(output.contains("OK"), output)
    }

    @Test
    fun `all temporal types combined are readable by PyArrow`(@TempDir tempDir: File) {
        val file = File(tempDir, "all_temporal.parquet")

        val schema = ParquetSchema.create(
            DataField.date("birth_date"),
            DataField.time("meeting_time"),
            DataField.timestamp("created_at"),
            DataField.timestampMicros("updated_at")
        )

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(
            DataColumn(schema.fields[0], arrayOf(LocalDate.of(1990, 5, 15).toEpochDay().toInt())),
            DataColumn(schema.fields[1], arrayOf(36000000000L)),
            DataColumn(schema.fields[2], arrayOf(1705334400000L)),
            DataColumn(schema.fields[3], arrayOf(1705334400000000L))
        ))
        writer.close()

        val (exitCode, output) = runPyArrow("""
import pyarrow.parquet as pq
t = pq.read_table('${file.absolutePath}')
assert t.num_rows == 1
s = t.schema
assert 'date' in str(s.field('birth_date').type)
assert 'time' in str(s.field('meeting_time').type)
assert 'timestamp' in str(s.field('created_at').type)
assert 'timestamp' in str(s.field('updated_at').type)
print('OK: all temporal types verified: ' + ', '.join(str(s.field(i).type) for i in range(4)))
""".trimIndent())
        assertEquals(0, exitCode, "PyArrow temporal types test failed: $output")
        assertTrue(output.contains("OK"), output)
    }
}
