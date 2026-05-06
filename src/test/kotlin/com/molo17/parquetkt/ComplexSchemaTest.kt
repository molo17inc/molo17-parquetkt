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

import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.LogicalType
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.ParquetType
import com.molo17.parquetkt.schema.Repetition
import java.io.File
import java.math.BigInteger
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ComplexSchemaTest {

    private data class FieldSpec(
        val name: String,
        val required: Boolean,
        val type: String,
        val length: Int? = null,
        val precision: Int? = null,
        val scale: Int? = null,
        val logicalString: Boolean = false
    )

    @Test
    fun `roundtrip full reported schema with random nullable data`() {
        val specs = parseSchema(REPORTED_SCHEMA)
        assertTrue(specs.isNotEmpty(), "Schema parsing failed")
        assertTrue(specs.any { it.name == "_RRN" })
        assertTrue(specs.any { it.name == "APDES" })

        val schemaFields = specs.map { it.toDataField() }
        val schema = ParquetSchema.create(*schemaFields.toTypedArray())

        val rowCount = 4_245
        val seed = 20260506
        val random = Random(seed)
        val expectedByField = mutableMapOf<String, List<Any?>>()

        val columns = specs.map { spec ->
            val values = generateValues(spec, rowCount, random)
            expectedByField[spec.name] = values

            if (spec.required) {
                @Suppress("UNCHECKED_CAST")
                com.molo17.parquetkt.data.DataColumn.createRequired(
                    spec.toDataField(),
                    values as List<Any>
                )
            } else {
                com.molo17.parquetkt.data.DataColumn.create(spec.toDataField(), values)
            }
        }

        val rowGroup = com.molo17.parquetkt.data.RowGroup(schema, columns)

        val tempFile = File.createTempFile("complex_schema_full", ".parquet")
        tempFile.deleteOnExit()

        ParquetFile.write(tempFile, schema, listOf(rowGroup))

        val readRowGroups = ParquetFile.read(tempFile)
        assertEquals(1, readRowGroups.size)

        val readGroup = readRowGroups[0]
        assertEquals(rowCount, readGroup.rowCount)
        assertEquals(specs.size, readGroup.columnCount)

        for (spec in specs) {
            val actualColumn = readGroup.getColumn(spec.name)
            assertNotNull(actualColumn, "Missing column ${spec.name}")
            assertEquals(rowCount, actualColumn.size, "Unexpected size for ${spec.name}")
        }

        // strict check for the required metadata-like row id column
        verifyColumnFully(readGroup, expectedByField, "_RRN")
    }

    @Test
    fun `new files with full schema are readable by pyarrow`() {
        val specs = parseSchema(REPORTED_SCHEMA)
        val schema = ParquetSchema.create(*specs.map { it.toDataField() }.toTypedArray())

        val random = Random(20260506)
        val rowCount = 6
        val columns = specs.map { spec ->
            val values = generateValues(spec, rowCount, random)
            if (spec.required) {
                @Suppress("UNCHECKED_CAST")
                com.molo17.parquetkt.data.DataColumn.createRequired(spec.toDataField(), values as List<Any>)
            } else {
                com.molo17.parquetkt.data.DataColumn.create(spec.toDataField(), values)
            }
        }

        val file = File.createTempFile("full_schema_pyarrow", ".parquet")
        val rowGroup = com.molo17.parquetkt.data.RowGroup(schema, columns)
        ParquetFile.write(file, schema, listOf(rowGroup))

        val process = ProcessBuilder(
            "python3",
            "-c",
            "import pyarrow.parquet as pq; t=pq.read_table('${file.absolutePath}'); print(t.num_rows, t.num_columns)"
        ).redirectErrorStream(true).start()

        val output = process.inputStream.bufferedReader().readText().trim()
        val exitCode = process.waitFor()

        assertEquals(0, exitCode, "PyArrow failed to read generated file: $output")
        assertTrue(output.startsWith("6 333"), "Unexpected PyArrow output: $output")
    }

    @Test
    fun `read reported parquet file from trash for reproduction`() {
        val candidates = listOf(
            "/Users/danieleangeli/.openclaw/media/inbound/2026-05-06T08_49_59.705030925---a64562a1-94d4-41a0-ab20-79f24aa8acf9.parquet",
            "/Users/danieleangeli/.Trash/2026-05-06T08_49_59.705030925.parquet"
        )

        val externalFile = candidates.map(::File).firstOrNull { it.exists() }
        if (externalFile == null) {
            println("⚠️ External file not found in known locations")
            return
        }

        ParquetReader(externalFile.absolutePath).use { reader ->
            println("Reading: ${externalFile.name}")
            println("Rows: ${reader.totalRowCount}, RowGroups: ${reader.rowGroupCount}, Fields: ${reader.schema.fieldCount}")

            val firstColMeta = reader.fileMetadata?.rowGroups?.firstOrNull()?.columns?.firstOrNull()?.metaData
            println("_RRN metadata: numValues=${firstColMeta?.numValues}, totalUncompressedSize=${firstColMeta?.totalUncompressedSize}, totalCompressedSize=${firstColMeta?.totalCompressedSize}")

            // Reproduce the original issue: file metadata appears corrupted (uncompressed size = 0)
            assertFailsWith<IllegalStateException> {
                reader.readRowGroup(0)
            }
        }
    }

    private fun verifyColumnFully(
        readGroup: com.molo17.parquetkt.data.RowGroup,
        expectedByField: Map<String, List<Any?>>, 
        fieldName: String
    ) {
        val expected = expectedByField[fieldName] ?: error("Missing expected field: $fieldName")
        val actual = readGroup.getColumn(fieldName)
        assertNotNull(actual)
        for (i in expected.indices) {
            assertCellEquals(expected[i], actual.get(i), fieldName, i)
        }
    }

    private fun assertCellEquals(expected: Any?, actual: Any?, field: String, row: Int) {
        when {
            expected == null && actual == null -> return
            expected == null || actual == null -> error("Mismatch at $field[$row]: expected=$expected actual=$actual")
            expected is ByteArray && actual is ByteArray ->
                assertContentEquals(expected, actual, "Mismatch at $field[$row]")
            else -> assertEquals(expected, actual, "Mismatch at $field[$row]")
        }
    }

    private fun generateValues(spec: FieldSpec, rowCount: Int, random: Random): List<Any?> {
        val nullChance = 0.20
        return (1..rowCount).map { i ->
            if (!spec.required && random.nextDouble() < nullChance) {
                null
            } else {
                when (spec.type) {
                    "int64" -> i.toLong()
                    "binary" -> randomString(spec.name, i, random).toByteArray(Charsets.UTF_8)
                    "fixed" -> {
                        val precision = requireNotNull(spec.precision)
                        val length = requireNotNull(spec.length)
                        randomDecimalFixedBytes(precision, length, random)
                    }
                    else -> error("Unsupported spec type: ${spec.type}")
                }
            }
        }
    }

    private fun randomString(field: String, row: Int, random: Random): String {
        val suffix = random.nextInt(100_000, 999_999)
        return "$field-$row-$suffix"
    }

    private fun randomDecimalFixedBytes(precision: Int, length: Int, random: Random): ByteArray {
        val bound = pow10(precision) - 1L
        val unscaled = random.nextLong(-bound, bound + 1)
        val bytes = BigInteger.valueOf(unscaled).toByteArray()

        if (bytes.size == length) return bytes

        val signByte = if (unscaled < 0) 0xFF.toByte() else 0x00.toByte()
        return if (bytes.size < length) {
            val out = ByteArray(length) { signByte }
            System.arraycopy(bytes, 0, out, length - bytes.size, bytes.size)
            out
        } else {
            // trim only leading sign-extension bytes when needed
            bytes.copyOfRange(bytes.size - length, bytes.size)
        }
    }

    private fun pow10(p: Int): Long {
        var result = 1L
        repeat(p) { result *= 10L }
        return result
    }

    private fun parseSchema(schemaText: String): List<FieldSpec> {
        val statementRegex = Regex(
            """(required|optional)\s+(int64|binary|fixed_len_byte_array\((\d+)\))\s+([A-Za-z0-9_]+)(?:\s+\(DECIMAL\((\d+),(\d+)\)\)|\s+\(STRING\))?\s*$"""
        )

        val declarations = schemaText
            .replace("message schema", "")
            .replace("{", " ")
            .replace("}", " ")
            .replace("\n", " ")
            .replace("\r", " ")
            .split(';')
            .map { it.trim() }
            .filter { it.startsWith("required ") || it.startsWith("optional ") }

        return declarations.map { statement ->
            val m = statementRegex.matchEntire(statement)
                ?: error("Schema declaration not parsed exactly: '$statement'")

            val required = m.groupValues[1] == "required"
            val rawType = m.groupValues[2]
            val len = m.groupValues[3].takeIf { it.isNotEmpty() }?.toInt()
            val name = m.groupValues[4]
            val precision = m.groupValues[5].takeIf { it.isNotEmpty() }?.toInt()
            val scale = m.groupValues[6].takeIf { it.isNotEmpty() }?.toInt()
            val type = when {
                rawType == "int64" -> "int64"
                rawType == "binary" -> "binary"
                rawType.startsWith("fixed_len_byte_array") -> "fixed"
                else -> error("Unsupported type in declaration: '$statement'")
            }

            FieldSpec(
                name = name,
                required = required,
                type = type,
                length = len,
                precision = precision,
                scale = scale,
                logicalString = rawType == "binary"
            )
        }
    }

    private fun FieldSpec.toDataField(): DataField {
        val repetition = if (required) Repetition.REQUIRED else Repetition.OPTIONAL
        return when (type) {
            "int64" -> DataField(
                name = name,
                dataType = ParquetType.INT64,
                repetition = repetition,
                maxDefinitionLevel = if (required) 0 else 1
            )

            "binary" -> DataField(
                name = name,
                dataType = ParquetType.BYTE_ARRAY,
                logicalType = LogicalType.STRING,
                repetition = repetition,
                maxDefinitionLevel = if (required) 0 else 1
            )

            "fixed" -> DataField(
                name = name,
                dataType = ParquetType.FIXED_LEN_BYTE_ARRAY,
                logicalType = LogicalType.DECIMAL,
                repetition = repetition,
                maxDefinitionLevel = if (required) 0 else 1,
                length = length,
                precision = precision,
                scale = scale
            )

            else -> error("Unsupported field type: $type")
        }
    }

    private companion object {
        val REPORTED_SCHEMA = """
            message schema {
             required int64 _RRN;
             optional fixed_len_byte_array(3) APART (DECIMAL(5,0));
             optional fixed_len_byte_array(2) APCOL (DECIMAL(3,0));
             optional binary APDES (STRING);
             optional fixed_len_byte_array(3) APPRE (DECIMAL(5,2));
             optional fixed_len_byte_array(2) APINC (DECIMAL(3,2));
             optional fixed_len_byte_array(2) APKEYT (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APCOSC (DECIMAL(3,2));
             optional fixed_len_byte_array(3) APCOSF (DECIMAL(4,2));
             optional fixed_len_byte_array(2) APCOSP (DECIMAL(3,2));
             optional fixed_len_byte_array(3) APCOEF (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV1 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV2 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV3 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV4 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV5 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV6 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV7 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV8 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV9 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSV10 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF1 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF2 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF3 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF4 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF5 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF6 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF7 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF8 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF9 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSF10 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE1 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE2 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE3 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE4 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE5 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE6 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE7 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE8 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE9 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSE10 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA1 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA2 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA3 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA4 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA5 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA6 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA7 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA8 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA9 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSA10 (DECIMAL(5,0));
             optional fixed_len_byte_array(4) APSM1 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM2 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM3 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM4 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM5 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM6 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM7 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM8 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM9 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM10 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM11 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM12 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM13 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM14 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM15 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM16 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM17 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM18 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM19 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM20 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM21 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM22 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM23 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM24 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM25 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM26 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM27 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM28 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM29 (DECIMAL(7,0));
             optional fixed_len_byte_array(4) APSM30 (DECIMAL(7,0));
             optional fixed_len_byte_array(3) APSC1 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC2 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC3 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC4 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC5 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC6 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC7 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC8 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC9 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC10 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC11 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC12 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC13 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC14 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC15 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC16 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC17 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC18 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC19 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC20 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC21 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC22 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC23 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC24 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC25 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC26 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC27 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC28 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC29 (DECIMAL(5,2));
             optional fixed_len_byte_array(3) APSC30 (DECIMAL(5,2));
             optional fixed_len_byte_array(5) APSI1 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI2 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI3 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI4 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI5 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI6 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI7 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI8 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI9 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI10 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI11 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI12 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI13 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI14 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI15 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI16 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI17 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI18 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI19 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI20 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI21 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI22 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI23 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI24 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI25 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI26 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI27 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI28 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI29 (DECIMAL(9,2));
             optional fixed_len_byte_array(5) APSI30 (DECIMAL(9,2));
             optional fixed_len_byte_array(2) APCOSU (DECIMAL(3,0));
             optional binary APTEMP (STRING);
             optional fixed_len_byte_array(2) APPIC01 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APPIC02 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APPIC03 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APPIC04 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APCPZA (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APCPAR (DECIMAL(2,0));
             optional fixed_len_byte_array(1) APCOLE (DECIMAL(1,0));
             optional binary APORIG (STRING);
             optional binary APBORD (STRING);
             optional binary APSERI (STRING);
             optional binary APFLOC (STRING);
             optional fixed_len_byte_array(3) APANOT (DECIMAL(5,0));
             optional binary APTENO (STRING);
             optional binary APCJTO (STRING);
             optional fixed_len_byte_array(2) APCODC (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APCODF (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APCODP (DECIMAL(3,0));
             optional binary APETI (STRING);
             optional binary APCOMP (STRING);
             optional fixed_len_byte_array(2) APCAJA (DECIMAL(2,0));
             optional binary APNOMC (STRING);
             optional binary APFENV (STRING);
             optional binary APFINQ (STRING);
             optional binary APBASI (STRING);
             optional binary APSUP (STRING);
             optional fixed_len_byte_array(2) APCLAV (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APUDPA (DECIMAL(2,0));
             optional binary APPTO (STRING);
             optional binary APTINT (STRING);
             optional binary APPROD (STRING);
             optional fixed_len_byte_array(3) APPESM (DECIMAL(4,0));
             optional fixed_len_byte_array(2) APTORG (DECIMAL(3,0));
             optional binary APCONT (STRING);
             optional binary APUBIC (STRING);
             optional binary APEXRE (STRING);
             optional binary APCAPE (STRING);
             optional binary APTRIC (STRING);
             optional fixed_len_byte_array(2) APGALG (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APTIPO (DECIMAL(2,0));
             optional binary APESTI (STRING);
             optional binary APPSB (STRING);
             optional fixed_len_byte_array(2) APCOLCH (DECIMAL(2,0));
             optional fixed_len_byte_array(4) APCAMI (DECIMAL(7,0));
             optional fixed_len_byte_array(3) AP1XTP (DECIMAL(4,0));
             optional fixed_len_byte_array(3) AP1XTC (DECIMAL(4,0));
             optional fixed_len_byte_array(3) AP1XTF (DECIMAL(4,0));
             optional fixed_len_byte_array(3) AP1XTB (DECIMAL(4,0));
             optional fixed_len_byte_array(2) APCMUE (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APCPRE (DECIMAL(3,0));
             optional binary APFAMA (STRING);
             optional fixed_len_byte_array(2) APCCOM (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APCUID (DECIMAL(3,0));
             optional binary APPART (STRING);
             optional binary APFAMI (STRING);
             optional binary APFAMS (STRING);
             optional fixed_len_byte_array(2) APGRPR (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APGRPE (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APEGRU (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APESEC (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APECOL (DECIMAL(2,0));
             optional binary APTBOR (STRING);
             optional binary APMASM (STRING);
             optional fixed_len_byte_array(2) APQMM (DECIMAL(3,0));
             optional binary APAPRF (STRING);
             optional binary AP1XTL (STRING);
             optional binary APFASE (STRING);
             optional binary APFASS (STRING);
             optional fixed_len_byte_array(3) APSD1 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD2 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD3 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD4 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD5 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD6 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD7 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD8 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD9 (DECIMAL(5,0));
             optional fixed_len_byte_array(3) APSD10 (DECIMAL(5,0));
             optional binary APST1 (STRING);
             optional binary APST2 (STRING);
             optional binary APST3 (STRING);
             optional binary APST4 (STRING);
             optional binary APST5 (STRING);
             optional binary APST6 (STRING);
             optional binary APST7 (STRING);
             optional binary APST8 (STRING);
             optional binary APST9 (STRING);
             optional binary APST10 (STRING);
             optional binary APTIPA (STRING);
             optional binary APSERV (STRING);
             optional binary APLAVD (STRING);
             optional binary APSINP (STRING);
             optional binary APREPR (STRING);
             optional fixed_len_byte_array(2) APCOL1 (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APCOL2 (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APCOL3 (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APCOL4 (DECIMAL(3,0));
             optional binary APCATA (STRING);
             optional binary APFINC (STRING);
             optional binary APFINL (STRING);
             optional fixed_len_byte_array(2) APRESP (DECIMAL(2,0));
             optional binary APCOLM (STRING);
             optional fixed_len_byte_array(2) APETEC (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APPIC05 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APPIC06 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APPIC07 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APPIC08 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APPIC09 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APPIC10 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) ECATEGO (DECIMAL(2,0));
             optional fixed_len_byte_array(2) ESUBCT1 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) ESUBCT2 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) SUBCAD01 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) SUBCAD02 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) SUBCAD03 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) SUBCAD04 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) SUBCAD05 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APESPEC (DECIMAL(2,0));
             optional binary USERFIQN (STRING);
             optional fixed_len_byte_array(4) SGCNFF (DECIMAL(8,0));
             optional binary SGCNFU (STRING);
             optional binary ARTECM (STRING);
             optional binary EEXCMUE (STRING);
             optional fixed_len_byte_array(2) EINCMUE (DECIMAL(2,0));
             optional binary APSPEEDP (STRING);
             optional binary EEXCSCT (STRING);
             optional fixed_len_byte_array(2) EINCSCT1 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) EINCSCT2 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APUDEMP (DECIMAL(2,0));
             optional fixed_len_byte_array(2) EINCMUE2 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) EINCMUE3 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) EINCMUE4 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) EINCMUE5 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) EINCSCT12 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) EINCSCT22 (DECIMAL(2,0));
             optional fixed_len_byte_array(2) UNSHOES (DECIMAL(2,0));
             optional binary EXCLSTOA (STRING);
             optional binary EXCLSTOUA (STRING);
             optional fixed_len_byte_array(4) EXCLSTOFA (DECIMAL(8,0));
             optional fixed_len_byte_array(4) EXCLSTOHA (DECIMAL(6,0));
             optional binary EXCLOBSEA (STRING);
             optional binary EXCLSTOP (STRING);
             optional binary EXCLSTOUP (STRING);
             optional fixed_len_byte_array(4) EXCLSTOFP (DECIMAL(8,0));
             optional fixed_len_byte_array(4) EXCLSTOHP (DECIMAL(6,0));
             optional binary EXCLOBSEP (STRING);
             optional binary AP2UBEC (STRING);
             optional binary CDMISSA (STRING);
             optional binary PAISDI (STRING);
             optional binary IGNOPSER (STRING);
             optional fixed_len_byte_array(2) APSILUET (DECIMAL(3,0));
             optional fixed_len_byte_array(2) APPOSSIL (DECIMAL(2,0));
             optional binary APBACKSH (STRING);
             optional binary APRAMADA (STRING);
             optional binary APNAVIDA (STRING);
             optional binary AP2DSPPK (STRING);
             optional binary APOTRATMP (STRING);
             optional binary APTMPC3 (STRING);
             optional binary APFAMADE (STRING);
             optional fixed_len_byte_array(2) APCODSOS (DECIMAL(2,0));
             optional binary QRRU (STRING);
             optional fixed_len_byte_array(2) APCODSO2 (DECIMAL(2,0));
             optional binary APSESFOT (STRING);
             optional binary APDENIM (STRING);
             optional binary APCOLPEQ (STRING);
             optional fixed_len_byte_array(3) APUNIPRE (DECIMAL(5,0));
             optional fixed_len_byte_array(2) APGAMA (DECIMAL(3,0));
             optional binary APPREESP (STRING);
             optional binary APTIPCAL (STRING);
             optional binary APTEJIDO (STRING);
             optional binary APUNISEX (STRING);
             optional binary APLETCO4 (STRING);
             optional fixed_len_byte_array(3) APCODC4 (DECIMAL(4,0));
             optional fixed_len_byte_array(3) APCODF4 (DECIMAL(4,0));
             optional fixed_len_byte_array(3) APCODP4 (DECIMAL(4,0));
             optional binary APCAPSU (STRING);
             optional fixed_len_byte_array(3) APTORG4 (DECIMAL(4,0));
             optional binary APCNEW (STRING);
             optional binary APTVTA (STRING);
             optional binary APWOW (STRING);
             optional binary APEST (STRING);
             optional binary APESCORP (STRING);
             optional binary APBAREF (STRING);
             optional fixed_len_byte_array(2) APFORMA (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APLARGO (DECIMAL(2,0));
             optional fixed_len_byte_array(2) APTITO (DECIMAL(2,0));
             optional binary _operation (STRING);
             optional binary _receiverLibrary (STRING);
             optional binary _receiverName (STRING);
             optional binary _sequenceNumber (STRING);
             optional binary _timestamp (STRING);
            }
        """.trimIndent()
    }
}
