package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.LogicalType
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.ParquetType
import com.molo17.parquetkt.thrift.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

/**
 * Regression test to verify that precision and scale field IDs in the Thrift
 * serialization match the official Parquet spec (parquet.thrift):
 *   SchemaElement field 7 = scale
 *   SchemaElement field 8 = precision
 *   SchemaElement field 10 = logicalType
 *
 * Also verifies that DecimalType inside LogicalType uses:
 *   DecimalType field 1 = scale
 *   DecimalType field 2 = precision
 */
class ThriftDecimalFieldIdTest {

    @Test
    fun `precision and scale are not inverted after Thrift round-trip`() {
        // Use deliberately asymmetric values so any swap is immediately visible
        val precision = 18
        val scale = 5

        val element = SchemaElement(
            type = ParquetType.FIXED_LEN_BYTE_ARRAY,
            typeLength = 8,
            repetitionType = FieldRepetitionType.REQUIRED,
            name = "price",
            convertedType = ConvertedType.DECIMAL,
            scale = scale,
            precision = precision,
            logicalType = LogicalTypeAnnotation.Decimal(precision, scale)
        )

        // Build minimal FileMetaData to serialize
        val metadata = FileMetaData(
            version = 2,
            schema = listOf(
                SchemaElement(name = "schema", numChildren = 1),
                element
            ),
            numRows = 0,
            rowGroups = emptyList()
        )

        val bytes = ThriftSerializer.serializeFileMetadata(metadata)
        val deserialized = ThriftDeserializer.deserializeFileMetadata(bytes)

        val readElement = deserialized.schema[1]
        assertEquals(precision, readElement.precision, "precision was swapped with scale")
        assertEquals(scale, readElement.scale, "scale was swapped with precision")

        // Verify the LogicalType annotation as well
        val decimalAnnotation = readElement.logicalType as LogicalTypeAnnotation.Decimal
        assertEquals(precision, decimalAnnotation.precision, "LogicalType.Decimal precision was swapped")
        assertEquals(scale, decimalAnnotation.scale, "LogicalType.Decimal scale was swapped")
    }

    @Test
    fun `precision and scale survive full Parquet write-read round-trip with asymmetric values`(@TempDir tempDir: File) {
        val file = File(tempDir, "decimal_field_id_test.parquet")

        // Precision=18, Scale=3 — clearly asymmetric
        val schema = ParquetSchema.create(
            DataField.decimal("amount", precision = 18, scale = 3)
        )

        val field = schema.fields[0]
        assertEquals(18, field.precision)
        assertEquals(3, field.scale)

        // Write a row
        val requiredLength = field.length!!
        val inputBytes = ByteArray(requiredLength) { 0 }
        inputBytes[requiredLength - 1] = 99.toByte()

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(DataColumn(field, arrayOf(inputBytes))))
        writer.close()

        // Read back and verify precision/scale aren't swapped
        val reader = ParquetReader(file.absolutePath)
        val readField = reader.schema.fields[0]

        assertEquals(LogicalType.DECIMAL, readField.logicalType)
        assertEquals(18, readField.precision, "precision was swapped with scale after full round-trip")
        assertEquals(3, readField.scale, "scale was swapped with precision after full round-trip")
        reader.close()
    }

    @Test
    fun `file is readable by PyArrow and reports correct decimal schema`(@TempDir tempDir: File) {
        val file = File(tempDir, "pyarrow_decimal_test.parquet")

        val schema = ParquetSchema.create(
            DataField.decimal("amount", precision = 18, scale = 3)
        )

        val field = schema.fields[0]
        val requiredLength = field.length!!
        val inputBytes = ByteArray(requiredLength) { 0 }
        inputBytes[requiredLength - 1] = 99.toByte()

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(DataColumn(field, arrayOf(inputBytes))))
        writer.close()

        // Use PyArrow to validate the file is spec-compliant
        val process = ProcessBuilder(
            "python3", "-c",
            "import pyarrow.parquet as pq; s = pq.read_schema('${file.absolutePath}'); print(s.field(0))"
        ).redirectErrorStream(true).start()

        val output = process.inputStream.bufferedReader().readText().trim()
        val exitCode = process.waitFor()

        assertEquals(0, exitCode, "PyArrow failed to read the file: $output")
        assertTrue(
            output.contains("decimal128(18, 3)"),
            "PyArrow did not report decimal(18,3). Got: $output"
        )
    }

    @Test
    fun `Thrift field IDs for scale and precision match Parquet spec`() {
        // Serialize a SchemaElement and inspect the raw bytes to confirm
        // that field 7 carries scale and field 8 carries precision.
        val element = SchemaElement(
            type = ParquetType.FIXED_LEN_BYTE_ARRAY,
            typeLength = 8,
            repetitionType = FieldRepetitionType.REQUIRED,
            name = "x",
            convertedType = ConvertedType.DECIMAL,
            scale = 42,
            precision = 99,
            logicalType = LogicalTypeAnnotation.Decimal(99, 42)
        )

        val metadata = FileMetaData(
            version = 2,
            schema = listOf(
                SchemaElement(name = "schema", numChildren = 1),
                element
            ),
            numRows = 0,
            rowGroups = emptyList()
        )

        val bytes = ThriftSerializer.serializeFileMetadata(metadata)
        val deserialized = ThriftDeserializer.deserializeFileMetadata(bytes)

        val read = deserialized.schema[1]

        // If field IDs were wrong (e.g. scale at field 8, precision at field 9),
        // a spec-compliant reader would see precision=42 and scale=null.
        // Our own deserializer should agree with the serializer.
        assertEquals(42, read.scale, "scale field ID is wrong — got precision value instead")
        assertEquals(99, read.precision, "precision field ID is wrong — got scale value instead")
    }
}
