package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.LogicalType
import com.molo17.parquetkt.schema.ParquetSchema
import com.molo17.parquetkt.schema.ParquetType
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class DecimalDataTest {

    @Test
    fun `test decimal with scale 0`(@TempDir tempDir: File) {
        val file = File(tempDir, "decimal_scale_zero.parquet")

        // scale = 0 means the decimal represents a whole number
        val schema = ParquetSchema.create(
            DataField.decimal("amount", precision = 10, scale = 0)
        )

        val amountField = schema.fields[0]
        assertEquals(ParquetType.FIXED_LEN_BYTE_ARRAY, amountField.dataType)
        assertEquals(LogicalType.DECIMAL, amountField.logicalType)
        assertEquals(10, amountField.precision)
        assertEquals(0, amountField.scale)

        val requiredLength = amountField.length!!
        // Encode the integer value 42 as big-endian bytes
        val inputBytes = ByteArray(requiredLength) { 0 }
        inputBytes[requiredLength - 1] = 42

        val writer = ParquetWriter(file.absolutePath, schema)
        writer.writeRowGroup(listOf(DataColumn(amountField, arrayOf(inputBytes))))
        writer.close()

        assertTrue(file.exists())

        val reader = ParquetReader(file.absolutePath)
        assertEquals(1, reader.rowGroupCount)

        val readSchema = reader.schema
        val readAmountField = readSchema.fields[0]
        assertEquals(10, readAmountField.precision)
        assertEquals(0, readAmountField.scale)

        val readRowGroup = reader.readRowGroup(0)
        val readBytes = readRowGroup.columns[0].rawData[0] as ByteArray
        assertEquals(requiredLength, readBytes.size)
        assertEquals(42.toByte(), readBytes[requiredLength - 1])
        reader.close()
    }

    @Test
    fun `test decimal with 8-byte input array on 9-byte required precision`(@TempDir tempDir: File) {
        val file = File(tempDir, "decimal_fixed_len.parquet")
        
        // Precision 20 requires 9 bytes
        val schema = ParquetSchema.create(
            DataField.decimal("amount", precision = 20, scale = 10, nullable = true)
        )
        
        val priceField = schema.fields[0]
        assertEquals(ParquetType.FIXED_LEN_BYTE_ARRAY, priceField.dataType)
        assertEquals(LogicalType.DECIMAL, priceField.logicalType)
        assertEquals(9, priceField.length)
        
        val writer = ParquetWriter(file.absolutePath, schema)
        
        // 394872724123104000 represented in 8 bytes
        val inputBytes = byteArrayOf(5, 122, -34, -90, -124, -39, 51, 0)
        assertEquals(8, inputBytes.size)
        
        val column = DataColumn(schema.fields[0], arrayOf(inputBytes))
        writer.writeRowGroup(listOf(column))
        writer.close()
        
        // Ensure file exists and can be read back
        assertTrue(file.exists())
        
        val reader = ParquetReader(file.absolutePath)
        assertEquals(1, reader.rowGroupCount)
        
        val readRowGroup = reader.readRowGroup(0)
        val columnData = readRowGroup.columns[0]
        val readBytes = columnData.rawData[0] as ByteArray
        
        // Should be padded to 9 bytes
        assertEquals(9, readBytes.size)
        // Since original byte array was positive (5), padding byte should be 0
        assertEquals(0.toByte(), readBytes[0])
        assertEquals(5.toByte(), readBytes[1])
        assertEquals(122.toByte(), readBytes[2])
        reader.close()
    }
}
