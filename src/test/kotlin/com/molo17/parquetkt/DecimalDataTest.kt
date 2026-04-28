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
