package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import org.junit.jupiter.api.Test
import java.io.File

class GenerateDecimalFieldIdTestFile {

    @Test
    fun `generate decimal field id test file`() {
        val dir = File("test-output")
        dir.mkdirs()
        val file = File(dir, "decimal_field_id_test.parquet")

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

        println("Generated: ${file.absolutePath}")
    }
}
