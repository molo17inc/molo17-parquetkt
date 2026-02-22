package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import kotlin.random.Random

/**
 * Shared helpers for generating repeatable schemas and datasets used by performance tests.
 */
object BenchmarkFixtures {
    private const val DEFAULT_RANDOM_SEED = 42

    fun generateSchema(columnCount: Int): ParquetSchema {
        require(columnCount % 5 == 0) {
            "Column count must be divisible by 5 to keep data type distribution even."
        }

        val fields = mutableListOf<DataField>()
        val typesPerCategory = columnCount / 5

        repeat(typesPerCategory) { i ->
            fields.add(DataField.int32("int32_col_$i"))
        }
        repeat(typesPerCategory) { i ->
            fields.add(DataField.int64("int64_col_$i"))
        }
        repeat(typesPerCategory) { i ->
            fields.add(DataField.double("double_col_$i"))
        }
        repeat(typesPerCategory) { i ->
            fields.add(DataField.string("string_col_$i"))
        }
        repeat(typesPerCategory) { i ->
            fields.add(DataField.boolean("boolean_col_$i"))
        }

        return ParquetSchema.create(fields)
    }

    fun generateDataColumns(
        schema: ParquetSchema,
        rowCount: Int,
        seed: Int = DEFAULT_RANDOM_SEED
    ): List<DataColumn<*>> {
        val random = Random(seed)
        val columns = mutableListOf<DataColumn<*>>()

        for (i in 0 until schema.fieldCount) {
            val field = schema.getField(i)
            val column = when {
                field.name.startsWith("int32_") -> {
                    val data = (1..rowCount).map { random.nextInt(0, 1_000_000) }
                    DataColumn.createRequired(field, data)
                }
                field.name.startsWith("int64_") -> {
                    val data = (1..rowCount).map { random.nextLong(0, 1_000_000_000L) }
                    DataColumn.createRequired(field, data)
                }
                field.name.startsWith("double_") -> {
                    val data = (1..rowCount).map { random.nextDouble(0.0, 1_000_000.0) }
                    DataColumn.createRequired(field, data)
                }
                field.name.startsWith("string_") -> {
                    val data = (1..rowCount).map {
                        "string_value_${random.nextInt(0, 10_000)}".toByteArray()
                    }
                    DataColumn.createRequired(field, data)
                }
                field.name.startsWith("boolean_") -> {
                    val data = (1..rowCount).map { random.nextBoolean() }
                    DataColumn.createRequired(field, data)
                }
                else -> throw IllegalArgumentException("Unknown column type: ${field.name}")
            }
            columns.add(column)
        }
        return columns
    }

    fun createRowGroup(
        schema: ParquetSchema,
        rowCount: Int,
        seed: Int = DEFAULT_RANDOM_SEED
    ): RowGroup {
        val columns = generateDataColumns(schema, rowCount, seed)
        return RowGroup(schema, columns)
    }

    fun createRowGroup(
        columnCount: Int,
        rowCount: Int,
        seed: Int = DEFAULT_RANDOM_SEED
    ): RowGroup {
        val schema = generateSchema(columnCount)
        return createRowGroup(schema, rowCount, seed)
    }

    fun estimateDataSize(rowCount: Int, columnCount: Int): Int {
        val bytesPerCell = 8 // Rough average across the generated data
        val totalBytes = rowCount * columnCount * bytesPerCell
        return totalBytes / 1024 / 1024
    }
}
