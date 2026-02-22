package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import java.nio.file.Files
import kotlin.math.max
import kotlin.system.measureTimeMillis
import kotlin.test.Test
import kotlin.test.assertTrue

class WriterResourceUsageTest {

    @Test
    fun `stream writer handles 1M long-text rows across 200 columns within memory budget`() {
        val runtime = Runtime.getRuntime()
        val baseline = currentHeapUsage(runtime)

        val columnCount = 200
        val schema = ParquetSchema.create(
            (0 until columnCount).map { idx ->
                DataField.string("text_col_$idx")
            }
        )
        val tempFile = Files.createTempFile("parquetkt-writer", ".parquet").toFile()

        val rowsPerGroup = 5_000
        val groupCount = 200 // 1,000,000 rows total
        val totalRows = rowsPerGroup * groupCount
        val longTextLength = 512
        var peakHeap = baseline

        val durationMs = measureTimeMillis {
            ParquetWriter(
                outputPath = tempFile.absolutePath,
                schema = schema,
                compressionCodec = CompressionCodec.SNAPPY,
                enableDictionary = true,
                bufferSize = 256 * 1024,
                enableParallelCompression = true,
                maxRowGroupsInMemory = 2,
                maxRowsInMemory = rowsPerGroup
            ).use { writer ->
                repeat(groupCount) { idx ->
                    val rowGroup = createLongTextRowGroup(schema, rowsPerGroup, longTextLength, idx)
                    writer.write(rowGroup)
                    peakHeap = max(peakHeap, currentHeapUsage(runtime))
                }
            }
        }

        val heapDelta = max(0L, peakHeap - baseline)
        val throughputRowsPerSecond = if (durationMs == 0L) Double.POSITIVE_INFINITY else totalRows / (durationMs / 1000.0)
        val fileSizeMb = tempFile.length() / 1024.0 / 1024.0

        println(
            "WriterResourceUsageTest -> rows=$totalRows, columns=$columnCount, duration=${durationMs}ms, " +
                "throughput=${"%.0f".format(throughputRowsPerSecond)} rows/s, " +
                "heapDelta=${formatBytes(heapDelta)}, fileSize=${"%.2f".format(fileSizeMb)} MB"
        )

        val maxAllowedHeap = 1_200L * 1024 * 1024 // ~1.2 GB safety ceiling for text-heavy payloads
        assertTrue(
            heapDelta <= maxAllowedHeap,
            "Heap increase ${formatBytes(heapDelta)} exceeds budget ${formatBytes(maxAllowedHeap)}"
        )

        assertTrue(tempFile.exists(), "Temp parquet file should exist")
        if (!tempFile.delete()) {
            tempFile.deleteOnExit()
        }
    }

    private fun currentHeapUsage(runtime: Runtime): Long {
        val used = runtime.totalMemory() - runtime.freeMemory()
        return if (used < 0) 0 else used
    }

    private fun formatBytes(bytes: Long): String {
        if (bytes <= 0) return "0 B"
        val units = arrayOf("B", "KB", "MB", "GB", "TB")
        var value = bytes.toDouble()
        var unitIndex = 0
        while (value >= 1024 && unitIndex < units.lastIndex) {
            value /= 1024
            unitIndex++
        }
        return String.format("%.2f %s", value, units[unitIndex])
    }

    private fun createLongTextRowGroup(
        schema: ParquetSchema,
        rowCount: Int,
        textLength: Int,
        batchIndex: Int
    ): RowGroup {
        val columns = mutableListOf<DataColumn<*>>()
        for (fieldIndex in 0 until schema.fieldCount) {
            val field = schema.getField(fieldIndex)
            val longText = buildString {
                append("batch", batchIndex, "_col", fieldIndex, "_")
                while (length < textLength) {
                    append('x')
                }
            }.toByteArray()
            val data = Array<ByteArray?>(rowCount) { longText }
            columns.add(DataColumn(field, data))
        }
        return RowGroup(schema, columns)
    }
}
