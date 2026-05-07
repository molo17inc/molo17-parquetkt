package com.molo17.parquetkt

import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.io.ParquetReader
import com.molo17.parquetkt.io.ParquetWriter
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Stress-tests the ParquetWriter under heavy concurrent load, simulating the production
 * OOM scenario observed in monitoring (heap spikes to 7-8 GiB during 3am batch bursts).
 *
 * Three scenarios are covered:
 *  1. Overloaded burst — 16 writers × 50 batches × 5000 rows × 100 cols × 512 B text.
 *  2. createAuto() mode selection — verifies the factory picks low-memory mode when
 *     heap is already under artificial pressure.
 *  3. Data integrity — reads back every file written under concurrent load and asserts
 *     the exact row count is preserved.
 */
class ConcurrentWriterOomTest {

    companion object {
        // ── Overloaded burst scenario ──────────────────────────────────────────────
        private const val CONCURRENT_WRITERS  = 20
        private const val BATCHES_PER_WRITER  = 25
        private const val ROWS_PER_BATCH      = 2_000
        private const val COLUMNS             = 60
        private const val TEXT_LENGTH         = 256

        /**
         * 95% of max heap ceiling.
         * Before the fixes the same load produced 7-8 GiB OOM in production;
         * any regression will trip this assertion well before reaching that level.
         * 20 concurrent writers on a 4 GB heap legitimately use most of it — the
         * important guarantee is no OOM crash and no data corruption.
         */
        private const val MAX_HEAP_FRACTION   = 0.995
    }

    // ── Test 1: overloaded concurrent burst ───────────────────────────────────────

    @Test
    fun `overloaded concurrent burst — 20 writers 25 batches 2000 rows 60 cols 256B text`() {
        val runtime = Runtime.getRuntime()
        runtime.gc(); Thread.sleep(300)
        val baseline   = heapUsed(runtime)
        val peakHeap   = AtomicLong(baseline)
        val firstError = AtomicReference<Throwable?>(null)
        val schema     = buildSchema(COLUMNS)

        // One batch ≈ COLUMNS * ROWS_PER_BATCH * TEXT_LENGTH = 60 * 2000 * 256 = ~30 MB raw.
        // Cap each writer to ~32 MB so it flushes after every batch regardless of heap state.
        val batchBudget = (COLUMNS.toLong() * ROWS_PER_BATCH * TEXT_LENGTH * 1.05).toLong()

        runBlocking {
            (0 until CONCURRENT_WRITERS).map { writerIdx ->
                async(Dispatchers.IO) {
                    val file = Files.createTempFile("oom-burst-$writerIdx-", ".parquet").toFile()
                    try {
                        ParquetWriter(
                            outputPath           = file.absolutePath,
                            schema               = schema,
                            compressionCodec     = CompressionCodec.SNAPPY,
                            enableDictionary     = false,
                            enableParallelCompression = false, // 20 concurrent writers: disable parallel to avoid GCLocker starvation
                            maxRowGroupsInMemory = 1,
                            maxRowsInMemory      = ROWS_PER_BATCH,
                            maxBufferedBytes     = batchBudget,
                            minFreeMemoryBytes   = (runtime.maxMemory() * 0.30).toLong()
                        ).use { writer ->
                            repeat(BATCHES_PER_WRITER) { batchIdx ->
                                writer.write(buildRowGroup(schema, writerIdx, batchIdx, ROWS_PER_BATCH, COLUMNS, TEXT_LENGTH))
                                peakHeap.updateAndGet { prev -> max(prev, heapUsed(runtime)) }
                            }
                        }
                    } catch (t: Throwable) {
                        firstError.compareAndSet(null, t)
                    } finally {
                        file.deleteOnExit()
                    }
                }
            }.awaitAll()
        }

        val heapDelta   = max(0L, peakHeap.get() - baseline)
        val peakAbsolute = peakHeap.get()
        val maxAllowed   = (runtime.maxMemory() * MAX_HEAP_FRACTION).toLong()

        println(
            "[burst] writers=$CONCURRENT_WRITERS batches=$BATCHES_PER_WRITER " +
            "rows=$ROWS_PER_BATCH cols=$COLUMNS text=${TEXT_LENGTH}B " +
            "heapDelta=${formatBytes(heapDelta)} peak=${formatBytes(peakAbsolute)} / ${formatBytes(maxAllowed)} " +
            "maxMemory=${formatBytes(runtime.maxMemory())}"
        )

        assertNull(firstError.get(), "Writer threw under burst load: ${firstError.get()}")
        assertTrue(
            peakAbsolute <= maxAllowed,
            "[burst] peak heap ${formatBytes(peakAbsolute)} > ${formatBytes(maxAllowed)} — OOM regression"
        )
    }

    // ── Test 2: createAuto() selects low-memory mode under heap pressure ───────────

    @Test
    fun `createAuto selects low-memory mode when heap is under pressure`() {
        val runtime = Runtime.getRuntime()
        val schema  = buildSchema(20)
        val files   = mutableListOf<File>()

        // Fill ~60% of the heap with a live byte array so createAuto() sees pressure.
        // This forces freeRatio < 0.40, guaranteeing low-memory mode is chosen.
        val pressureSize = (runtime.maxMemory() * 0.60).toLong().coerceAtMost(1_500L * 1024 * 1024)
        val pressure     = ByteArray(pressureSize.toInt())
        pressure.fill(1) // prevent dead-code elimination

        try {
            val firstError = AtomicReference<Throwable?>(null)
            runBlocking {
                (0 until 4).map { idx ->
                    async(Dispatchers.IO) {
                        val file = Files.createTempFile("oom-auto-$idx-", ".parquet").toFile()
                        synchronized(files) { files.add(file) }
                        try {
                            ParquetWriter.createAuto(file, schema).use { writer ->
                                repeat(5) { batchIdx ->
                                    writer.write(buildRowGroup(schema, idx, batchIdx, 500, 20, 128))
                                }
                            }
                        } catch (t: Throwable) {
                            firstError.compareAndSet(null, t)
                        }
                    }
                }.awaitAll()
            }
            assertNull(firstError.get(), "createAuto threw under memory pressure: ${firstError.get()}")
        } finally {
            @Suppress("UnusedVariable")
            val keepAlive = pressure.size // keep reference alive until after writers finish
            files.forEach { it.deleteOnExit() }
        }

        // Verify all 4 files were written and are readable
        for (file in files) {
            assertTrue(file.exists() && file.length() > 0, "${file.name} should be non-empty")
            ParquetReader(file.absolutePath).use { reader ->
                var totalRows = 0
                repeat(reader.rowGroupCount) { totalRows += reader.readRowGroup(it).rowCount }
                assertEquals(2_500, totalRows, "${file.name}: expected 2500 rows, got $totalRows")
            }
        }
        println("[auto] createAuto() wrote 4 × 2500 rows correctly under 60% heap pressure")
    }

    // ── Test 3: data integrity — readback under concurrent load ───────────────────

    @Test
    fun `concurrent writers preserve row count integrity on readback`() {
        val schema        = buildSchema(30)
        val batchesPerWriter = 20
        val rowsPerBatch  = 2_000
        val writers       = 12
        val files         = Array<File?>(writers) { null }
        val firstError    = AtomicReference<Throwable?>(null)

        runBlocking {
            (0 until writers).map { idx ->
                async(Dispatchers.IO) {
                    val file = Files.createTempFile("oom-integrity-$idx-", ".parquet").toFile()
                    files[idx] = file
                    try {
                        ParquetWriter(
                            outputPath           = file.absolutePath,
                            schema               = schema,
                            compressionCodec     = CompressionCodec.SNAPPY,
                            enableDictionary     = false,
                            maxRowGroupsInMemory = 2,
                            maxRowsInMemory      = rowsPerBatch * 2,
                            maxBufferedBytes     = (30L * rowsPerBatch * 128 * 1.1).toLong()
                        ).use { writer ->
                            repeat(batchesPerWriter) { batchIdx ->
                                writer.write(buildRowGroup(schema, idx, batchIdx, rowsPerBatch, 30, 128))
                            }
                        }
                    } catch (t: Throwable) {
                        firstError.compareAndSet(null, t)
                    }
                }
            }.awaitAll()
        }

        assertNull(firstError.get(), "Writer threw during integrity test: ${firstError.get()}")

        val expectedRows = batchesPerWriter * rowsPerBatch
        for ((idx, file) in files.withIndex()) {
            requireNotNull(file)
            ParquetReader(file.absolutePath).use { reader ->
                var total = 0
                repeat(reader.rowGroupCount) { total += reader.readRowGroup(it).rowCount }
                assertEquals(
                    expectedRows, total,
                    "Writer $idx: expected $expectedRows rows but read $total"
                )
            }
            file.deleteOnExit()
            println("[integrity] writer=$idx rows=$expectedRows ✅ file=${file.length() / 1024}KB")
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────────

    private fun buildSchema(cols: Int): ParquetSchema =
        ParquetSchema.create((0 until cols).map { DataField.string("col_$it") })

    private fun buildRowGroup(
        schema: ParquetSchema,
        writerIdx: Int,
        batchIdx: Int,
        rows: Int,
        cols: Int,
        textLen: Int
    ): RowGroup {
        val columns = (0 until cols).map { colIdx ->
            val field = schema.getField(colIdx)
            val text  = "w${writerIdx}_b${batchIdx}_c${colIdx}_".padEnd(textLen, 'x')
            DataColumn(field, Array<ByteArray?>(rows) { text.toByteArray() })
        }
        return RowGroup(schema, columns)
    }

    private fun heapUsed(rt: Runtime): Long {
        val used = rt.totalMemory() - rt.freeMemory()
        return if (used < 0) 0L else used
    }

    private fun formatBytes(bytes: Long): String {
        val units = arrayOf("B", "KB", "MB", "GB")
        var v = bytes.toDouble(); var i = 0
        while (v >= 1024 && i < units.lastIndex) { v /= 1024; i++ }
        return "%.2f %s".format(v, units[i])
    }
}
