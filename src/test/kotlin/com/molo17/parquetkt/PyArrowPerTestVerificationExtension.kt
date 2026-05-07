package com.molo17.parquetkt

import com.molo17.parquetkt.core.ParquetFile
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

/**
 * Global per-test PyArrow verification phase.
 *
 * For each test, discover parquet files touched during that test and verify
 * that files readable by this library are also readable by PyArrow.
 */
class PyArrowPerTestVerificationExtension : BeforeEachCallback, AfterEachCallback {

    override fun beforeEach(context: ExtensionContext) {
        context.getStore(NAMESPACE).put(startKey(context), System.currentTimeMillis())
    }

    override fun afterEach(context: ExtensionContext) {
        val startTime = context.getStore(NAMESPACE).get(startKey(context), Long::class.java) ?: return
        val candidates = collectCandidateParquetFiles(context, startTime)

        candidates.forEach { file ->
            if (isReadableByParquetKt(file)) {
                verifyWithPyArrow(file)
            }
        }
    }

    private fun collectCandidateParquetFiles(context: ExtensionContext, startTimeMillis: Long): List<File> {
        val files = linkedSetOf<File>()

        // Prefer test-local @TempDir fields.
        for (instance in context.requiredTestInstances.allInstances) {
            for (field in instance::class.java.declaredFields) {
                if (!field.isAnnotationPresent(TempDir::class.java)) continue
                field.isAccessible = true
                when (val value = field.get(instance)) {
                    is File -> files.addAll(findParquetFilesUnder(value.toPath(), startTimeMillis, maxDepth = 8))
                    is Path -> files.addAll(findParquetFilesUnder(value, startTimeMillis, maxDepth = 8))
                }
            }
        }

        // Also cover @TempDir method parameters and createTempFile() usage under system tmp.
        val tmpRoot = File(System.getProperty("java.io.tmpdir")).toPath()
        files.addAll(findParquetFilesUnder(tmpRoot, startTimeMillis, maxDepth = 4))

        return files
            .filter { it.exists() && it.length() > 0 }
            .filter { VERIFIED.add("${it.absolutePath}:${it.lastModified()}") }
            .sortedBy { it.absolutePath }
    }

    private fun findParquetFilesUnder(root: Path, startTimeMillis: Long, maxDepth: Int): List<File> {
        if (!Files.exists(root)) return emptyList()

        val found = mutableListOf<File>()
        try {
            Files.walkFileTree(root, java.util.EnumSet.noneOf(java.nio.file.FileVisitOption::class.java), maxDepth,
                object : SimpleFileVisitor<Path>() {
                    override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                        if (attrs.isRegularFile && file.fileName.toString().endsWith(".parquet")) {
                            val f = file.toFile()
                            if (f.lastModified() >= (startTimeMillis - 1500)) {
                                found.add(f)
                            }
                        }
                        return FileVisitResult.CONTINUE
                    }

                    override fun visitFileFailed(file: Path, exc: java.io.IOException?): FileVisitResult {
                        // Ignore inaccessible paths under /tmp and continue.
                        return FileVisitResult.CONTINUE
                    }
                }
            )
        } catch (_: Exception) {
            // Best effort discovery only.
        }
        return found
    }

    private fun isReadableByParquetKt(file: File): Boolean {
        return try {
            ParquetFile.read(file)
            true
        } catch (_: Exception) {
            false
        }
    }

    private fun verifyWithPyArrow(file: File) {
        val script = """
import pyarrow.parquet as pq
pq.read_table(r'''${file.absolutePath}''')
print("OK")
        """.trimIndent()

        val process = ProcessBuilder("python3", "-c", script)
            .redirectErrorStream(true)
            .start()

        if (!process.waitFor(Duration.ofSeconds(20).toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly()
            throw IllegalStateException("PyArrow verification timed out for ${file.absolutePath}")
        }

        val output = process.inputStream.bufferedReader().readText().trim()
        val exitCode = process.exitValue()

        check(exitCode == 0) {
            "PyArrow verification failed for ${file.absolutePath}: $output"
        }
    }

    private fun startKey(context: ExtensionContext): String = "start:${context.uniqueId}"

    companion object {
        private val NAMESPACE = ExtensionContext.Namespace.create(PyArrowPerTestVerificationExtension::class.java)
        private val VERIFIED = ConcurrentHashMap.newKeySet<String>()
    }
}
