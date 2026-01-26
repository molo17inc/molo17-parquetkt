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

import org.json.JSONArray
import java.io.File

/**
 * Test-only helper that shells out to python/pyarrow to validate that
 * the Parquet files produced by the writer are readable by reference
 * implementations. This bypasses the in-progress Kotlin reader while
 * we finish the Thrift deserialization work.
 */
object PyArrowUtils {

    private fun runPyArrowScript(script: String, vararg args: String): String {
        val command = mutableListOf("python3", "-c", script)
        command.addAll(args)

        val process = ProcessBuilder(command)
            .redirectErrorStream(true)
            .start()

        val output = process.inputStream.bufferedReader().readText().trim()
        val exitCode = process.waitFor()

        if (exitCode != 0) {
            throw IllegalStateException("PyArrow script failed with exit code $exitCode: $output")
        }

        return output
    }

    fun readRecords(file: File): List<Map<String, Any?>> {
        val script = """
import json
import pyarrow.parquet as pq
import sys

table = pq.read_table(sys.argv[1])
print(json.dumps(table.to_pylist()))
""".trimIndent()

        val output = runPyArrowScript(script, file.absolutePath)
        if (output.isEmpty()) return emptyList()

        val array = JSONArray(output)
        val records = mutableListOf<Map<String, Any?>>()
        for (i in 0 until array.length()) {
            val obj = array.getJSONObject(i)
            val map = obj.keys().asSequence().associateWith { key ->
                val value = obj.get(key)
                if (value == org.json.JSONObject.NULL) null else value
            }
            records.add(map)
        }
        return records
    }

    fun readSchemaFields(file: File): List<String> {
        val script = """
import json
import pyarrow.parquet as pq
import sys

schema = pq.read_schema(sys.argv[1])
print(json.dumps(schema.names))
""".trimIndent()

        val output = runPyArrowScript(script, file.absolutePath)
        if (output.isEmpty()) return emptyList()
        val array = JSONArray(output)
        return List(array.length()) { idx -> array.getString(idx) }
    }
}
