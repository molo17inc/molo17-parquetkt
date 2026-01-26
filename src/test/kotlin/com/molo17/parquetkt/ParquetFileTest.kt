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
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals

data class Person(
    val name: String,
    val age: Int,
    val salary: Double,
    val isActive: Boolean
)

data class Employee(
    val id: Long,
    val name: String,
    val department: String?,
    val salary: Double
)

class ParquetFileTest {
    
    @TempDir
    lateinit var tempDir: File
    
    @Test
    fun `test write and read simple objects`() {
        val people = listOf(
            Person("Alice", 30, 75000.0, true),
            Person("Bob", 25, 65000.0, true),
            Person("Charlie", 35, 85000.0, false)
        )
        
        val file = File(tempDir, "people.parquet")
        
        // Write objects
        ParquetFile.writeObjects(file, people, CompressionCodec.SNAPPY)
        
        // Read objects back using PyArrow
        val readRecords = PyArrowUtils.readRecords(file)
        
        assertEquals(people.size, readRecords.size)
        // Convert PyArrow records back to Person objects for comparison
        val readPeople = readRecords.map { record ->
            Person(
                name = record["name"] as String,
                age = record["age"] as Int,
                salary = record["salary"] as Double,
                isActive = record["isActive"] as Boolean
            )
        }
        assertEquals(people, readPeople)
    }
    
    @Test
    fun `test write and read with nullable fields`() {
        val employees = listOf(
            Employee(1L, "Alice", "Engineering", 75000.0),
            Employee(2L, "Bob", null, 65000.0),
            Employee(3L, "Charlie", "Sales", 85000.0)
        )
        
        val file = File(tempDir, "employees.parquet")
        
        ParquetFile.writeObjects(file, employees)
        val readEmployees = ParquetFile.readObjects<Employee>(file)
        
        assertEquals(employees.size, readEmployees.size)
        assertEquals(employees, readEmployees)
    }
    
    @Test
    fun `test read schema from file`() {
        val people = listOf(
            Person("Alice", 30, 75000.0, true)
        )
        
        val file = File(tempDir, "schema_test.parquet")
        ParquetFile.writeObjects(file, people)
        
        val fieldNames = PyArrowUtils.readSchemaFields(file)
        
        assertEquals(4, fieldNames.size)
        assert(fieldNames.contains("name"))
        assert(fieldNames.contains("age"))
        assert(fieldNames.contains("salary"))
        assert(fieldNames.contains("isActive"))
    }
}
        val schema = ParquetFile.readSchema(file)
        
        assertEquals(4, schema.fieldCount)
        assert(schema.hasField("name"))
        assert(schema.hasField("age"))
        assert(schema.hasField("salary"))
        assert(schema.hasField("isActive"))
    }
}
