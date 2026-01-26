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


package com.molo17.parquetkt.examples

import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.data.DataColumn
import com.molo17.parquetkt.data.RowGroup
import com.molo17.parquetkt.schema.CompressionCodec
import com.molo17.parquetkt.schema.DataField
import com.molo17.parquetkt.schema.ParquetSchema
import java.io.File

data class User(
    val id: Long,
    val name: String,
    val email: String,
    val age: Int,
    val balance: Double,
    val isActive: Boolean
)

fun main() {
    basicExample()
    println("\n" + "=".repeat(50) + "\n")
    lowLevelExample()
    println("\n" + "=".repeat(50) + "\n")
    highLevelExample()
}

fun basicExample() {
    println("=== Basic Example: High-Level API ===\n")
    
    val users = listOf(
        User(1L, "Alice Johnson", "alice@example.com", 30, 1500.50, true),
        User(2L, "Bob Smith", "bob@example.com", 25, 2300.75, true),
        User(3L, "Charlie Brown", "charlie@example.com", 35, 500.00, false),
        User(4L, "Diana Prince", "diana@example.com", 28, 3200.25, true)
    )
    
    val file = File("users.parquet")
    
    println("Writing ${users.size} users to ${file.name}...")
    ParquetFile.writeObjects(file, users, CompressionCodec.SNAPPY)
    println("✓ Write completed\n")
    
    println("Reading users from ${file.name}...")
    val readUsers = ParquetFile.readObjects<User>(file)
    println("✓ Read ${readUsers.size} users\n")
    
    println("Users:")
    readUsers.forEach { user ->
        println("  - ${user.name} (${user.age}): ${user.email} - Balance: $${user.balance}")
    }
    
    file.delete()
}

fun lowLevelExample() {
    println("=== Low-Level API Example ===\n")
    
    val schema = ParquetSchema.create(
        DataField.int64("id", nullable = false),
        DataField.string("name", nullable = false),
        DataField.int32("age", nullable = false),
        DataField.double("salary", nullable = false)
    )
    
    println("Schema created:")
    println(schema)
    
    val idColumn = DataColumn.createRequired(
        DataField.int64("id"),
        listOf(1L, 2L, 3L, 4L, 5L)
    )
    
    val nameColumn = DataColumn.createRequired(
        DataField.string("name"),
        listOf("Alice", "Bob", "Charlie", "Diana", "Eve")
    )
    
    val ageColumn = DataColumn.createRequired(
        DataField.int32("age"),
        listOf(30, 25, 35, 28, 32)
    )
    
    val salaryColumn = DataColumn.createRequired(
        DataField.double("salary"),
        listOf(75000.0, 65000.0, 85000.0, 72000.0, 68000.0)
    )
    
    val rowGroup = RowGroup(
        schema,
        listOf(idColumn, nameColumn, ageColumn, salaryColumn)
    )
    
    println("Created row group with ${rowGroup.rowCount} rows")
    
    val file = File("employees_lowlevel.parquet")
    println("\nWriting to ${file.name}...")
    ParquetFile.write(file, schema, listOf(rowGroup), CompressionCodec.SNAPPY)
    println("✓ Write completed")
    
    file.delete()
}

fun highLevelExample() {
    println("=== High-Level API with Streaming ===\n")
    
    data class Product(
        val id: Long,
        val name: String,
        val price: Double,
        val inStock: Boolean
    )
    
    val products = listOf(
        Product(1L, "Laptop", 999.99, true),
        Product(2L, "Mouse", 29.99, true),
        Product(3L, "Keyboard", 79.99, false),
        Product(4L, "Monitor", 299.99, true),
        Product(5L, "Webcam", 89.99, true)
    )
    
    val file = File("products.parquet")
    
    println("Writing ${products.size} products...")
    ParquetFile.writeObjects(file, products)
    println("✓ Write completed\n")
    
    println("Reading products as sequence (streaming)...")
    val rows = ParquetFile.readRowsAsSequence(file)
    
    println("\nProducts in stock:")
    rows.filter { it["inStock"] == true }
        .forEach { row ->
            println("  - ${row["name"]}: $${row["price"]}")
        }
    
    file.delete()
}
