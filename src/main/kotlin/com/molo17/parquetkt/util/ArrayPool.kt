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

package com.molo17.parquetkt.util

import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.math.max

/**
 * Thread-safe pool for reusable byte arrays to reduce GC pressure.
 * Arrays are organized by size buckets for efficient allocation.
 */
class ArrayPool(
    private val maxPoolSize: Int = DEFAULT_MAX_POOL_SIZE,
    private val maxArraySize: Int = DEFAULT_MAX_ARRAY_SIZE
) {
    // Size buckets: 1KB, 4KB, 16KB, 64KB, 256KB, 1MB, 4MB
    private val buckets = mapOf(
        1024 to ConcurrentLinkedQueue<ByteArray>(),
        4 * 1024 to ConcurrentLinkedQueue<ByteArray>(),
        16 * 1024 to ConcurrentLinkedQueue<ByteArray>(),
        64 * 1024 to ConcurrentLinkedQueue<ByteArray>(),
        256 * 1024 to ConcurrentLinkedQueue<ByteArray>(),
        1024 * 1024 to ConcurrentLinkedQueue<ByteArray>(),
        4 * 1024 * 1024 to ConcurrentLinkedQueue<ByteArray>()
    )
    
    private val bucketSizes = buckets.keys.sorted()
    
    /**
     * Rent a byte array of at least the specified size.
     * Returns a pooled array if available, otherwise allocates a new one.
     */
    fun rent(minimumSize: Int): ByteArray {
        require(minimumSize > 0) { "Size must be positive" }
        
        // Find the appropriate bucket
        val bucketSize = findBucketSize(minimumSize)
        
        // Try to get from pool
        if (bucketSize <= maxArraySize) {
            val queue = buckets[bucketSize]
            val array = queue?.poll()
            if (array != null) {
                return array
            }
        }
        
        // Allocate new array
        return ByteArray(max(minimumSize, bucketSize))
    }
    
    /**
     * Return a byte array to the pool for reuse.
     * Arrays larger than maxArraySize are not pooled.
     */
    fun returnArray(array: ByteArray) {
        val size = array.size
        
        // Don't pool arrays that are too large
        if (size > maxArraySize) {
            return
        }
        
        // Find the bucket this array belongs to
        val bucketSize = findBucketSize(size)
        val queue = buckets[bucketSize] ?: return
        
        // Only add to pool if not at capacity
        if (queue.size < maxPoolSize) {
            // Clear the array before returning to pool (security/privacy)
            array.fill(0)
            queue.offer(array)
        }
    }
    
    /**
     * Clear all pooled arrays.
     */
    fun clear() {
        buckets.values.forEach { it.clear() }
    }
    
    /**
     * Get statistics about the pool.
     */
    fun getStats(): PoolStats {
        val bucketStats = buckets.map { (size, queue) ->
            BucketStats(size, queue.size)
        }
        return PoolStats(bucketStats)
    }
    
    private fun findBucketSize(requestedSize: Int): Int {
        // Find the smallest bucket that can fit the requested size
        return bucketSizes.firstOrNull { it >= requestedSize } 
            ?: requestedSize // If larger than all buckets, use exact size
    }
    
    data class PoolStats(val buckets: List<BucketStats>) {
        val totalArrays: Int = buckets.sumOf { it.count }
        
        override fun toString(): String {
            return buildString {
                appendLine("ArrayPool Statistics:")
                appendLine("  Total pooled arrays: $totalArrays")
                buckets.forEach { bucket ->
                    appendLine("  ${bucket.sizeKB} KB: ${bucket.count} arrays")
                }
            }
        }
    }
    
    data class BucketStats(val size: Int, val count: Int) {
        val sizeKB: Int = size / 1024
    }
    
    companion object {
        const val DEFAULT_MAX_POOL_SIZE = 16 // Max arrays per bucket
        const val DEFAULT_MAX_ARRAY_SIZE = 4 * 1024 * 1024 // 4 MB
        
        /**
         * Shared global instance for general use.
         * Can be used across the application to reduce allocations.
         */
        val shared = ArrayPool()
    }
}

/**
 * Extension function to use an array from the pool and automatically return it.
 */
inline fun <R> ArrayPool.use(minimumSize: Int, block: (ByteArray) -> R): R {
    val array = rent(minimumSize)
    try {
        return block(array)
    } finally {
        returnArray(array)
    }
}

/**
 * Extension function to copy data to a properly sized array.
 * Useful when you need to return data from a pooled array.
 */
fun ByteArray.copyToSized(actualSize: Int): ByteArray {
    require(actualSize <= size) { "Actual size cannot exceed array size" }
    return copyOf(actualSize)
}
