package com.molo17.parquetkt

import com.molo17.parquetkt.util.ArrayPool
import com.molo17.parquetkt.util.use
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ArrayPoolTest {
    
    @Test
    fun `test rent and return basic functionality`() {
        val pool = ArrayPool(maxPoolSize = 5)
        
        // Rent an array
        val array1 = pool.rent(1024)
        assertTrue(array1.size >= 1024)
        
        // Return it
        pool.returnArray(array1)
        
        // Rent again - should get the same array
        val array2 = pool.rent(1024)
        assertTrue(array2 === array1, "Should reuse the same array")
    }
    
    @Test
    fun `test bucket sizing`() {
        val pool = ArrayPool()
        
        // Request 100 bytes, should get 1KB bucket
        val small = pool.rent(100)
        assertEquals(1024, small.size)
        
        // Request 5KB, should get 16KB bucket
        val medium = pool.rent(5 * 1024)
        assertEquals(16 * 1024, medium.size)
        
        // Request 100KB, should get 256KB bucket
        val large = pool.rent(100 * 1024)
        assertEquals(256 * 1024, large.size)
        
        pool.returnArray(small)
        pool.returnArray(medium)
        pool.returnArray(large)
    }
    
    @Test
    fun `test pool capacity limit`() {
        val pool = ArrayPool(maxPoolSize = 2)
        
        // Create and return 3 arrays
        val arrays = List(3) { pool.rent(1024) }
        arrays.forEach { pool.returnArray(it) }
        
        // Pool should only keep 2
        val stats = pool.getStats()
        val bucket1KB = stats.buckets.find { it.size == 1024 }
        assertEquals(2, bucket1KB?.count)
    }
    
    @Test
    fun `test use extension function`() {
        val pool = ArrayPool()
        var usedArray: ByteArray? = null
        
        val result = pool.use(1024) { array ->
            usedArray = array
            array[0] = 42
            "success"
        }
        
        assertEquals("success", result)
        
        // Array should be returned to pool and cleared
        val nextArray = pool.rent(1024)
        assertTrue(nextArray === usedArray)
        assertEquals(0, nextArray[0], "Array should be cleared")
    }
    
    @Test
    fun `test concurrent access`() {
        val pool = ArrayPool()
        val threads = 10
        val iterationsPerThread = 100
        
        val threadList = List(threads) {
            Thread {
                repeat(iterationsPerThread) {
                    val array = pool.rent(4 * 1024)
                    // Simulate some work
                    array[0] = it.toByte()
                    Thread.sleep(1)
                    pool.returnArray(array)
                }
            }
        }
        
        threadList.forEach { it.start() }
        threadList.forEach { it.join() }
        
        // Should have some arrays in pool
        val stats = pool.getStats()
        assertTrue(stats.totalArrays > 0)
        
        println("✅ Concurrent access test passed")
        println(stats)
    }
    
    @Test
    fun `test large array not pooled`() {
        val pool = ArrayPool(maxArraySize = 1024 * 1024) // 1 MB max
        
        // Request 5 MB array
        val largeArray = pool.rent(5 * 1024 * 1024)
        assertEquals(5 * 1024 * 1024, largeArray.size)
        
        // Return it
        pool.returnArray(largeArray)
        
        // Should not be in pool
        val stats = pool.getStats()
        assertEquals(0, stats.totalArrays)
    }
    
    @Test
    fun `test pool statistics`() {
        val pool = ArrayPool()
        
        // Rent and return arrays of different sizes
        val arrays = listOf(
            pool.rent(512),      // 1KB bucket
            pool.rent(2 * 1024), // 4KB bucket
            pool.rent(10 * 1024), // 16KB bucket
            pool.rent(50 * 1024)  // 64KB bucket
        )
        
        arrays.forEach { pool.returnArray(it) }
        
        val stats = pool.getStats()
        assertEquals(4, stats.totalArrays)
        
        println("✅ Pool statistics test passed")
        println(stats)
    }
    
    @Test
    fun `test clear pool`() {
        val pool = ArrayPool()
        
        // Add some arrays
        repeat(5) {
            val array = pool.rent(1024)
            pool.returnArray(array)
        }
        
        assertTrue(pool.getStats().totalArrays > 0)
        
        // Clear pool
        pool.clear()
        
        assertEquals(0, pool.getStats().totalArrays)
    }
}
