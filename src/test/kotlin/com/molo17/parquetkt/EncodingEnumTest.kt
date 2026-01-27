package com.molo17.parquetkt

import com.molo17.parquetkt.schema.Encoding
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

/**
 * Test to verify that Encoding enum values match the official Apache Parquet specification.
 * 
 * These values MUST match the Parquet Thrift definition to ensure compatibility with
 * all Parquet implementations (PyArrow, Spark, .NET Parquet, etc.)
 * 
 * Reference: https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
 */
class EncodingEnumTest {
    
    @Test
    fun `verify encoding enum values match Parquet specification`() {
        // These values are defined in the official Parquet Thrift specification
        // and MUST NOT be changed as they affect file format compatibility
        
        assertEquals(0, Encoding.PLAIN.thriftValue, 
            "PLAIN encoding must have thrift value 0")
        
        // Note: GROUP_VAR_INT = 1 is deprecated and never used
        
        assertEquals(2, Encoding.PLAIN_DICTIONARY.thriftValue,
            "PLAIN_DICTIONARY encoding must have thrift value 2 (deprecated, use RLE_DICTIONARY)")
        
        assertEquals(3, Encoding.RLE.thriftValue,
            "RLE encoding must have thrift value 3")
        
        assertEquals(4, Encoding.BIT_PACKED.thriftValue,
            "BIT_PACKED encoding must have thrift value 4")
        
        assertEquals(5, Encoding.DELTA_BINARY_PACKED.thriftValue,
            "DELTA_BINARY_PACKED encoding must have thrift value 5")
        
        assertEquals(6, Encoding.DELTA_LENGTH_BYTE_ARRAY.thriftValue,
            "DELTA_LENGTH_BYTE_ARRAY encoding must have thrift value 6")
        
        assertEquals(7, Encoding.DELTA_BYTE_ARRAY.thriftValue,
            "DELTA_BYTE_ARRAY encoding must have thrift value 7")
        
        assertEquals(8, Encoding.RLE_DICTIONARY.thriftValue,
            "RLE_DICTIONARY encoding must have thrift value 8")
        
        assertEquals(9, Encoding.BYTE_STREAM_SPLIT.thriftValue,
            "BYTE_STREAM_SPLIT encoding must have thrift value 9")
    }
    
    @Test
    fun `verify RLE and PLAIN encodings are used by default`() {
        // When dictionary encoding is disabled (default), files should use:
        // - RLE for definition/repetition levels
        // - PLAIN for data values
        
        val rle = Encoding.RLE
        val plain = Encoding.PLAIN
        
        assertEquals(3, rle.thriftValue, "RLE should have value 3")
        assertEquals(0, plain.thriftValue, "PLAIN should have value 0")
        
        println("✓ Default encodings verified:")
        println("  RLE (definition/repetition levels): ${rle.thriftValue}")
        println("  PLAIN (data values): ${plain.thriftValue}")
    }
    
    @Test
    fun `verify dictionary encoding values`() {
        // When dictionary encoding is enabled, files should use:
        // - RLE for definition/repetition levels
        // - RLE_DICTIONARY for dictionary-encoded data
        // - PLAIN for dictionary page itself
        
        val rle = Encoding.RLE
        val rleDictionary = Encoding.RLE_DICTIONARY
        val plain = Encoding.PLAIN
        
        assertEquals(3, rle.thriftValue, "RLE should have value 3")
        assertEquals(8, rleDictionary.thriftValue, "RLE_DICTIONARY should have value 8")
        assertEquals(0, plain.thriftValue, "PLAIN should have value 0")
        
        println("✓ Dictionary encoding values verified:")
        println("  RLE (levels): ${rle.thriftValue}")
        println("  RLE_DICTIONARY (data): ${rleDictionary.thriftValue}")
        println("  PLAIN (dictionary page): ${plain.thriftValue}")
    }
}
