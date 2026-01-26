#!/usr/bin/env python3
"""
Debug script to examine the Thrift metadata in the Parquet file
"""
import struct
import sys

def read_footer(filename):
    with open(filename, 'rb') as f:
        # Read file size
        f.seek(0, 2)
        file_size = f.tell()
        
        # Check start magic
        f.seek(0)
        start_magic = f.read(4)
        print(f"Start magic: {start_magic} (expected: b'PAR1')")
        
        # Check end magic
        f.seek(file_size - 4)
        end_magic = f.read(4)
        print(f"End magic: {end_magic} (expected: b'PAR1')")
        
        # Read metadata length (4 bytes before end magic)
        f.seek(file_size - 8)
        metadata_length_bytes = f.read(4)
        metadata_length = struct.unpack('<I', metadata_length_bytes)[0]
        print(f"Metadata length: {metadata_length} bytes (0x{metadata_length:x})")
        
        # Calculate metadata offset
        metadata_offset = file_size - 8 - metadata_length
        print(f"Metadata offset: {metadata_offset} (0x{metadata_offset:x})")
        print(f"File size: {file_size}")
        
        # Read metadata
        f.seek(metadata_offset)
        metadata_bytes = f.read(metadata_length)
        
        print(f"\nFirst 100 bytes of metadata (hex):")
        print(metadata_bytes[:100].hex())
        
        print(f"\nFirst 100 bytes of metadata (with ASCII):")
        for i in range(0, min(100, len(metadata_bytes)), 16):
            hex_part = ' '.join(f'{b:02x}' for b in metadata_bytes[i:i+16])
            ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in metadata_bytes[i:i+16])
            print(f"{i:04x}: {hex_part:<48} {ascii_part}")
        
        # Try to parse with pyarrow
        try:
            import pyarrow.parquet as pq
            table = pq.read_table(filename)
            print(f"\n✅ SUCCESS! File can be read by pyarrow")
            print(f"Rows: {table.num_rows}, Columns: {table.num_columns}")
        except Exception as e:
            print(f"\n❌ ERROR: {e}")

if __name__ == '__main__':
    filename = 'benchmark-output/benchmark_100cols_500krows.parquet'
    read_footer(filename)
