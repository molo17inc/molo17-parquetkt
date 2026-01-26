#!/bin/bash

echo "=========================================="
echo "Parquet File Verification"
echo "=========================================="
echo ""

FILE="benchmark-output/benchmark_100cols_500krows.parquet"

if [ ! -f "$FILE" ]; then
    echo "❌ File not found: $FILE"
    echo "Run the benchmark first:"
    echo "  ./gradlew test --tests 'io.github.parquetkt.PerformanceBenchmarkTest.benchmark - write 100 columns with 500K rows'"
    exit 1
fi

echo "✅ File found: $FILE"
echo "File size: $(ls -lh $FILE | awk '{print $5}')"
echo ""

# Check magic numbers
echo "Checking magic numbers..."
head -c 4 "$FILE" | xxd -p | grep -q "50415231" && echo "✅ Start magic: PAR1" || echo "❌ Invalid start magic"
tail -c 4 "$FILE" | xxd -p | grep -q "50415231" && echo "✅ End magic: PAR1" || echo "❌ Invalid end magic"
echo ""

# Try reading with Python pyarrow if available
if command -v python3 &> /dev/null; then
    echo "Attempting to read with Python pyarrow..."
    python3 << 'EOF'
try:
    import pyarrow.parquet as pq
    table = pq.read_table('benchmark-output/benchmark_100cols_500krows.parquet')
    print(f"✅ Successfully read file!")
    print(f"   Rows: {table.num_rows}")
    print(f"   Columns: {table.num_columns}")
    print(f"   Schema fields: {len(table.schema)}")
except ImportError:
    print("⚠️  pyarrow not installed. Install with: pip3 install pyarrow")
except Exception as e:
    print(f"❌ Error reading file: {e}")
EOF
    echo ""
fi

# Try reading with parquet-tools if available
if command -v parquet-tools &> /dev/null; then
    echo "Reading metadata with parquet-tools..."
    parquet-tools meta "$FILE" 2>&1 | head -20
elif command -v parquet &> /dev/null; then
    echo "Reading metadata with parquet CLI..."
    parquet "$FILE" 2>&1 | head -20
else
    echo "⚠️  parquet-tools not installed. Install with: brew install parquet-tools"
fi

echo ""
echo "=========================================="
echo "Verification complete!"
echo "=========================================="
