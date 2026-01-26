#!/bin/bash

echo "=================================="
echo "Parquet-Kotlin Performance Benchmark"
echo "=================================="
echo ""

# Check if Gradle wrapper exists
if [ ! -f "./gradlew" ]; then
    echo "Gradle wrapper not found. Please run:"
    echo "  gradle wrapper"
    echo "or install Gradle and run this script again."
    exit 1
fi

# Make gradlew executable
chmod +x ./gradlew

# Run the benchmark test
echo "Running benchmark: 100 columns x 500,000 rows..."
echo ""

./gradlew test --tests "io.github.parquetkt.PerformanceBenchmarkTest" --info

echo ""
echo "Benchmark complete!"
