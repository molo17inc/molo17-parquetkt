
import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.serialization.ParquetSerializer
import com.molo17.parquetkt.serialization.SchemaReflector

data class MinimalData(val id: Int)

fun main() {
    val data = listOf(MinimalData(1), MinimalData(2), MinimalData(3))
    val schema = SchemaReflector.reflectSchema<MinimalData>()
    val serializer = ParquetSerializer.create<MinimalData>()
    val rowGroup = serializer.serialize(data, schema)
    ParquetFile.write("test-output/our_minimal.parquet", schema, listOf(rowGroup))
    println("File created")
}
