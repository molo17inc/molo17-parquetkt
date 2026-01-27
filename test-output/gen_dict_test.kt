
import com.molo17.parquetkt.core.ParquetFile
import com.molo17.parquetkt.serialization.ParquetSerializer
import com.molo17.parquetkt.serialization.SchemaReflector
import java.io.File

data class RepetitiveData(val id: Int, val category: String, val name: String)

fun main() {
    val data = (1..1000).map { i ->
        RepetitiveData(
            id = i,
            category = "Category_\${i % 5}",  // Only 5 unique values
            name = "Name_\${i % 10}"          // Only 10 unique values
        )
    }
    
    val schema = SchemaReflector.reflectSchema<RepetitiveData>()
    val serializer = ParquetSerializer.create<RepetitiveData>()
    val rowGroup = serializer.serialize(data, schema)
    
    ParquetFile.write("test-output/dict_test.parquet", schema, listOf(rowGroup))
    println("File created: test-output/dict_test.parquet")
}
