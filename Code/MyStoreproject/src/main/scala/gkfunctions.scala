import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, TimestampType, DoubleType}

object gkfunctions {
  def read_schema(schema_arg : String) = {
    var sch : StructType = new StructType
    val split_values = schema_arg.split(",").toList

    val d_types = Map(
      "StringType" -> StringType,
      "IntegerType" -> IntegerType,
      "TimestampType" -> TimestampType,
      "DoubleType" -> DoubleType
    )

    for(i <- split_values){
      val columnVal = i.split(" ").toList
      sch = sch.add(columnVal(0), d_types(columnVal(1)), true)
    }
    sch

  }

}