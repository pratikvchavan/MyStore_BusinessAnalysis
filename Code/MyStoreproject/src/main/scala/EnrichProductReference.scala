import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import com.typesafe.config.{Config, ConfigFactory}
import java.time.LocalDate

object EnrichProductReference {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("EnrichProductReference").master("local[*]")
      .getOrCreate()

    val gkconfig : Config = ConfigFactory.load("application.conf")
    val inputLocation = gkconfig.getString("paths.inputLocation")
    val outputLocation = gkconfig.getString("paths.outputLocation")

    // Reading valid data
    val validFileSchema  = StructType(List(
      StructField("Sale_ID", StringType, false),
      StructField("Product_ID", StringType, false),
      StructField("Quantity_Sold", IntegerType, false),
      StructField("Vendor_ID", StringType, false),
      StructField("Sale_Date", TimestampType, false),
      StructField("Sale_Amount", DoubleType, false),
      StructField("Sale_Currency", StringType, false)
    ))

    val productPriceReferenceSchema = StructType(List(
      StructField("Product_ID",StringType, true),
      StructField("Product_Name",StringType, true),
      StructField("Product_Price",IntegerType, true),
      StructField("Product_Price_Currency",StringType, true),
      StructField("Product_updated_date",TimestampType, true)
    ))

    val dateToday = LocalDate.now()
    val yesterDate = dateToday.minusDays(1)

    //val currDayZoneSuffix  = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currDayZoneSuffix  = "_19072020"
    //val prevDayZoneSuffix = "_" + yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val prevDayZoneSuffix = "_18072020"

    val validDataDF = spark.read
      .schema(validFileSchema)
      .option("delimiter", "|")
      .option("header", true)
      .csv(outputLocation + "Valid/ValidData"+currDayZoneSuffix)
    validDataDF.createOrReplaceTempView("validData")
    //validDataDF.show()

    //Reading Project Reference
    val productPriceReferenceDF = spark.read
      .schema(productPriceReferenceSchema)
      .option("delimiter", "|")
      .option("header", true)
      .csv(inputLocation + "Products")
    productPriceReferenceDF.createOrReplaceTempView("productPriceReferenceDF")
    //productPriceReferenceDF.show()

    val productEnrichedDF = spark.sql("select a.Sale_ID, a.Product_ID, b.Product_Name, " +
      "a.Quantity_Sold, a.Vendor_ID, a.Sale_Date, " +
      "b.Product_Price * a.Quantity_Sold AS Sale_Amount, " +
      "a.Sale_Currency " +
      "from validData a INNER JOIN productPriceReferenceDF b " +
      "ON a.Product_ID = b.Product_ID")

    productEnrichedDF.show()

    productEnrichedDF.write
      .option("header", true)
      .option("delimiter","|")
      .mode("overwrite")
      .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currDayZoneSuffix)

  }

}
