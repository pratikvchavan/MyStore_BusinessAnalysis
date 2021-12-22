import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, TimestampType, DoubleType, FloatType}
import com.typesafe.config.{Config, ConfigFactory}
//import java.time.format.DateTimeFormatter
import java.time.LocalDate
//import org.apache.spark.sql.functions.{col, when}

object VendorEnrichment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("VendorEnrichment").master("local[*]")
      .getOrCreate()

    //paths
    val gkconfig : Config = ConfigFactory.load("application.conf")
    val inputLocation = gkconfig.getString("paths.inputLocation")
    val outputLocation = gkconfig.getString("paths.outputLocation")

    //Dates
    val dateToday = LocalDate.now()
    val yesterDate = dateToday.minusDays(1)
    //val currDayZoneSuffix  = "_" + dateToday.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val currDayZoneSuffix  = "_19072020"
    //val prevDayZoneSuffix = "_" + yesterDate.format(DateTimeFormatter.ofPattern("ddMMyyyy"))
    val prevDayZoneSuffix = "_18072020"

    val productEnrichedInputSchema = StructType(List(
      StructField("Sale_ID",StringType, true),
      StructField("Product_ID",StringType, true),
      StructField("Product_Name",StringType, true),
      StructField("Quantity_Sold",IntegerType, true),
      StructField("Vendor_ID",StringType, true),
      StructField("Sale_Date",TimestampType, true),
      StructField("Sale_Amount",DoubleType, true),
      StructField("Sale_Currency",StringType, true)
    ))

    val vendorReferenceSchema = StructType(List(
      StructField("Vendor_ID",StringType, true),
      StructField("Vendor_Name",StringType, true),
      StructField("Vendor_Add_Street",StringType, true),
      StructField("Vendor_Add_City",StringType, true),
      StructField("Vendor_Add_State",StringType, true),
      StructField("Vendor_Add_Country",StringType, true),
      StructField("Vendor_Add_Zip",StringType, true),
      StructField("Vendor_Updated_Date",TimestampType, true)
    ))

    val usdReferenceSchema = StructType(List(
      StructField("Currency", StringType, true),
      StructField("Currency_Code", StringType, true),
      StructField("Exchange_Rate", FloatType, true),
      StructField("Currency_Updated_Date", TimestampType, true)
    ))

    //Reading the required zones

    val productEnrichedDF = spark.read
      .schema(productEnrichedInputSchema)
      .option("delimiter", "|")
      .option("header", true)
      .csv(outputLocation + "Enriched/SaleAmountEnrichment/SaleAmountEnriched" + currDayZoneSuffix)
    productEnrichedDF.createOrReplaceTempView("productEnrichedDF")

    val usdReferenceDF = spark.read
      .schema(usdReferenceSchema)
      .option("delimiter", "|")
      .csv(inputLocation + "USD_Rates")
    usdReferenceDF.createOrReplaceTempView("usdReferenceDF")

    val vendorReferenceDF = spark.read
      .schema(vendorReferenceSchema)
      .option("delimiter", "|")
      .option("header", false)
      .csv(inputLocation + "Vendors")
    vendorReferenceDF.createOrReplaceTempView("vendorReferenceDF")

    val vendorEnrichedDF = spark.sql("select a.*, b.Vendor_Name FROM " +
      "productEnrichedDF a INNER JOIN vendorReferenceDF b " +
      "ON a.Vendor_ID = b.Vendor_ID")
    vendorEnrichedDF.createOrReplaceTempView("vendorEnrichedDF")

    val usdEnriched = spark.sql("select *, ROUND((a.Sale_Amount / b.Exchange_Rate),2) AS Amount_USD " +
      "from vendorEnrichedDF a JOIN usdReferenceDF b " +
      "ON a.Sale_Currency = b.Currency_Code")
    usdEnriched.createOrReplaceTempView("usdEnriched")

    usdEnriched.write
      .option("header", true)
      .option("delimiter","|")
      .mode("overwrite")
      .csv(outputLocation + "Enriched/Vendor_USD_Enriched/Vendor_USD_Enriched" + currDayZoneSuffix)

    val reportDF = spark.sql("select Sale_ID, Product_ID, Product_Name, " +
      "Quantity_Sold, Vendor_ID, Sale_Date, Sale_Amount, " +
      "Sale_Currency, Vendor_Name, Amount_USD FROM usdEnriched")

     //Mysql connectivity

//    reportDF.write.format("jdbc")
//      .options(Map(
//        "url" -> "jdbc:mysql://localhost:3306/mystoreprojectdb",
//        "driver" -> "com.mysql.jdbc.Driver",
//        "dbtable" -> "finale_sales",
//        "user" -> "root",
//        "password" -> "root"
//      ))
//      .mode("append")
//      .save()

  }

}
