import com.microsoft.sqlserver.jdbc.SQLServerDriver

|

import java.text.SimpleDateFormat

|

import java.util.Calendar

|

import org.apache.spark.sql.functions._

|

import scala.collection.JavaConversions._

|

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}

|

import org.apache.spark.storage.StorageLevel

|

import org.apache.spark.sql.types._

|

import org.apache.hadoop.fs.{FileContext, FileSystem, Path}

|

import org.apache.hadoop.fs.Options.Rename

|

import scala.collection.mutable

|

import org.apache.hadoop.hive.conf.HiveConf

|

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

|

import org.apache.hadoop.hive.metastore.api._

|

import org.apache.hadoop.hive.metastore.TableType

|

import scala.collection.JavaConverters._

|
|
| val dfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
| val dfc = FileContext.getFileContext(spark.sparkContext.hadoopConfiguration)
| val hiveMetaStore = new HiveMetaStoreClient(new HiveConf())
|
| val sparkToHiveDataType: Map[String, String] = Map("String" -> "string", "Long" -> "bigint", "Integer" -> "int", "Date" -> "date", "Timestamp" -> "timestamp", "Decimal" -> "decimal")
|
| val formatInfo = Map("orc" -> Map("serde" -> "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
  | "inputFormat" -> "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
| "outputFormat" -> "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
| "parquet" -> Map("serde" -> "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
  | "inputFormat" -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
| "outputFormat" -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
| "csv" -> Map("serde" -> "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
  | "inputFormat" -> "org.apache.hadoop.mapred.TextInputFormat",
| "outputFormat" -> "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat") )
|
|
| def getColsFromDF(df: DataFrame) = {
  | println ("Extracting Column Info from DataFrame")
  |
  val cols = mutable.ArrayBuffer[(String, String)]()
  |
  for (column <- df.dtypes) {
    |
    val (col, dataType) = column
    | cols +=((col, sparkToHiveDataType(dataType.split("Type").head)))
    |
  }
  | cols
    |
}
|
| def checkDDL(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, cols: Seq[(String, String)]): Boolean = {
  |
  |
  val tbl = hiveMetaStore.getTable(dbName, tblName)
  |
  val hiveCols = tbl.getSd.getCols.asScala.map(x => (x.getName, x.getType)).toArray
  | cols
  .map(x => (x._1.toLowerCase, x._2.toLowerCase)) == hiveCols.deep
  |
  |
}
|
|
| def checkAndCreateHiveDDL(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, format: String,
                            | location: String, cols: Seq[(String, String)]) = {
  |
  |
  val tblExist = hiveMetaStore.tableExists(dbName, tblName)
  |
  |
  val ddlMatch = if (tblExist) checkDDL(hiveMetaStore, dbName, tblName, cols) else false
  |
  |
  if (!tblExist || !ddlMatch) {
    | println ("TABLE Does Not Exists OR DDL Mismatch, Creating New One")
    |
    |
    if (tblExist) hiveMetaStore.dropTable(dbName, tblName)
    |
    |
    val s = new SerDeInfo()
    | s
    .setSerializationLib(formatInfo(format)("serde"))
    | s
    .setParameters(Map("serialization.format" -> "1").asJava)
    |
    |
    val sd = new StorageDescriptor()
    | sd
    .setSerdeInfo(s)
    | sd
    .setInputFormat(formatInfo(format)("inputFormat"))
    | sd
    .setOutputFormat(formatInfo(format)("outputFormat"))
    |
    |
    val tblCols = mutable.ListBuffer[List[FieldSchema]]()
    |
    for (col <- cols) {
      |
      val (c, dataType) = col
      | tblCols += List (new FieldSchema(c, dataType, c))
      |
    }
    | sd
    .setCols(tblCols.toList.flatMap(x => x).asJava)
    |
    | sd
    .setLocation(location)
    |
    |
    val t = new Table()
    |
    | t
    .setSd(sd)
    |
    | t
    .setTableType(TableType.EXTERNAL_TABLE.toString)
    | t
    .setDbName(dbName)
    | t
    .setTableName(tblName)
    | t
    .setParameters(Map("EXTERNAL" -> "TRUE", "tableType" -> "EXTERNAL_TABLE").asJava)
    |
    | hiveMetaStore
    .createTable(t)
    |
    |
  }
  |
  |
}
|
|
| def hdfsMove(dfc: FileContext, srcPath: String, tgtPath: String): Unit = {
  | println (s"Moving $srcPath to $tgtPath")
  | dfc
  .rename(new Path(srcPath), new Path(tgtPath), Rename.OVERWRITE)
  |
}
|
| def hdfsRemoveAndMove(dfs: FileSystem, dfc: FileContext, srcPath: String, tgtPath: String): Unit = {
  |
  if (dfs.exists(new Path(tgtPath))) dfc.delete(new Path(tgtPath), true)
  | dfs
  .mkdirs(new Path(tgtPath))
  | hdfsMove(dfc, srcPath, tgtPath)
  |
}
|
|
|// val format = new SimpleDateFormat("yyyyMMddhhmmss")
|
|// EDIT HDFS Location
  | spark.sparkContext.setCheckpointDir(dfs.getHomeDirectory.toString + "/" + spark.sparkContext.applicationId)
|
|// Replace println with the app Logger
| def createCheckPoint(df: DataFrame) = {
  | df
  .rdd.checkpoint
  |
  val dfCount = df.rdd.count
  | println (s"dfCount: $dfCount")
  | spark
  .createDataFrame(df.rdd, df.schema)
  |
}
|
| val user = "dataservices"
| val password = "..."
| val url = "jdbc:sqlserver://...:1433" // RPTDB01
|// val url = "jdbc:sqlserver://...:1433" // RPTDB02
|
| def getData(edwSqlText: String) = {
  | spark
    |.read
  |.option("user", user)
  |.option("url", url)
  |.option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  |.option("password", password)
  |.format("jdbc")
  |.option("dbtable", s"($edwSqlText) as src")
  |.load
  |
}
|
| def getPrimaryKey(db: String, tbl: String) = {
  |
  val query =
    |
  s
  """
            |       |SELECT column_name as PRIMARYKEYCOLUMN
            |       |FROM $db
.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
            |       |INNER JOIN
            |       |    $db.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
            |       |          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
            |       |             TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
            |       |             KU.table_name='$tbl'
            |     """.stripMargin
  |
  |  getData(query).collect().filterNot(_(0)
    .toString.toLowerCase contains "date")(0)(0).toString.toLowerCase
  |}
|
|def
getEDWData(sqlQuery: String, numPartitions: Int = 20) = {
  |
  |    var query = sqlQuery.replaceAll("\\s+", " ")
  |
  |    val (db, schema, table) = """(?i) FROM ([\w.]+)""".r.
    findAllIn(query).matchData.
    map(_.group(1)).
    toList(0).split("\\.") match {case Array(a,b,c) => (a,b,c)
  }
  |
  |    val partitionColumnName = getPrimaryKey(db, table)
  |
  |    val boundQueryDf = getData(s"SELECT MIN($partitionColumnName) min, MAX($partitionColumnName) max FROM $db.$
  schema.$table").collect()(0).asInstanceOf[org.apache.spark.sql.Row]
  |
  |    val (lowerBound, upperBound) = (
    boundQueryDf.get(0).toString.toLong, boundQueryDf.get(1).toString.toLong)
  |
  |    val cols = """(?i)SELECT (.*?) FROM """.r.findAllIn(query).matchData.
    map(_.group(1)).toList.head.split(",").map(_.trim).map(_.toLowerCase).toSet
  |
  |    if (!cols.contains(partitionColumnName) && cols.mkString(",") != "*") {
    |
      query = query.
      replaceFirst("(?i)SELECT ", "SELECT "+ partitionColumnName +", ")
    |    }
  |
  |    spark.read
  |      .format("jdbc")
  |      .option("user", user)
  |      .option("password",password)
  |      .option("url", url)
  |      .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  |      .option("partitionColumn", partitionColumnName)
  |      .option("lowerBound", lowerBound)
  |      .option("upperBound",

    upperBound)
  |      .option("numPartitions", numPartitions)
  |      .option("dbtable", String.format("(%s) as src", query))
  |      .load
  |  }
|
|
|/** Returns DataFrame with new nullability constraints from given schema
  * | *
  * | * @param spark =  source Spark Session
  * | * @param df = source DataFrame
  * | * @param schema =  schema with nullability constraints to be enforced on source DataFrame
  * | */
|def
enforceSchemaNullability(spark: SparkSession, df: DataFrame, schema: StructType):DataFrame = {
  | //ensures schema field names are only entered once
  |  val colCount:Int = schema.map(x => (x.name, x.nullable)).groupBy(_._1).
    mapValues(_.length).values.reduce(math.max)
  |  assert(colCount == 1,
    "Enforced schema has more than one field with the same name!")
  |
  |  val nullabilityMap:Map[String,Boolean] = schema.map(
    x => (x.name.toLowerCase, x.nullable)).toMap
  |  val newFields = df.schema.map(x
  => x.

      copy(nullable = nullabilityMap(x.name.toLowerCase)))
  |  val newSchema = StructType(newFields)
  |  spark.createDataFrame(df.rdd, newSchema)
  |}
|
|/** Returns DataFrame with new column types casted from given schema
  * | *
  * | * @param df = source DataFrame
  * | * @param schema =
   new schema to which the source DataFrame will have its columns casted to align with
  * | */
| def enforceSchemaTypes(df: DataFrame, schema: StructType):DataFrame = {
  | //ensures schema field names are only entered once
  |  val colCount:Int = schema.map(x => (x.name, x.dataType)).groupBy(_._1
  ).mapValues(_.length).values.reduce(math.max)
  |  assert(colCount == 1, "Enforced schema has more than one field with the same name!")
  |
  |  val typeMap:Map[String,DataType] = schema.map(x =>
    (x.name.

      toLowerCase, x.dataType)).toMap
  |  df.columns.
    foldLeft(df){ (z,col) => z.withColumn(col, z(col).cast(typeMap(col.toLowerCase))) }
  |}
|
|
|/** Returns DataFrame with updated rows
  * | *
  * | * DataFrames cannot be defined from others using a Spark SQL UPDATE statement.  This function allows
  * | * the caller to supply an UPDATE-like statement and the DataFrame to be updated and will merge the updated rows
  * | * returned by the query with those not selected for update.
  * | *
  * | * @param spark = SparkSession object
  * | * @param df = DataFrame from which new, updated rows will be generated from
  * | * @param dfViewName =  the view created for the df
  * | * @param query =  String which will query the passed df to generate new rows that can be unioned with the old, unmodified rows
  * | */
|def updateDataFrame(spark:
                     SparkSession, df: DataFrame, dfViewName: String, query: String): DataFrame = {
  |
  |  // Ensure query contains the substring "{SOURCE_DF}"
  |  val queryDFKeyword = "{SOURCE_DF}"
  |  assert(query.contains(queryDFKeyword), s
    "Updating query does not contain ${queryDFKeyword}!")
  |
  | // Add a unique row identifier to the DataFrame being updated and create a corresponding view
  |  val rowIdColName = "tmpRowId"
  |  val
  oldDFWithIds = df.withColumn(rowIdColName,
      monotonically_increasing_id())
  |
  | // Create view atop the source DataFrame with a rowid field
  |  val oldViewName =
    "oldDFWithIds_VIEW"
  |  oldDFWithIds.createOrReplaceTempView(oldViewName)
  |
  |
  // Inject the view with rowids into the supplied query
  |  val viewAdjustedQuery = query.replaceAllLiterally(
    queryDFKeyword, oldViewName)
  |
  |
  // Get updated rows
  |  val newRowsWithTMPCols = spark.sql(viewAdjustedQuery)
  |
  |  // Find new "_TMP" columns
  |  val tmpRegEx = "\\_TMP$".r
  |  val tmpCols = newRowsWithTMPCols.columns.filter(_.endsWith("_TMP"))
  |  assert(tmpCols.
    length > 0, s"No new columns defined in the update to ${dfViewName}!")
  |
  |
  // Rename the TMP columns in the updated rows to their proper names
  |  var
  newRowsWithRenamedTMPCols = tmpCols.foldLeft(newRowsWithTMPCols) {
    |    (z, col) => {
      |      val cleanedCol = tmpRegEx.replaceAllIn(col, "")
      |      z.withColumn(cleanedCol, z(col)).drop(col)
      |    }
    |  }
  |
  |  val checkExtraCols = newRowsWithRenamedTMPCols.columns.map(_.toLowerCase).toSet -- oldDFWithIds.columns.map(_.
    toLowerCase).toSet
  |
  |  if (!checkExtraCols.isEmpty) {
    |        newRowsWithRenamedTMPCols = newRowsWithRenamedTMPCols.drop(checkExtraCols.toList: _*)
    |  }
  |
  |  // Enforce the old schema on the new df
  |  val updatedNullabilityRows =
    enforceSchemaNullability(spark, newRowsWithRenamedTMPCols,
      oldDFWithIds.schema)
  |  val newDFWithIds = enforceSchemaTypes(
    updatedNullabilityRows, oldDFWithIds.schema)
  |
  |  val newViewName = "newDFWithIds_VIEW"
  |  newDFWithIds.
  createOrReplaceTempView(newViewName)
  |
  |newDFWithIds.persist
  |  valupdatedCount = newDFWithIds.count
  |  println(s"UPDATED ROWS Count: $updatedCount")
  |
  |  val cols = oldDFWithIds.columns
  |              .filterNot(_ == rowIdColName)
  |              .map(x => s"CASE WHEN B.${rowIdColName} IS NOT NULL THEN B.${x} ELSE A.$
  {x} END AS ${x}")
  |              .mkString(",")
  |
  |
  val joinquery = s
    "SELECT ${cols} FROM ${oldViewName} A LEFT JOIN ${newViewName} B ON A.${rowIdColName} = B.${
    rowIdColName}"
  |
  |  val
  finalDF = spark.sql(joinquery).repartition(50)
  |  finalDF.persist
  |  val totalCount = finalDF.count
  |  println(s"TOTAL ROWS Count: $totalCount ")
  |
  |  newDFWithIds.unpersist
  |  finalDF
    |}
|
|
|
|val sql = spark.sql _ |val parsedDate = new SimpleDateFormat("yyyy-MM-dd")
|val parsedDateInt = new SimpleDateFormat("yyyyMMdd")
|
|val cal = Calendar.getInstance()
|var currentDate = parsedDate.format(cal.getTime)
|cal.add(Calendar.DATE, -2)
|var startDate = parsedDate.format(cal.getTime)
| var startDateInt = parsedDateInt.format(cal.getTime)
|
|val endDate = "9999-12-31"
|
|val twoYearsBack = Calendar.getInstance()
|twoYearsBack.set(twoYearsBack.get(Calendar.YEAR) - 2, 0, 1)
|val rollingTwoYears = parsedDate.format(twoYearsBack.getTime)
|
|val sixMonthsBackCal = Calendar.getInstance()
|sixMonthsBackCal.setTime(cal.getTime)
|sixMonthsBackCal.add(Calendar.MONTH, -6)
|val sixMonthsBack = parsedDateInt.format(sixMonthsBackCal.getTime)
|
|
|val sqlReservation = s"SELECT bookingcategoryid, departuredate, reservationpaymentstatustypeid, traveleremailid, brandid, reservationid, createdate, inquiryserviceentryguid, arrivaldate, cancelleddate, activeflag, dwupdatedatetime, devicecategorysessionid, siteid, fullvisitorid, bookingdate, visitorid, websitereferralmediumsessionid, reservationavailabilitystatustypeid, regionid, listingunitid, visitid, bookingreporteddate, onlinebookingprovidertypeid FROM dw.dbo.reservation"
|
|val dfReservation = getEDWData(sqlReservation, 40)
|
|dfReservation.persist
|
|dfReservation.createOrReplaceTempView("tmpReservation")
|
|
| val sqlQuoteitem = s"SELECT quoteitemid, name, quoteitemtype, taxamount FROM dw.dbo.quoteitem with (NOLOCK)"
|
|val dfQuoteitem = getEDWData(sqlQuoteitem, 40)
|
|dfQuoteitem.persist
|
|dfQuoteitem.createOrReplaceTempView("tmpQuoteitem")
|
|
|val sqlQuote = s"SELECT reservationid, quoteguid, currencycode, quoteid, bookingtypeid, calculatedtaxamount, paymentmethodtypeid, taxamount, dwlastupdatedate FROM dw.dbo.quote with (NOLOCK)"
|
|val dfQuote = getEDWData(sqlQuote, 40)
|
|dfQuote.createOrReplaceTempView("tmpQuote")
|
|
|val sqlQuotefact = s"SELECT paidamountlocalcurrency, dwcreatedate, reservationid, currencycode, displayregionid, refundedamountlocalcurrency, quoteid, quotestatusid, quoteitemactiveflag, quotecreateddate, quoteitemid, quoteitemcreateddate, amountlocalcurrency, fullvisitorid, websitereferralmediumsessionid, visitid, dwlastupdatedate, travelerproductid, quoteactiveflag FROM dw.dbo.quotefact with (NOLOCK)"
|
|val dfQuotefact = getEDWData(sqlQuotefact, 40)
|
|dfQuotefact.persist
|
|dfQuotefact.createOrReplaceTempView("tmpQuotefact")
|
|
|val sqlTravelerproduct = s"SELECT travelerproductid, quoteitemtype, refquoteitemdescription FROM dw.dbo.travelerproduct with (NOLOCK)"
|
|val dfTravelerproduct = getEDWData(sqlTravelerproduct)
|
|dfTravelerproduct.persist
|
|dfTravelerproduct.createOrReplaceTempView("tmpTravelerproduct")
|
|
|val sqlTravelerorderpayment = s"SELECT travelerorderpaymenttypename, travelerorderpaymentid, travelerorderpaymentstatusname, paymentmethodtypeid, travelerorderid, currencycode, paymentdate, amount FROM dw_traveler.dbo.travelerorderpayment with (NOLOCK)"
|
|val dfTravelerorderpayment = getEDWData(sqlTravelerorderpayment)
|
|dfTravelerorderpayment.persist
|
| dfTravelerorderpayment.createOrReplaceTempView("tmpTravelerorderpayment")
|
|
|val sqlInquiry = s"SELECT inquiryserviceentryguid, listingid, inquiryid FROM dw.dbo.inquiry with (NOLOCK)"
|
|val dfInquiry = getEDWData(sqlInquiry, 40)
|
|dfInquiry.createOrReplaceTempView("tmpInquiry")
|
|
|val sqlTravelerorderpaymentdistributiontype = s"SELECT travelerorderpaymentdistributiontypeid, travelerorderpaymentdistributiontypename FROM dw_traveler.dbo.travelerorderpaymentdistributiontype with (NOLOCK)"
|
|val dfTravelerorderpaymentdistributiontype = getData(sqlTravelerorderpaymentdistributiontype)
|
|dfTravelerorderpaymentdistributiontype.persist
|
|dfTravelerorderpaymentdistributiontype.createOrReplaceTempView("tmpTravelerorderpaymentdistributiontype")
|
|
|val sqlPaymentmethodtype = s"SELECT paymentcollectiontype, paymentmethodtypeid FROM dw_traveler.dbo.paymentmethodtype with (NOLOCK)"
|
|val dfPaymentmethodtype = getEDWData(sqlPaymentmethodtype)
|
|dfPaymentmethodtype.persist
|
|dfPaymentmethodtype.createOrReplaceTempView("tmpPaymentmethodtype")
| |
|val sqlInquiryslafact = s"SELECT inquiryid, listingid, fullvisitorid, websitereferralmediumsessionid, visitid, inquirydate FROM dw.dbo.inquiryslafact with (NOLOCK)"
|
|val dfInquiryslafact = getEDWData(sqlInquiryslafact, 40)
|
|dfInquiryslafact.persist
|
|dfInquiryslafact.createOrReplaceTempView("tmpInquiryslafact")
|
|
|val sqlCurrency = s"SELECT currencyid, currencycode FROM dw.dbo.currency with (NOLOCK)"
|
|val dfCurrency = getEDWData(sqlCurrency)
|
|dfCurrency.persist
|
|dfCurrency.createOrReplaceTempView("tmpCurrency")
|
|
|val sqlWebsitereferralmediumsession = s"SELECT marketingmedium, websitereferralmediumsessionid, websitereferralmedium FROM dw.dbo.websitereferralmediumsession with (NOLOCK)"
|
|val dfWebsitereferralmediumsession = getEDWData(sqlWebsitereferralmediumsession)
|
|dfWebsitereferralmediumsession.persist
|
|dfWebsitereferralmediumsession.createOrReplaceTempView("tmpWebsitereferralmediumsession")
|
|
|val sqlCustomerattributes = s"SELECT customerattributesid, rowstartdate, customerid, persontype, rowenddate FROM dw.dbo.customerattributes with (NOLOCK)"
|
|val dfCustomerattributes = getEDWData(sqlCustomerattributes)
|
|dfCustomerattributes.persist
|
|dfCustomerattributes.createOrReplaceTempView("tmpCustomerattributes")
|
|
|val sqlListingunitattributes = s"SELECT listingunitattributesid, listingunitid, rowstartdate, rowenddate FROM dw.dbo.listingunitattributes with (NOLOCK)"
|
|val dfListingunitattributes = getEDWData(sqlListingunitattributes)
|
|dfListingunitattributes.persist
|
|dfListingunitattributes.createOrReplaceTempView("tmpListingunitattributes")
|
|
|val sqlProduct = s"SELECT productid, productguid, grossbookingvalueproductcategory FROM dw_traveler.dbo.product with (NOLOCK)"
|
|val dfProduct = getData(sqlProduct)
|
|dfProduct.persist
|
|dfProduct.createOrReplaceTempView("tmpProduct")
|
|
|val sqlOnlinebookingprovidertype = s"SELECT onlinebookingprovidertypeid, olbprovidergatewaytypedescription, listingsourcename FROM dw.dbo.onlinebookingprovidertype with (NOLOCK)"
|
|val dfOnlinebookingprovidertype = getEDWData(sqlOnlinebookingprovidertype)
|
|dfOnlinebookingprovidertype.persist
|
|dfOnlinebookingprovidertype.createOrReplaceTempView("tmpOnlinebookingprovidertype")
|
|
|val sqlPersontype = s"SELECT persontypeid, persontypename FROM dw.dbo.persontype with (NOLOCK)"
|
|val dfPersontype = getData(sqlPersontype)
|
|dfPersontype.persist
|
|dfPersontype.createOrReplaceTempView("tmpPersontype")
|
|
|val sqlListingunit = s"SELECT customerid, listingid, regionid, listingunitid, paymenttypeid FROM dw.dbo.listingunit with (NOLOCK)"
|
|val dfListingunit = getEDWData(sqlListingunit)
|
|dfListingunit.persist
|
|dfListingunit.createOrReplaceTempView("tmpListingunit")
|
|
|val sqlTravelerorderpaymentdistributionamounttype = s"SELECT travelerorderpaymentdistributionamounttypeid, travelerorderpaymentdistributionamounttypename FROM dw_traveler.dbo.travelerorderpaymentdistributionamounttype with (NOLOCK)"
|
|val dfTravelerorderpaymentdistributionamounttype = getEDWData(sqlTravelerorderpaymentdistributionamounttype)
|
| dfTravelerorderpaymentdistributionamounttype.persist
|
|dfTravelerorderpaymentdistributionamounttype.createOrReplaceTempView("tmpTravelerorderpaymentdistributionamounttype")
|
|
|val sqlChannel = s"SELECT channelid, channelname, listingsource FROM dw.dbo.channel with (NOLOCK)"
|
|val dfChannel = getEDWData(sqlChannel)
|
|dfChannel.persist
|
|dfChannel.createOrReplaceTempView("tmpChannel")
|
|
|val sqlTravelerorderitem = s"SELECT travelerorderid, currencycode, productid, amount, travelerorderitemstatus, travelerorderitemid FROM dw_traveler.dbo.travelerorderitem with (NOLOCK)"
|
|val dfTravelerorderitem = getEDWData(sqlTravelerorderitem)
|
|dfTravelerorderitem.persist
|
|dfTravelerorderitem.createOrReplaceTempView("tmpTravelerorderitem") |
|
|val sqlProductfulfillment = s"SELECT ExternalRefId, ExternalRefType, ProductTypeGuid FROM DW_Traveler.dbo.ProductFulfillment with (NOLOCK)"
|
|val dfProductfulfillment =
  getEDWData(
    sqlProductfulfillment)
|
|
  dfProductfulfillment.
createOrReplaceTempView(
  "tmpProductfulfillment")
|
|
|val
sqlVw_quotegrossbookingvalue =
  |
"""
       |    |SELECT qf.ReservationId ,
       |    |       qf.CurrencyCode ,
       |    |       qf.QuoteId ,
       |    |       qf.QuoteCreatedDate ,
       |    |       qf.QuoteStatusId ,
       |    |       q.PaymentMethodTypeId ,
       |    |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType NOT IN ('COMMISSION', 'OFFLINE_RDD', 'DEPOSIT', 'CSA_PDP', 'MANUAL_PDP', 'Product', 'PROTECTION', 'Stay_Tax_Fee', 'TAX', 'Traveler Fee') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS RentalAmount,
       |    |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType IN ('DEPOSIT', 'OFFLINE_RDD') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS RefundableDamageDepositAmount,
       |    |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType = 'Traveler Fee' THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS ServiceFeeAmount,
       |    |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType IN ('Stay_Tax_Fee', 'TAX') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) + coalesce(q.CalculatedTaxAmount,0) AS TaxAmount,
       |    |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType IN ('PROTECTION','Product', 'MANUAL_PDP', 'CSA_PDP') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS VasAmount,
       |    |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType = 'Commission' THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS CommissionAmount,
       |    |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType NOT IN ('Commission') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) + coalesce(q.CalculatedTaxAmount, 0) AS OrderAmount,
       |    |       SUM(qf.PaidAmountLocalCurrency) AS PaidAmount,
       |    |       SUM(qf.RefundedAmountLocalCurrency) AS RefundAmount,
       |    |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType NOT IN ('DEPOSIT', 'OFFLINE_RDD', 'Commission') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) + coalesce(q.CalculatedTaxAmount, 0) AS BookingValue
       |    |FROM tmpQuote q
       |    |JOIN tmpQuoteFact qf ON qf.QuoteId = q.QuoteId
       |    |JOIN (
       |    |       SELECT qi2.QuoteItemId ,
       |    |              CASE WHEN qi2.Name IN ('Booking Commission', 'IPM Booking Commission', 'PPS IPM Booking Commission') THEN 'COMMISSION' WHEN (tp.refQuoteItemDescription IN ('Service Fee', 'Traveler Fee')) THEN 'Traveler Fee' ELSE qi2.QuoteItemType END AS AdjustedQuoteItemType
       |    |       FROM tmpQuoteItem qi2
       |    |       JOIN tmpquotefact qf ON qi2.QuoteItemId = qf.QuoteItemId
       |    |       JOIN tmpTravelerProduct tp ON qf.TravelerProductId=tp.TravelerProductId
       |    |     ) qi ON qi.QuoteItemId = qf.QuoteItemId
       |    |WHERE 1 = 1 AND qf.QuoteActiveFlag = 1 AND qf.AmountLocalCurrency < 999999999999 and qf.QuoteCreatedDate >='2013-01-01' /* these are TEST quotes that skew the data */
       |    |GROUP BY qf.ReservationId ,
       |    | qf.CurrencyCode ,
       |    | qf.QuoteId ,
       |    | qf.QuoteCreatedDate ,
       |    | qf.QuoteStatusId ,
       |    | q.PaymentMethodTypeId ,
       |    | q.CalculatedTaxAmount
       |  """.stripMargin
|
|val dfVw_quotegrossbookingvalue = sql(sqlVw_quotegrossbookingvalue)
|
|dfVw_quotegrossbookingvalue.persist
|
|dfVw_quotegrossbookingvalue.createOrReplaceTempView("tmpVw_quotegrossbookingvalue")
|
|
|val sqlTravelerorderpaymentdistribution = s"SELECT travelerorderpaymentid, travelerorderitemid, updatedate, distributedamount, travelerorderpaymentdistributiontypeid, travelerorderpaymentdistributiontoaccounttypeid, travelerorderpaymentdistributionamounttypeid FROM dw_traveler.dbo.travelerorderpaymentdistribution with (NOLOCK)"
|
| val dfTravelerorderpaymentdistribution = getEDWData(sqlTravelerorderpaymentdistribution)
|
| dfTravelerorderpaymentdistribution.persist
|
|dfTravelerorderpaymentdistribution.createOrReplaceTempView("tmpTravelerorderpaymentdistribution")
|val sqlBookingvaluesource = s"SELECT * FROM dw.dbo.bookingvaluesource with (NOLOCK)"
|
|val dfBookingvaluesource = getEDWData(sqlBookingvaluesource)
|
|dfBookingvaluesource.persist
|
|dfBookingvaluesource.createOrReplaceTempView("tmpBookingvaluesource")
|
|
| /*
 |The below table dw_traveler.dbo.visitorfact is Partitioned on dateid column(s) in SqlServer.
 |Hence declaring WHERE clause to reduce load.
 |PLEASE ADJUST THE WHERE CLAUSE ACCORDING TO YOUR NEEDS.
 |*/
|val sqlVisitorfact = s"SELECT DISTINCT dateid, source, visitdate, fullvisitorid, visitstarttime, visitid FROM bq_exporter.visitorfact WHERE env='prod' and pdateid >= $sixMonthsBack"
|
|val dfVisitorfact = sql(sqlVisitorfact).repartition(100)
|
|dfVisitorfact.persist
|
|dfVisitorfact.createOrReplaceTempView("tmpVisitorfact")
|
|
| val sqlBrand = s"SELECT reportingcurcode, brandid FROM dw.dbo.brand with (NOLOCK) "
|
|val dfBrand = getEDWData(sqlBrand)
|
|dfBrand.persist
|
| dfBrand.createOrReplaceTempView("tmpBrand")
|
|
|val sqlTravelerorder = s"SELECT reservationid, travelerorderid, quoteid FROM dw_traveler.dbo.travelerorder with (NOLOCK)"
|
|val dfTravelerorder = getEDWData(sqlTravelerorder)
|
|dfTravelerorder.persist |
|dfTravelerorder.createOrReplaceTempView("tmpTravelerorder")
|
|
|val sqlAverageknownnightlybookingvalue = s"SELECT * FROM dw.dbo.averageknownnightlybookingvalue with (NOLOCK)"
|
|val dfAverageknownnightlybookingvalue = getEDWData(sqlAverageknownnightlybookingvalue)
|
|dfAverageknownnightlybookingvalue.persist
|
|dfAverageknownnightlybookingvalue.createOrReplaceTempView("tmpAverageknownnightlybookingvalue")
|
|
|val sqlTravelerorderpaymentdistributiontoaccounttype = s"SELECT travelerorderpaymentdistributiontoaccounttypeid, travelerorderpaymentdistributiontoaccounttypename FROM dw_traveler.dbo.travelerorderpaymentdistributiontoaccounttype with (NOLOCK)"
|
|val dfTravelerorderpaymentdistributiontoaccounttype = getEDWData(sqlTravelerorderpaymentdistributiontoaccounttype)
|
| dfTravelerorderpaymentdistributiontoaccounttype.persist
|
| dfTravelerorderpaymentdistributiontoaccounttype.createOrReplaceTempView("tmpTravelerorderpaymentdistributiontoaccounttype")
|
|
|val sqlBrandattributes = s"SELECT brandattributesid, brandid, rowstartdate, rowenddate FROM dw.dbo.brandattributes with (NOLOCK)"
|
|val dfBrandattributes = getEDWData(sqlBrandattributes)
|
|dfBrandattributes.persist |
|dfBrandattributes.createOrReplaceTempView("tmpBrandattributes")
|
|
|val sqlCurrencyconversion = s"SELECT tocurrency, rowstartdate, fromcurrency, rowenddate, currencyconversionid, conversionrate FROM dw.dbo.currencyconversion with (NOLOCK)"
|
| val dfCurrencyconversion = getEDWData(sqlCurrencyconversion)
|
|dfCurrencyconversion.persist
|
|dfCurrencyconversion.createOrReplaceTempView("tmpCurrencyconversion")
|
|
|val sqlBookingCategory = s"SELECT bookingcategoryid, bookingindicator, knownbookingindicator FROM dw.dbo.bookingcategory with (NOLOCK)"
|
|val dfBookingCategory = getEDWData(sqlBookingCategory)
|
|dfBookingCategory.persist
|
|dfBookingCategory.createOrReplaceTempView("tmpBookingCategory")
|
|
|val sqlListingattributes = s"SELECT listingattributesid, listingid, rowstartdate, rowenddate FROM dw.dbo.listingattributes with (NOLOCK)"
|
|val dfListingattributes = getEDWData(sqlListingattributes, 40)
|
|dfListingattributes.createOrReplaceTempView("tmpListingattributes")
|
|
| /*
 |The below table dw_facts.dbo.bookingfact is Partitioned on reservationcreatedateid column(s) in SqlServer.
 |Hence declaring WHERE clause to reduce load.
 |PLEASE ADJUST THE WHERE CLAUSE ACCORDING TO YOUR NEEDS.
 |*/
|val sqlBookingfact = s"SELECT * FROM dw_facts.dbo.bookingfact with (NOLOCK)" |
|val dfBookingfact = getEDWData(sqlBookingfact, 40)
| |dfBookingfact.persist
|
|dfBookingfact.createOrReplaceTempView("tmpBookingfact")
|
|
|val sqlCalendar = s"SELECT quarterenddate, yearnumber, dateid FROM dw.dbo.calendar with (NOLOCK)"
|
|val dfCalendar = getData(sqlCalendar)
|
| dfCalendar.persist
|
|dfCalendar.createOrReplaceTempView("tmpCalendar")
|
|
|val sqlListing = s"SELECT city, strategicdestinationid, country, listingid, postalcode, subscriptionid, paymenttypeid, bedroomnum FROM dw.dbo.listing with (NOLOCK)"
|
|val dfListing = getEDWData(sqlListing) |
|dfListing.persist
|
|dfListing.createOrReplaceTempView("tmpListing")
|
|
|val sqlDestinationattributes = s"SELECT destinationattributesid, destinationid, rowstartdate, rowenddate FROM dw.dbo.destinationattributes with (NOLOCK)"
|
|val dfDestinationattributes = getEDWData(sqlDestinationattributes)
|
| dfDestinationattributes.persist
|
| dfDestinationattributes.createOrReplaceTempView("tmpDestinationattributes") |val sqlSubscription = s"SELECT subscriptionid, paymenttypeid, listingid, subscriptionstartdate, subscriptionenddate  FROM dw.dbo.subscription with (NOLOCK)"
|
|val dfSubscription = getEDWData(sqlSubscription)
|
|
  dfSubscription.persist
|
|
  dfSubscription.createOrReplaceTempView("tmpSubscription")
|
|
|val sqlSite =
  s
    "SELECT bookingchannelid, siteid  FROM dw.dbo.site with (NOLOCK)"
|
|
val dfSite = getEDWData(
  sqlSite)
|
|dfSite.persist
|
|dfSite
.createOrReplaceTempView("tmpSite")
|
|
|var query =
  |          s
"""
                |             |select
                |             |            distinct qf.reservationid AS reservationid
                |             |        from
                |             |            tmpQuotefact qf
                |             |        where
                |             |            1 = 1
                |             |            and CAST(qf.quotecreateddate AS DATE) >= '$rollingTwoYears
'
                |             |            and qf.quoteitemactiveflag = 1
                |             |            and qf.quoteactiveflag = 1
                |             |            and qf.quotestatusid >= -1
                |             |            and (
                |             |                CAST(qf.dwcreatedate AS DATE) between '$startDate' and '$endDate
'
                |             |                or CAST(dwlastupdatedate AS DATE) between '$startDate' and '$endDate
'
                |             |            )
                |             |            and exists (
                |             |                select
                |             |                    1
                |             |                from
                |             |                    tmpReservation r
                |             |                where
                |             |                    r.activeflag = 1
                |             |                    and CAST(r.createdate AS DATE) >= '$rollingTwoYears
'
                |             |                    and r.bookingcategoryid != 1
                |             |                    and qf.reservationid = r.reservationid
                |             |            )
                |          """.stripMargin
|
|val
dfResList1 = sql(query)
|dfResList1.
createOrReplaceTempView("tmpDfResList1")
|
|
|query =
  |          s
"""
                |             |select
                |             |            r.reservationid AS reservationid
                |             |        from
                |             |            tmpReservation r
                |             |        where
                |             |            CAST(r.createdate AS DATE) >= '$rollingTwoYears'
                |             |            and r.activeflag = 1
                |             |            and r.bookingcategoryid != 1
                |             |            and CAST(r.dwupdatedatetime AS DATE) between '$startDate' and '$
endDate
'
                |             |            and not exists (
                |             |                select
                |             |                    1
                |             |                from
                |             |                    tmpDfResList1 r2
                |             |                where
                |             |                    r2.reservationid = r.reservationid
                |             |            )
                |          """.stripMargin
|
|
val dfResList2 = sql(query)
|
  dfResList2.createOrReplaceTempView("tmpDfResList2")
|
|
|query =
  |          s
"""
                |            |SELECT reservationid from tmpDfResList1
                |            |UNION ALL
                |            |SELECT reservationid from tmpDfResList2
                |
   """.stripMargin
|
|val cmb_tmpDfResList1 = sql(query)
|cmb_tmpDfResList1.
createOrReplaceTempView("cmb_tmpDfResList1")
|
|query =
  |          s
"""
                |            |select
                |            |            distinct tto.reservationid AS reservationid
                |            |        from
                |            |            tmpTravelerorder tto
                |            |        inner join
                |            |            tmpTravelerorderitem toi
                |            |                on tto.travelerorderid = toi.travelerorderid
                |            |        inner join
                |            |            tmpTravelerorderpayment topp
                |            |                on tto.travelerorderid = topp.travelerorderid
                |            |        inner join
                |            |            tmpTravelerorderpaymentdistribution topd
                |            |                on topp.travelerorderpaymentid = topd.travelerorderpaymentid
                |            |                and topd.travelerorderitemid = topd.travelerorderitemid
                |            |        where
                |            |            topp.travelerorderpaymenttypename = 'PAYMENT'
                |            |            and topp.travelerorderpaymentstatusname in (
                |            |                'PAID' , 'SETTLED' , 'REMITTED'
                |            |            )
                |            |            and CAST(topd.updatedate AS DATE) between '$ startDate' and '$endDate
'
                |            |            and exists (
                |            |                select
                |            |                    1
                |            |                from
                |            |                    tmpReservation r
                |            |                where
                |            |                    r.reservationid = tto.reservationid
                |            |                    and r.activeflag = 1
                |            |                    and r.bookingcategoryid != 1
                |            |                    and CAST(r.createdate AS DATE) >= '$
   rollingTwoYears
'
                |            |            )
                |            |            and not exists (
                |            |                select
                |            |                    1
                |            |                from
                |            |                    cmb_tmpDfResList1 r2
                |            |                where
                |            |                    r2.reservationid = tto.reservationid
                |            |            )
                |          """.stripMargin
|
|val dfResList3 = sql(query)
|dfResList3.createOrReplaceTempView("tmpDfResList3")
|
|query =
  |          s
"""
                |            |SELECT reservationid from cmb_tmpDfResList1
                |            |UNION ALL
                |            |SELECT reservationid from tmpDfResList3
                |          """.stripMargin
|
|val
cmb_tmpDfResList2 = sql(query)
|
  cmb_tmpDfResList2.createOrReplaceTempView("cmb_tmpDfResList2")
|
|
|query =
  |          s
"""
                |            |select
                |            |        r.arrivaldate ,
                |            |        r.createdate  ,
                |            |        r.bookingcategoryid ,
                |            |        r.bookingdate AS bookingdateid,
                |            |        r.bookingreporteddate AS bookingreporteddateid,
                |            |        r.cancelleddate AS cancelleddateid,
                |            |        r.brandid ,
                |            |        r.listingunitid ,
                |            |        r.reservationid ,
                |            |        r.siteid ,
                |            |        r.traveleremailid ,
                |            |        coalesce ( r.visitorid , 0 ) AS visitorid,
                |            |        r.onlinebookingprovidertypeid ,
                |            |        r.reservationavailabilitystatustypeid AS reservationavailabilitystatustypeid,
                |            |        r.reservationpaymentstatustypeid  AS reservationpaymentstatustypeid,
                |            |        r.devicecategorysessionid AS devicecategorysessionid,
                |            |        r.inquiryserviceentryguid ,
                |            |        0 AS inquiryid ,
                |            |        lu.customerid ,
                |            |        lu.listingid ,
                |            |        b.reportingcurcode as brandcurrencycode ,
                |            |        lu.regionid ,
                |            |        case
                |            |            when r.arrivaldate = r.departuredate then 1
                |            |            else datediff (r.departuredate, r.arrivaldate)
                |            |        end AS bookednightscount,
                |            |        0 AS brandattributesid,
                |            |        0 AS customerattributesid,
                |            |        0 AS listingattributesid ,
                |            |        -1 AS listingunitattributesid,
                |            |        -1 AS subscriptionid,
                |            |        -1 AS paymenttypeid,
                |            |        -1 AS listingchannelid,
                |            |        -1 AS persontypeid,
                |            |        1 AS bookingcount,
                |            |        -1 AS bookingchannelid,
                |            |        0 AS strategicdestinationid,
                |            |        0 AS strategicdestinationattributesid,
                |            |        r.visitid ,
                |            |        r.fullvisitorid ,
                |            |        r.websitereferralmediumsessionid
                |            |    from
                |            |        tmpReservation r
                |            |    inner join
                |            |        tmpListingunit lu
                |            |            on r.listingunitid = lu.listingunitid
                |            |    inner join
                |            |        tmpBrand b
                |            |            on r.brandid = b.brandid
                |            |    inner join
                |            |        cmb_tmpDfResList2 rl
                |            |            on r.reservationid = rl.reservationid
                |          """.stripMargin
|
|
|var dfRes1 = sql(query)
|
|val
dfResNullableCols = Set(
    "listingchannelid")
|dfRes1 = spark.
  createDataFrame(dfRes1.rdd, StructType(dfRes1.schema.
    map(x => if (dfResNullableCols.contains(x.name)) x.copy
    (nullable = true) else x)))
|
|dfRes1.persist
|dfRes1.createOrReplaceTempView(
  "dfRes1")
|
|
|//
  Getting Two Records Extra in the below JOIN -- duplicates on the joining columns -- Solution: Kept RANK as of now to circumvent the issue
|query =
  |          s
"""
                |            |select
                |            |       r.*, i.inquiryid  as inquiryid_TMP
                |            |    from
                |            |        {SOURCE_DF} r
                |            |    inner join
                |            |        (select * from (select *, row_number() OVER (PARTITION BY inquiryserviceentryguid, listingid ORDER BY inquiryserviceentryguid, listingid) AS rank FROM tmpInquiry) tmp WHERE rank = 1) i
                |            |            on lower(r.inquiryserviceentryguid) = lower(i.inquiryserviceentryguid)
                |            |            and r.listingid = i.listingid
                |          """.stripMargin
|
|
|var dfRes2 = updateDataFrame(spark,
  dfRes1, "dfRes1", query)
|
  dfRes2 = createCheckPoint(dfRes2)
|dfRes1.unpersist
|dfRes2.createOrReplaceTempView("dfRes2")
|
|
|query =
  |          s
"""
                |            |select
                |            |        r.*, ch.channelid  as listingchannelid_TMP
                |            |    from
                |            |        {SOURCE_DF} r
                |            |    inner join
                |            |        tmpOnlinebookingprovidertype obpt
                |            |            on r.onlinebookingprovidertypeid = obpt.onlinebookingprovidertypeid
                |            |    left join
                |            |        tmpChannel ch
                |            |            on case
                |            |                when obpt.olbprovidergatewaytypedescription like 'Clear Stay%' then 'clearstay'
                |            |                else obpt.listingsourcename
                |            |            end = ch.channelname
                |            |            and obpt.listingsourcename = ch.listingsource
                |          """.stripMargin
|
|var dfRes3 = updateDataFrame(spark, dfRes2, "dfRes2",
  query)
|dfRes3 = createCheckPoint(dfRes3)
|dfRes2.unpersist
|dfRes3.createOrReplaceTempView("dfRes3")
|
|query =
  |          s"""
                |            |    select
                |            |        r.*, la.listingattributesid  as listingattributesid_TMP
                |            |    from
                |            |        {SOURCE_DF} r
                |            |    inner join
                |            |        tmpListingattributes la
                |            |            on r.listingid = la.listingid
                |            |            and r.createdate between la.rowstartdate and la.rowenddate
                |            |
                |          """.stripMargin
|
|var dfRes4 = updateDataFrame(spark, dfRes3, "dfRes3", query)
|dfRes4 = createCheckPoint(
  dfRes4)
|
  dfRes3.unpersist
|dfRes4.
createOrReplaceTempView(
  "dfRes4")
|
|
|// Getting Two Records Extra in the below JOIN -- duplicates
  on the joining columns -- Solution: Kept RANK as of now to circumvent the
  issue
|query =
  |  s
"""
        |    |select * from
        |    |(
        |    |select
        |    |        r.*,
        |    |        row_number() OVER (PARTITION BY reservationid ORDER BY r.bookingdateid desc) as rank,
        |    |        s.subscriptionid  as subscriptionid_TMP,
        |    |        s.paymenttypeid  as paymenttypeid_TMP
        |    |    from
        |    |        {SOURCE_DF} r
        |    |    inner join
        |    |         tmpSubscription s
        |    |            on r.listingid = s.listingid
        |    |    where
        |    |        r.bookingdateid between s.subscriptionstartdate and s.subscriptionenddate
        |    |) a where rank =1
        |  """.stripMargin
|
|var dfRes5 = updateDataFrame(spark, dfRes4, "dfRes4", query)
|
  dfRes5 = createCheckPoint(
  dfRes5)
|dfRes4.unpersist
|dfRes5.
createOrReplaceTempView(
  "dfRes5")
|
|
|query =
  |          s
"""
                |            |select
                |            |        r.*, l.strategicdestinationid  as strategicdestinationid_TMP
                |            |    from
                |            |        {SOURCE_DF} r
                |            |    inner join
                |            |        tmpListing l
                |            |            on r.listingid = l.listingid
                |          """.stripMargin
|
|var dfRes6 = updateDataFrame(spark, dfRes5,
  "dfRes5", query)
|dfRes6 =
  createCheckPoint(dfRes6)
|dfRes5.
unpersist
|dfRes6.
createOrReplaceTempView("dfRes6")
|
|
|
  query =
  |          s
"""
                |            |select
                |            |        r.*, da.destinationattributesid  as strategicdestinationattributesid_TMP
                |            |    from
                |            |        {SOURCE_DF} r
                |            |    inner join
                |            |        tmpDestinationattributes da
                |            |            on r.strategicdestinationid = da.destinationid
                |            |            and r.createdate between da.rowstartdate and da.rowenddate
                |          """.
  stripMargin
|
|var dfRes7 = updateDataFrame(spark, dfRes6,
  "dfRes6", query)
|dfRes7 = createCheckPoint(
  dfRes7)
|dfRes6.unpersist
|dfRes7.
createOrReplaceTempView("dfRes7")
|
|
|
  query =
  |          s
"""
                |            |select
                |            |        r.*, ba.brandattributesid  as brandattributesid_TMP
                |            |    from
                |            |        {SOURCE_DF} r
                |            |    inner join
                |            |        tmpBrandattributes ba
                |            |            on r.brandid = ba.brandid
                |            |            and r.createdate between ba.rowstartdate and ba.rowenddate
                |          """.
  stripMargin
|
|var dfRes8 =
  updateDataFrame(spark, dfRes7, "dfRes7", query)
|dfRes8 = createCheckPoint(
  dfRes8)
| dfRes7.unpersist
|dfRes8.createOrReplaceTempView("dfRes8")
|
|
|query =
  |          s
"""
                |            |select
                |            |        r.*,
                |            |        ca.customerattributesid  as customerattributesid_TMP,
                |            |        coalesce ( pt.persontypeid, -1 ) as persontypeid_TMP
                |            |    from
                |            |        {SOURCE_DF} r
                |            |    inner join
                |            |        tmpCustomerattributes ca
                |            |            on r.customerid = ca.customerid
                |            |            and r.createdate between ca.rowstartdate and ca.rowenddate
                |            |    left join
                |            |        tmpPersontype pt
                |            |            on coalesce ( ca.persontype ,'Unknown' ) = pt.persontypename
                |          """.
  stripMargin
|
|var dfRes9 =
  updateDataFrame(spark, dfRes8,
    "dfRes8", query)
|dfRes9 =
  createCheckPoint(dfRes9)
|dfRes8.unpersist
|
  dfRes9.createOrReplaceTempView("dfRes9"
)
|
|
|query =
  |          s
"""
                |            |select
                |            |        r.*, lua.listingunitattributesid  as listingunitattributesid_TMP
                |            |    from
                |            |        {SOURCE_DF} r
                |            |    inner join
                |            |        tmpListingunitattributes lua
                |            |            on r.listingunitid = lua.listingunitid
                |            |            and r.createdate between lua.rowstartdate and lua.rowenddate
                |          """.stripMargin
|
|var dfRes10 = updateDataFrame(spark, dfRes9,
  "dfRes9", query)
|dfRes10 = createCheckPoint(dfRes10)
|dfRes9.
unpersist
|dfRes10.
createOrReplaceTempView("dfRes10")
|
|
|query =
  |          s"""
                |            |select
                |            |        r.*, s.bookingchannelid  as bookingchannelid_TMP
                |            |    from
                |            |       {SOURCE_DF} r
                |            |    inner join
                |            |        tmpSite s
                |            |            on s.siteid = r.siteid
                |            |    where
                |            |        s.bookingchannelid > -1
                |          """.stripMargin
|
|var dfRes = updateDataFrame(spark, dfRes10,
  "dfRes10",
  query)
|dfRes =
  createCheckPoint(dfRes)
|dfRes10.unpersist
|dfRes.createOrReplaceTempView("dfRes")
|
|
|query =
  |          s
"""
                |            |select
                |            |        r.reservationid ,
                |            |        gbv.currencycode ,
                |            |        case when gbv.quotestatusid = -1 then 'Quote Unk' else 'Quote' end AS valuesource,
                |            |        gbv.rentalamount ,
                |            |        gbv.refundabledamagedepositamount ,
                |            |        gbv.servicefeeamount ,
                |            |        gbv.taxamount ,
                |            |        gbv.vasamount ,
                |            |        gbv.orderamount ,
                |            |        gbv.paidamount ,
                |            |        gbv.refundamount ,
                |            |        gbv.bookingvalue ,
                |            |        gbv.commissionamount ,
                |            |        gbv.paymentmethodtypeid ,
                |            |        row_number() over (partition by gbv.reservationid order by case when gbv.quotestatusid = -1 then 'Quote Unk' else 'Quote' end, gbv.quoteid) AS rownum
                |            |    from
                |            |        tmpVw_quotegrossbookingvalue gbv
                |            |    inner join
                |            |        dfRes r
                |            |            on gbv.reservationid = r.reservationid
                |          """.stripMargin
|
|var
dfQgbv = sql(query)
|
  dfQgbv.persist
|dfQgbv = createCheckPoint(
  dfQgbv)
|dfQgbv.createOrReplaceTempView("dfQgbv")
|
|
|
|
  query =
  |          s
"""
                |            |select
                |            |        gbv.reservationid ,
                |            |        gbv.currencycode ,
                |            |        gbv.rentalamount ,
                |            |        gbv.refundabledamagedepositamount ,
                |            |        gbv.servicefeeamount ,
                |            |        gbv.taxamount ,
                |            |        gbv.vasamount ,
                |            |        gbv.orderamount ,
                |            |        gbv.paidamount ,
                |            |        gbv.refundamount ,
                |            |        gbv.bookingvalue ,
                |            |        gbv.valuesource ,
                |            |        -1 AS displayregionid,
                |            |        gbv.commissionamount ,
                |            |        0 AS paymentmethodtypeid,
                |            |        0 AS paidamountoffline,
                |            |        0 AS paidamountonline
                |            |    from
                |            |        (select
                |            |        reservationid ,
                |            |        currencycode ,
                |            |        valuesource ,
                |            |        sum ( rentalamount ) AS rentalamount,
                |            |        sum ( refundabledamagedepositamount ) AS refundabledamagedepositamount,
                |            |        sum ( servicefeeamount ) AS servicefeeamount,
                |            |        sum ( taxamount ) AS taxamount,
                |            |        sum ( vasamount ) AS vasamount,
                |            |        sum ( orderamount ) AS orderamount,
                |            |        sum ( paidamount ) AS paidamount,
                |            |        sum ( refundamount ) AS refundamount,
                |            |        sum ( bookingvalue ) AS bookingvalue,
                |            |        sum ( commissionamount ) AS commissionamount
                |            |    from
                |            |        dfQgbv
                |            |    group by
                |            |        reservationid ,
                |            |        currencycode ,
                |            |        valuesource ) AS gbv
                |          """.stripMargin
|
|var dfGbv1 = sql(query)
|dfGbv1 = createCheckPoint(dfGbv1)
|dfGbv1.createOrReplaceTempView("dfGbv1")
|
|
|query =
  |  s
"""
        |    |SELECT * FROM (
        |    |    SELECT *, row_number() OVER (PARTITION BY ReservationId ORDER BY ValueSource ASC) rank FROM dfGbv1
        |    | ) tmp WHERE rank = 1
        |  """.stripMargin
|
|var dfGbv2
= sql(query).drop("rank")
|
  dfGbv2 = createCheckPoint(dfGbv2)
|
  dfGbv2.createOrReplaceTempView("dfGbv2")
|
|
|query =
  |          s
"""
                |            |select
                |            |        gbv.*, x.paymentmethodtypeid  as paymentmethodtypeid_TMP
                |            |    from
                |            |        {SOURCE_DF} gbv
                |            |    inner join
                |            |        dfQgbv x
                |            |            on x.reservationid = gbv.reservationid
                |            |            and x.valuesource = gbv.valuesource
                |            |    where
                |            |        x.rownum = 1
                |            |        and x.paymentmethodtypeid > 0
                |          """.stripMargin
|
|var dfGbv3 = updateDataFrame(spark, dfGbv2,
  "dfGbv2", query)
|dfGbv3 = createCheckPoint(dfGbv3)
|dfGbv3.createOrReplaceTempView("dfGbv3")
|
|query =
  |          s"""
                |            |select
                |            |    tot.reservationid ,
                |            |    g.currencycode ,
                |            |    case
                |            |        when g.currencycode != topp.currencycode and cc.conversionrate is not null then topp.amount * cc.conversionrate
                |            |        else topp.amount
                |            |    end AS amount,
                |            |    pmt.paymentcollectiontype
                |            |from
                |            |    tmpTravelerorderpayment topp
                |            |inner join
                |            |    tmpTravelerorder tot
                |            |        on topp.travelerorderid = tot.travelerorderid
                |            |inner join
                |            |    tmpPaymentmethodtype pmt
                |            |        on pmt.paymentmethodtypeid = topp.paymentmethodtypeid
                |            |inner join
                |            |    dfGbv3 g
                |            |        on tot.reservationid = g.reservationid
                |            |left join
                |            |    tmpCurrencyconversion cc
                |            |        on g.currencycode = cc.tocurrency
                |            |        and topp.currencycode = cc.fromcurrency
                |            |        and topp.paymentdate between cc.rowstartdate and cc.rowenddate
                |            |where
                |            |    topp.amount <> 0
                |            |    and tot.quoteid > 0
                |            |    and topp.travelerorderpaymenttypename in ( 'PAYMENT' )
                |            |    and topp.travelerorderpaymentstatusname in (  'PAID', 'SETTLED', 'REMITTED' )
                |          """.stripMargin
|
|val pivotDfGbv1 = sql(query)
|val pivotDfGbv2 = pivotDfGbv1.groupBy("reservationid", "currencycode").pivot("paymentcollectiontype", Array(
  "ONLINE",
  "OFFLINE", "UNKNOWN")).agg
(sum(round($"amount",3)))
|pivotDfGbv2.
createOrReplaceTempView("pivotDfGbv2")
|
|query =
  |  s
"""
        |    |SELECT
        |    |        p.ReservationId
        |    |       ,p.CurrencyCode
        |    |       ,COALESCE(p.ONLINE, 0) AS PaidAmountOnline
        |    |       ,COALESCE(p.OFFLINE, 0) + COALESCE(p.UNKNOWN, 0) AS PaidAmountOffline
        |    |FROM pivotDfGbv2 p
        |  """.stripMargin
|
|var GBV2 = sql(query)
|GBV2.persist
|GBV2 =
  createCheckPoint(GBV2)
|GBV2.
createOrReplaceTempView("GBV2")
|
|GBV2.
count
|
|query =
  |
"""
               |            |select
               |            |        g.*, r.displayregionid AS displayregionid_TMP
               |            |    from
               |            |        {SOURCE_DF} g
               |            |    inner join
               |            |        ( select
               |            |        r.reservationid ,
               |            |        max ( q.displayregionid ) as displayregionid
               |            |    from
               |            |        tmpQuotefact q
               |            |    inner join
               |            |        dfRes r
               |            |            on q.reservationid = r.reservationid
               |            |    group by
               |            |        r.reservationid ) r
               |            |            on g.reservationid = r.reservationid
               |          """.stripMargin
|
|var dfGbv4 = updateDataFrame(spark, dfGbv3, "dfGbv3",
  query)
|dfGbv3.unpersist
|dfGbv4 = createCheckPoint(dfGbv4
)
|dfGbv4.createOrReplaceTempView(
  "dfGbv4")
|
|query =
  |
"""
               |            |select
               |            |        tto.reservationid ,
               |            |        toi.currencycode ,
               |            |        sum ( topd.distributedamount ) AS servicefeeamount,
               |            |        sum ( case when topd.travelerorderpaymentdistributiontoaccounttypeid = 2 then topd.distributedamount else 0 end ) AS servicefeevatamount,
               |            |        sum ( case when topd.travelerorderpaymentdistributiontypeid in (10, 5, 4, 2, 1) then topd.distributedamount else 0 end ) AS servicefeeprocessingfeeamount
               |            |    from
               |            |        tmpTravelerorder tto
               |            |    inner join
               |            |        tmpTravelerorderitem toi
               |            |            on tto.travelerorderid = toi.travelerorderid
               |            |    inner join
               |            |        tmpProduct p
               |            |            on toi.productid = p.productid
               |            |    inner join
               |            |        tmpTravelerorderpayment topp
               |            |            on tto.travelerorderid = topp.travelerorderid
               |            |    inner join
               |            |        tmpTravelerorderpaymentdistribution topd
               |            |            on topp.travelerorderpaymentid = topd.travelerorderpaymentid
               |            |            and toi.travelerorderitemid = topd.travelerorderitemid
               |            |    inner join
               |            |        dfGbv4 gbv
               |            |            on tto.reservationid = gbv.reservationid
               |            |    where
               |            |        p.grossbookingvalueproductcategory = 'TRAVELER_FEE'
               |            |        and topp.travelerorderpaymenttypename = 'PAYMENT'
               |            |        and topp.travelerorderpaymentstatusname in ('PAID', 'SETTLED', 'REMITTED')
               |            |    group by
               |            |        tto.reservationid, toi.currencycode
               |          """.stripMargin
|
|var dfSf = sql(
  query)
|dfSf.persist
|dfSf =
  createCheckPoint(dfSf)
|dfSf.createOrReplaceTempView("dfSf"
)
|
|query =
  |
"""
               |            |select
               |            |        gbv.*, 'Quote and TravelerOrder'  as valuesource_TMP,
               |            |        case when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount else gbv.servicefeeamount end as servicefeeamount_TMP,
               |            |        gbv.taxamount + sf.servicefeevatamount as taxamount_TMP, gbv.orderamount - gbv.servicefeeamount - gbv.taxamount + case when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount else gbv.servicefeeamount end + gbv.taxamount + sf.servicefeevatamount as orderamount_TMP,
               |            |        gbv.bookingvalue - gbv.servicefeeamount - gbv.taxamount + case when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount else gbv.servicefeeamount end + gbv.taxamount + sf.servicefeevatamount as bookingvalue_TMP
               |            |    from
               |            |        {SOURCE_DF} gbv
               |            |    inner join
               |            |        dfSf sf
               |            |            on sf.reservationid = gbv.reservationid
               |            |            and sf.currencycode = gbv.currencycode
               |            |    where
               |            |        gbv.servicefeeamount != case
               |            |            when gbv.servicefeeamount = 0 then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount
               |            |            else gbv.servicefeeamount
               |            |        end
               |            |        or sf.servicefeevatamount != 0
               |            |
               |          """.stripMargin
|
|var dfGbv5 = updateDataFrame(spark,
  dfGbv4, "dfGbv4", query)
|dfGbv5 = createCheckPoint(dfGbv5)
|dfGbv5.
createOrReplaceTempView(
  "dfGbv5")
|
|query =
  |
"""
               |            |select
               |            |        r.reservationid ,
               |            |        r.listingunitid ,
               |            |        r.regionid ,
               |            |        r.arrivaldate ,
               |            |        cal.quarterenddate as arrivalquarterenddate ,
               |            |        cal.yearnumber as arrivalyearnumber ,
               |            |        r.brandcurrencycode ,
               |            |        r.brandid ,
               |            |        r.bookednightscount ,
               |            |        l.bedroomnum ,
               |            |        l.postalcode ,
               |            |        l.city ,
               |            |        l.country ,
               |            |        calb.quarterenddate AS bookingquarterenddate,
               |            |        calb.yearnumber AS bookingyearnumber,
               |            |        'USD' AS currencycode,
               |            |        cast (0 as decimal(18, 6)) AS bookingvalue,
               |            |        0 AS step
               |            |    from
               |            |        dfRes r
               |            |    inner join
               |            |        tmpBookingCategory bc
               |            |            on r.bookingcategoryid = bc.bookingcategoryid
               |            |    inner join
               |            |        tmpListing l
               |            |            on r.listingid = l.listingid
               |            |    inner join
               |            |        tmpCalendar cal
               |            |            on r.arrivaldate = cal.dateid
               |            |    inner join
               |            |        tmpCalendar calb
               |            |            on r.bookingdateid = calb.dateid
               |            |    where
               |            |        bc.bookingindicator = 1
               |            |        and bc.knownbookingindicator = 0
               |            |        and not exists (
               |            |            select
               |            |                g.reservationid
               |            |            from
               |            |                dfGbv5 g
               |            |            where
               |            |                r.reservationid = g.reservationid
               |            |        )
               |          """.
  stripMargin
|
|var dfEst1 = sql(query)
|dfEst1 =
  createCheckPoint(dfEst1)
|dfEst1.createOrReplaceTempView
("dfEst1")
|
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        1  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 1
               |            |            and f.listingunitid = x.listingunitid
               |            |            and f.arrivalquarterenddate = x.arrivalquarterenddate
               |          """.stripMargin
|
|var dfEst2 =
  updateDataFrame(spark, dfEst1, "dfEst1", query)
|dfEst2 =
  createCheckPoint(dfEst2)
|dfEst2.createOrReplaceTempView("dfEst2")
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        2  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 2
               |            |            and f.listingunitid = x.listingunitid
               |            |            and f.arrivalyearnumber = x.arrivalyearnumber
               |            |    where
               |            |        f.step=0
               |          """.stripMargin
|
|var dfEst3 = updateDataFrame
(spark, dfEst2, "dfEst2", query)
|dfEst3 =
  createCheckPoint(dfEst3)
|dfEst3.
createOrReplaceTempView("dfEst3")
|
  dfEst2.unpersist
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        3  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 3
               |            |            and f.listingunitid = x.listingunitid
               |            |    where
               |            |        f.step=0
               |          """.
  stripMargin
|
|var dfEst4 = updateDataFrame(spark, dfEst3, "dfEst3", query)
|dfEst4 = createCheckPoint(dfEst4)
|dfEst4.createOrReplaceTempView("dfEst4")
|dfEst3.unpersist
|
|
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        4  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 4
               |            |            and f.brandid = x.brandid
               |            |            and f.bedroomnum = x.bedroomnum
               |            |            and f.postalcode = x.postalcode
               |            |            and f.bookingquarterenddate = x.bookingquarterenddate
               |            |    where
               |            |        f.step = 0
               |          """.stripMargin
|
|var dfEst5 =
  updateDataFrame(spark, dfEst4, "dfEst4", query)
|
  dfEst5 = createCheckPoint(dfEst5)
|
  dfEst5.createOrReplaceTempView(
  "dfEst5")
|dfEst4.unpersist
|
|
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        5  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 5
               |            |            and f.brandid = x.brandid
               |            |            and f.bedroomnum = x.bedroomnum
               |            |            and f.postalcode = x.postalcode
               |            |            and f.arrivalyearnumber = x.arrivalyearnumber
               |            |    where
               |            |        f.step = 0
               |          """.stripMargin
|
|var dfEst6 = updateDataFrame(spark, dfEst5, "dfEst5", query)
|dfEst6 = createCheckPoint(dfEst6)
|dfEst6.createOrReplaceTempView("dfEst6")
|dfEst5.unpersist
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        6  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 6
               |            |            and f.brandid = x.brandid
               |            |            and f.bedroomnum = x.bedroomnum
               |            |            and f.postalcode = x.postalcode
               |            |    where
               |            |        f.step = 0
               |          """.stripMargin
|
|var dfEst7 =
  updateDataFrame(spark, dfEst6, "dfEst6", query)
|dfEst7 = createCheckPoint(dfEst7)
|dfEst7
.createOrReplaceTempView("dfEst7")
|dfEst6.unpersist
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        7  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 7
               |            |            and f.brandid = x.brandid
               |            |            and f.bedroomnum = x.bedroomnum
               |            |            and f.city = x.city
               |            |            and f.country = x.country
               |            |            and f.bookingquarterenddate = x.bookingquarterenddate
               |            |    where
               |            |        f.step = 0
               |          """.
  stripMargin
|
|var dfEst8 = updateDataFrame(spark, dfEst7, "dfEst7", query)
|dfEst8 =
  createCheckPoint(dfEst8)
|dfEst8.createOrReplaceTempView("dfEst8")
|dfEst7.unpersist
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        8  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 8
               |            |            and f.brandid = x.brandid
               |            |            and f.bedroomnum = x.bedroomnum
               |            |            and f.city = x.city
               |            |            and f.country = x.country
               |            |            and f.arrivalyearnumber = x.arrivalyearnumber
               |            |    where
               |            |        f.step = 0
               |          """.
  stripMargin
|
|var dfEst9 = updateDataFrame(spark, dfEst8, "dfEst8", query)
|dfEst9 =
  createCheckPoint(dfEst9)
|dfEst9.
createOrReplaceTempView("dfEst9")
|dfEst8.
unpersist
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round(x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        9  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 9
               |            |            and f.brandid = x.brandid
               |            |            and f.bedroomnum = x.bedroomnum
               |            |            and f.city = x.city
               |            |            and f.country = x.country
               |            |    where
               |            |        f.step = 0
               |          """.stripMargin
|
|var dfEst10 = updateDataFrame(spark, dfEst9, "dfEst9", query)
|dfEst10 = createCheckPoint(dfEst10)
|dfEst10.
createOrReplaceTempView("dfEst10")
|dfEst9.unpersist |
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round(x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        estimationstepnum  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 10
               |            |            and f.brandid = x.brandid
               |            |            and f.bedroomnum = x.bedroomnum
               |            |    where
               |            |        f.step = 0
               |          """.stripMargin
|
|var
dfEst11 = updateDataFrame(spark, dfEst10,
    "dfEst10", query)
|dfEst11 = createCheckPoint(dfEst11)
|
  dfEst11.createOrReplaceTempView("dfEst11")
|dfEst10.
unpersist
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round(x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        estimationstepnum  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 11
               |            |            and f.bedroomnum = x.bedroomnum
               |            |    where
               |            |        f.step = 0
               |          """.stripMargin
|
|var dfEst12 = updateDataFrame(spark,
  dfEst11, "dfEst11", query)
|dfEst12 =
  createCheckPoint(dfEst12)
|dfEst12.createOrReplaceTempView("dfEst12")
|
  dfEst11.unpersist
|
|query =
  |
"""
               |            |select
               |            |        f.*,
               |            |        round(x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
               |            |        estimationstepnum  as step_TMP
               |            |    from
               |            |        {SOURCE_DF} f
               |            |    inner join
               |            |        tmpAverageknownnightlybookingvalue x
               |            |            on x.estimationstepnum = 12
               |            |            and f.brandid = x.brandid
               |            |    where
               |            |        f.step = 0
               |          """.stripMargin
|
|var dfEst =
  updateDataFrame(spark, dfEst12, "dfEst12", query)
|dfEst = createCheckPoint(dfEst)
|dfEst.
createOrReplaceTempView("dfEst")
|dfEst12.unpersist
|
|
|query =
  |
"""
               |            |SELECT
               |            |       ReservationId
               |            |       ,CurrencyCode
               |            |       ,BookingValue AS RentalAmount
               |            |       ,0 AS RefundableDamageDepositAmount
               |            |       ,0 AS ServiceFeeAmount
               |            |       ,0 AS TaxAmount
               |            |       ,0 AS VasAmount
               |            |       ,BookingValue AS OrderAmount
               |            |       ,0 AS PaidAmount
               |            |       ,0 AS RefundAmount
               |            |       ,BookingValue
               |            |       ,CONCAT('Estimate Step ', Step) AS ValueSource
               |            |       ,-1 AS displayregionid
               |            |       ,0 AS CommissionAmount
               |            |       ,0 AS PaymentMethodTypeId
               |            |       ,0 AS PaidAmountOffline
               |            |       ,0 AS PaidAmountOnline
               |            |   FROM
               |            |       dfEst
               |          """.stripMargin
|
|var
dfGbv6 = sql(query)
|dfGbv6.createOrReplaceTempView("dfGbv6")
|
|query =
  |
"""
       |    |SELECT ReservationId, CurrencyCode, RentalAmount, RefundableDamageDepositAmount, ServiceFeeAmount, TaxAmount, VasAmount, OrderAmount, PaidAmount, RefundAmount, BookingValue, ValueSource, displayregionid, CommissionAmount, PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline FROM dfGbv5
       |    |UNION ALL
       |    |SELECT ReservationId, CurrencyCode, RentalAmount, RefundableDamageDepositAmount, ServiceFeeAmount, TaxAmount, VasAmount, OrderAmount, PaidAmount, RefundAmount, BookingValue, ValueSource, displayregionid, CommissionAmount, PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline FROM dfGbv6
       |  """.stripMargin
|
|var dfGbv = sql(query)
|dfGbv.
persist
|dfGbv = createCheckPoint(dfGbv)
|dfGbv.createOrReplaceTempView("dfGbv")
|
|
|query =
  |
"""
               |            |SELECT r.*,
               |            |       COALESCE (s.bookingvaluesourceid, 1)          AS bookingvaluesourceid,
               |            |       COALESCE (g.displayregionid, -1)              AS displayregionid,
               |            |       COALESCE (g.rentalamount, 0)                  AS rentalamount,
               |            |       COALESCE (g.refundabledamagedepositamount, 0) AS refundabledamagedepositamount,
               |            |       COALESCE (g.servicefeeamount, 0)              AS servicefeeamount,
               |            |       COALESCE (g.taxamount, 0)                     AS taxamount,
               |            |       COALESCE (g.vasamount, 0)                     AS vasamount,
               |            |       COALESCE (g.orderamount, 0)                   AS orderamount,
               |            |       COALESCE (g.paidamount, 0)                    AS paidamount,
               |            |       COALESCE (g.refundamount, 0)                  AS refundamount,
               |            |       COALESCE (g.bookingvalue, 0)                  AS bookingvalue,
               |            |       COALESCE (c.currencyid, 154)                  AS localcurrencyid,
               |            |       COALESCE (ccusd.currencyconversionid, -1)     AS currencyconversionidusd,
               |            |       COALESCE (ccbc.currencyconversionid, -1)      AS currencyconversionidbrandcurrency,
               |            |       COALESCE (g.commissionamount, 0)              AS commissionamount,
               |            |       COALESCE (g.paymentmethodtypeid, 0)           AS paymentmethodtypeid,
               |            |       COALESCE (g2.paidamountonline, 0)             AS paidamountonline,
               |            |       COALESCE (g2.paidamountoffline, 0)            AS paidamountoffline,
               |            |       0                                             AS inquiryvisitid,
               |            |       '[Unknown]'                                   AS inquiryfullvisitorid,
               |            |       'undefined'                                   AS inquirymedium,
               |            |       -1                                            AS inquirywebsitereferralmediumsessionid,
               |            |       '[N/A]'                                       AS inquirysource,
               |            |       0                                             AS bookingrequestvisitid,
               |            |       '[Unknown]'                                   AS bookingrequestfullvisitorid,
               |            |       'Undefined'                                   AS bookingrequestmedium,
               |            |       -1                                            AS bookingrequestwebsitereferralmediumsessionid,
               |            |       '[N/A]'                                       AS bookingrequestsource,
               |            |       0                                             AS calculatedvisitid,
               |            |       '[Unknown]'                                   AS calculatedfullvisitorid,
               |            |       'Undefined'                                   AS calculatedbookingmedium,
               |            |       -1                                            AS calculatedbookingwebsitereferralmediumsessionid,
               |            |       '[N/A]'                                       AS calculatedbookingsource
               |            |FROM   dfRes r
               |            |       LEFT JOIN dfgbv g
               |            |              ON r.reservationid = g.reservationid
               |            |       LEFT JOIN GBV2 g2
               |            |              ON r.reservationid = g2.reservationid
               |            |       LEFT JOIN tmpbookingvaluesource s
               |            |              ON g.valuesource = s.bookingvaluesourcename
               |            |       LEFT JOIN tmpcurrency c
               |            |              ON g.currencycode = c.currencycode
               |            |       LEFT JOIN tmpcurrencyconversion ccusd
               |            |              ON g.currencycode = ccusd.fromcurrency
               |            |                 AND ccusd.tocurrency = 'USD'
               |            |                 AND r.createdate BETWEEN ccusd.rowstartdate AND
               |            |                                          ccusd.rowenddate
               |            |       LEFT JOIN tmpcurrencyconversion ccbc
               |            |              ON r.brandcurrencycode = ccbc.tocurrency
               |            |                 AND g.currencycode = ccbc.fromcurrency
               |            |                 AND r.createdate BETWEEN ccbc.rowstartdate AND ccbc.rowenddate
               |          """.stripMargin
|
|var dfFinal1 = sql(query)
|
  dfFinal1.persist
|dfFinal1 =
  createCheckPoint(dfFinal1)
|dfFinal1.
createOrReplaceTempView("dfFinal1")
|
|
|
|query =
  |
"""
               |            |SELECT tto.reservationid,
               |            |       toi.travelerorderid,
               |            |       r.brandcurrencycode,
               |            |       COALESCE (cc.conversionrate, 1)                    AS conversionrate,
               |            |       COALESCE (q.currencycode, toi.currencycode)        AS localcurrencycode,
               |            |       Sum (toi.amount * COALESCE (cc.conversionrate, 1)) AS standalonevasamt
               |            |FROM   tmptravelerorder tto
               |            |       INNER JOIN tmptravelerorderitem toi
               |            |               ON toi.travelerorderid = tto.travelerorderid
               |            |       INNER JOIN tmpproduct p
               |            |               ON toi.productid = p.productid
               |            |       INNER JOIN tmpquote q
               |            |               ON tto.quoteid = q.quoteid
               |            |       INNER JOIN tmpproductfulfillment pf
               |            |               ON pf.externalrefid = q.quoteguid
               |            |                  AND pf.externalreftype = 'quote'
               |            |                  AND pf.producttypeguid = p.productguid
               |            |       INNER JOIN dfRes r
               |            |               ON q.reservationid = r.reservationid
               |            |       LEFT JOIN tmpcurrencyconversion cc
               |            |              ON toi.currencycode = cc.fromcurrency
               |            |                 AND q.currencycode = cc.tocurrency
               |            |                 AND r.createdate BETWEEN cc.rowstartdate AND cc.rowenddate
               |            |WHERE  p.productguid NOT IN ('TRAVELER_PROTECTION', 'COMMISSION')
               |            |       AND p.grossbookingvalueproductcategory = 'VAS'
               |            |       AND toi.travelerorderitemstatus = 'PAID'
               |            |       AND NOT EXISTS (SELECT 1
               |            |                       FROM   (SELECT q.quoteid,
               |            |                                      q.quoteitemid,
               |            |                                      q.quoteitemcreateddate,
               |            |                                      tp.quoteitemtype,
               |            |                                      q.amountlocalcurrency,
               |            |                                      q.reservationid
               |            |                               FROM   tmpquotefact q
               |            |                                      INNER JOIN tmpTravelerproduct tp
               |            |                                              ON q.travelerproductid = tp.travelerproductid
               |            |                                      INNER JOIN dfRes r
               |            |                                              ON q.reservationid = r.reservationid
               |            |                               WHERE  q.quotecreateddate >= '2013-01-01'
               |            |                                      AND q.quoteactiveflag = 1
               |            |                                      AND q.quoteitemactiveflag = 1) qf
               |            |                       WHERE  tto.reservationid = qf.reservationid AND qf.quoteitemtype = p.productguid)
               |            |GROUP  BY tto.reservationid,
               |            |          toi.travelerorderid,
               |            |          COALESCE (cc.conversionrate, 1),
               |            |          COALESCE (q.currencycode, toi.currencycode),
               |            |          r.brandcurrencycode
               |          """.
  stripMargin
|
|var dfVas = sql(query)
|dfVas.persist
|
  dfVas = createCheckPoint(dfVas)
|dfVas.createOrReplaceTempView("dfVas")
|
|
|query =
  |
"""
               |            |SELECT v.reservationid,
               |            |       v.localcurrencycode,
               |            |       v.brandcurrencycode,
               |            |       Round (Sum (v.standalonevasamt), 6) AS standalonevasamt,
               |            |       COALESCE (Round (Sum (topp.paidamount * v.conversionrate), 6), 0) AS paidamount,
               |            |       COALESCE (Min (topp.paymentmethodtypeid), 0) AS paymentmethodtypeid,
               |            |       COALESCE (Round (Sum (topp.paidamountonline * v.conversionrate), 6), 0) AS paidamountonline,
               |            |       COALESCE (Round (Sum (topp.paidamountoffline * v.conversionrate), 6), 0) AS paidamountoffline
               |            |FROM   dfVas v
               |            |       LEFT JOIN (SELECT tp.travelerorderid,
               |            |                         Min (tp.paymentmethodtypeid) AS paymentmethodtypeid,
               |            |                         Sum (topd.distributedamount) AS paidamount,
               |            |                         Sum (CASE  WHEN pmt.paymentcollectiontype = 'online' THEN topd.distributedamount ELSE 0 END) AS paidamountonline,
               |            |                         Sum (CASE  WHEN pmt.paymentcollectiontype = 'offline' THEN topd.distributedamount ELSE 0 END) AS paidamountoffline
               |            |                             FROM tmpTravelerorderpayment tp
               |            |                             JOIN tmpTravelerorderpaymentdistribution topd
               |            |                             ON topd.travelerorderpaymentid = tp.travelerorderpaymentid
               |            |                             JOIN tmpTravelerorderpaymentdistributionamounttype at
               |            |                             ON topd.travelerorderpaymentdistributionamounttypeid = at.travelerorderpaymentdistributionamounttypeid
               |            |                             JOIN tmpTravelerorderpaymentdistributiontype topdt
               |            |                             ON topd.travelerorderpaymentdistributiontypeid = topdt.travelerorderpaymentdistributiontypeid
               |            |                             JOIN tmpTravelerorderpaymentdistributiontoaccounttype act
               |            |                             ON topd.travelerorderpaymentdistributiontoaccounttypeid = act.travelerorderpaymentdistributiontoaccounttypeid
               |            |                             JOIN tmpPaymentmethodtype pmt
               |            |                             ON pmt.paymentmethodtypeid = tp.paymentmethodtypeid
               |            |                             WHERE  at.travelerorderpaymentdistributionamounttypename = 'credit'
               |            |                             AND act.travelerorderpaymentdistributiontoaccounttypename = 'vendor'
               |            |                             AND tp.travelerorderpaymenttypename = 'payment'
               |            |                             AND tp.travelerorderpaymentstatusname IN ( 'settled', 'remitted', 'paid' )
               |            |                             AND topdt.travelerorderpaymentdistributiontypename = 'service'
               |            |                             GROUP  BY tp.travelerorderid
               |            |                             ) topp
               |            |ON v.travelerorderid = topp.travelerorderid
               |            |GROUP  BY v.reservationid, v.localcurrencycode, v.brandcurrencycode
               |          """.stripMargin
|
|var dfVas2 = sql(query)
|dfVas2.persist
|dfVas2 = createCheckPoint(dfVas2)
|dfVas2.createOrReplaceTempView("dfvas2")
|
|
|
|query =
  |
"""
               |            |select
               |            |        bf.*,
               |            |        bf.vasamount + standalonevasamt as vasamount_TMP,
               |            |        bf.orderamount + standalonevasamt as orderamount_TMP,
               |            |        bf.bookingvalue + standalonevasamt as bookingvalue_TMP,
               |            |        11  as bookingvaluesourceid_TMP,
               |            |        bf.paidamount + v.paidamount as baidamount_TMP
               |            |    from
               |            |        {SOURCE_DF} bf
               |            |    join
               |            |        tmpCurrency c
               |            |            on bf.localcurrencyid = c.currencyid
               |            |    join
               |            |        dfVas2 v
               |            |            on bf.reservationid = v.reservationid
               |            |            and c.currencycode = v.localcurrencycode
               |            |    join
               |            |        tmpBookingvaluesource bvs
               |            |            on bf.bookingvaluesourceid = bvs.bookingvaluesourceid
               |            |    where
               |            |        bvs.bookingvaluesourcetype = 'Order'
               |          """.stripMargin
|
|var dfFinal2 = updateDataFrame(spark, dfFinal1, "dfFinal1", query)
|
  dfFinal2 = createCheckPoint(
  dfFinal2)
|dfFinal2.createOrReplaceTempView("dfFinal2")
|dfFinal1.
unpersist
|
|query =
  |
"""
               |            |select
               |            |        bf.*,
               |            |        bf.vasamount + standalonevasamt as vasamount_TMP,
               |            |        bf.orderamount + standalonevasamt as orderamount_TMP,
               |            |        bf.bookingvalue + standalonevasamt as bookingvalue_TMP,
               |            |        10  as bookingvaluesourceid_TMP,
               |            |        vc.currencyid  as localcurrencyid_TMP,
               |            |        cc.currencyconversionid  as currencyconversionidusd_TMP,
               |            |        v.paymentmethodtypeid  as paymentmethodtypeid_TMP,
               |            |        bf.paidamount + v.paidamount as paidamount_TMP,
               |            |        bc.currencyconversionid  as currencyconversionidbrandcurrency_TMP,
               |            |        bf.paidamountonline + v.paidamountonline as paidamountonline_TMP,
               |            |        bf.paidamountoffline + v.paidamountoffline as paidamountoffline_TMP
               |            |    from
               |            |        {SOURCE_DF} bf
               |            |    inner join
               |            |        tmpCurrency c
               |            |            on bf.localcurrencyid = c.currencyid
               |            |    inner join
               |            |        dfVas2 v
               |            |            on bf.reservationid = v.reservationid
               |            |            and c.currencycode != v.localcurrencycode
               |            |    inner join
               |            |        tmpCurrency vc
               |            |            on v.localcurrencycode = vc.currencycode
               |            |    inner join
               |            |        tmpCurrencyconversion cc
               |            |            on vc.currencycode = cc.fromcurrency
               |            |            and cc.tocurrency = 'USD'
               |            |            and bf.bookingdateid between cc.rowstartdate and cc.rowenddate
               |            |    inner join
               |            |        tmpCurrencyconversion bc
               |            |            on v.brandcurrencycode = bc.tocurrency
               |            |            and v.localcurrencycode = bc.fromcurrency
               |            |            and bf.bookingdateid between bc.rowstartdate and bc.rowenddate
               |            |    where
               |            |        bf.bookingvaluesourceid = 1
               |          """.
  stripMargin
|
|var dfFinal3 =
  updateDataFrame(spark, dfFinal2, "dfFinal2",
    query)
|dfFinal3 = createCheckPoint(
  dfFinal3)
|dfFinal3.createOrReplaceTempView("dfFinal3")
|
  dfFinal2.unpersist
|
|
|
  query =
  |          s"""
                |             |select
                |             |            r.arrivaldate ,
                |             |            r.createdate ,
                |             |            r.bookingcategoryid ,
                |             |            null AS bookingdateid,
                |             |            r.bookingreporteddate AS bookingreporteddateid,
                |             |            null AS cancelleddateid,
                |             |            f.brandid ,
                |             |            f.listingunitid ,
                |             |            r.reservationid ,
                |             |            f.siteid ,
                |             |            f.traveleremailid ,
                |             |            f.visitorid ,
                |             |            f.onlinebookingprovidertypeid ,
                |             |            f.reservationavailabilitystatustypeid ,
                |             |            f.reservationpaymentstatustypeid ,
                |             |            f.devicecategorysessionid ,
                |             |            r.inquiryserviceentryguid ,
                |             |            f.inquiryid ,
                |             |            f.customerid ,
                |             |            f.listingid ,
                |             |            'UNK' AS brandcurrencycode,
                |             |            f.regionid ,
                |             |            0 AS bookednightscount,
                |             |            f.brandattributesid ,
                |             |            f.customerattributesid ,
                |             |            f.listingattributesid ,
                |             |            f.listingunitattributesid ,
                |             |            f.subscriptionid ,
                |             |            f.paymenttypeid ,
                |             |            f.listingchannelid ,
                |             |            f.persontypeid ,
                |             |            0 AS bookingcount,
                |             |            f.bookingvaluesourceid ,
                |             |            f.displayregionid ,
                |             |            0 AS rentalamount,
                |             |            0 AS refundabledamagedepositamount,
                |             |            0 AS servicefeeamount,
                |             |            0 AS taxamount,
                |             |            0 AS vasamount,
                |             |            0 AS orderamount,
                |             |            0 AS paidamount,
                |             |            0 AS refundamount,
                |             |            0 AS bookingvalue,
                |             |            f.localcurrencyid ,
                |             |            f.currencyconversionidusd ,
                |             |            f.currencyconversionidbrandcurrency ,
                |             |            0 AS commissionamount,
                |             |            f.bookingchannelid AS bookingchannelid,
                |             |            f.paymentmethodtypeid AS paymentmethodtypeid,
                |             |            0 AS paidamountoffline,
                |             |            0 AS paidamountonline,
                |             |            f.strategicdestinationid ,
                |             |            f.strategicdestinationattributesid ,
                |             |            f.visitid ,
                |             |            f.fullvisitorid ,
                |             |            f.websitereferralmediumsessionid ,
                |             |            f.inquiryvisitid ,
                |             |            f.inquiryfullvisitorid ,
                |             |            f.inquirymedium ,
                |             |            f.inquirywebsitereferralmediumsessionid ,
                |             |            f.inquirysource ,
                |             |            f.bookingrequestvisitid ,
                |             |            f.bookingrequestfullvisitorid ,
                |             |            f.bookingrequestmedium ,
                |             |            f.bookingrequestwebsitereferralmediumsessionid ,
                |             |            f.bookingrequestsource ,
                |             |            f.calculatedvisitid ,
                |             |            f.calculatedfullvisitorid ,
                |             |            f.calculatedbookingmedium ,
                |             |            f.calculatedbookingwebsitereferralmediumsessionid ,
                |             |            f.calculatedbookingsource
                |             |        from
                |             |            tmpReservation r
                |             |        inner join
                |             |            tmpBookingfact f
                |             |                on r.reservationid = f.reservationid
                |             |                and r.createdate = f.reservationcreatedateid
                |             |        where
                |             |            r.createdate >= '$rollingTwoYears
'
                |             |            and (
                |             |                r.activeflag = 0
                |             |                or (
                |             |                    r.activeflag = 1
                |             |                    and r.bookingcategoryid = 1
                |             |                )
                |             |            )
                |             |            and r.dwupdatedatetime between '$startDate' and '$
  endDate
'
                |             |            and f.bookingcount = 1
                |             |
                |           """.stripMargin
|
|var tmpFinal = sql(query)
|tmpFinal.persist
|
  tmpFinal = createCheckPoint(tmpFinal)
|tmpFinal.createOrReplaceTempView("tmpFinal")
|
|
|query =
  |
"""
       |    |SELECT ArrivalDate, CreateDate, BookingCategoryId, BookingDateId, BookingReportedDateId, CancelledDateId, BrandId, ListingUnitId, ReservationId, SiteId, TravelerEmailId, VisitorId, OnlineBookingProviderTypeId, ReservationAvailabilityStatusTypeId, ReservationPaymentStatusTypeId, DeviceCategorySessionId, InquiryServiceEntryGUID, InquiryId, CustomerId, ListingId, BrandCurrencyCode, RegionId, BookedNightsCount, BrandAttributesId, CustomerAttributesId, ListingAttributesId, ListingUnitAttributesId, Subscriptionid, PaymentTypeId, ListingChannelId, PersonTypeId, BookingCount, BookingValueSourceId, DisplayRegionId, RentalAmount, RefundableDamageDepositAmount, ServiceFeeAmount, TaxAmount, VasAmount, OrderAmount, PaidAmount, RefundAmount, BookingValue, LocalCurrencyId, CurrencyConversionIdUSD, CurrencyConversionIdBrandCurrency, CommissionAmount, BookingChannelId, PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline, StrategicDestinationId, StrategicDestinationAttributesId, VisitId, FullVisitorId, WebsiteReferralMediumSessionId, InquiryVisitId, InquiryFullVisitorId, InquiryMedium, InquiryWebsiteReferralMediumSessionId, InquirySource, BookingRequestVisitId, BookingRequestFullVisitorId, BookingRequestMedium, BookingRequestWebsiteReferralMediumSessionID, BookingRequestSource, CalculatedVisitId, CalculatedFullVisitorId, CalculatedBookingMedium, CalculatedBookingWebsiteReferralMediumSessionID, CalculatedBookingSource FROM tmpFinal
       |    |UNION ALL
       |    |SELECT ArrivalDate, CreateDate, BookingCategoryId, BookingDateId, BookingReportedDateId, CancelledDateId, BrandId, ListingUnitId, ReservationId, SiteId, TravelerEmailId, VisitorId, OnlineBookingProviderTypeId, ReservationAvailabilityStatusTypeId, ReservationPaymentStatusTypeId, DeviceCategorySessionId, InquiryServiceEntryGUID, InquiryId, CustomerId, ListingId, BrandCurrencyCode, RegionId, BookedNightsCount, BrandAttributesId, CustomerAttributesId, ListingAttributesId, ListingUnitAttributesId, Subscriptionid, PaymentTypeId, ListingChannelId, PersonTypeId, BookingCount, BookingValueSourceId, DisplayRegionId, RentalAmount, RefundableDamageDepositAmount, ServiceFeeAmount, TaxAmount, VasAmount, OrderAmount, PaidAmount, RefundAmount, BookingValue, LocalCurrencyId, CurrencyConversionIdUSD, CurrencyConversionIdBrandCurrency, CommissionAmount, BookingChannelId, PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline, StrategicDestinationId, StrategicDestinationAttributesId, VisitId, FullVisitorId, WebsiteReferralMediumSessionId, InquiryVisitId, InquiryFullVisitorId, InquiryMedium, InquiryWebsiteReferralMediumSessionId, InquirySource, BookingRequestVisitId, BookingRequestFullVisitorId, BookingRequestMedium, BookingRequestWebsiteReferralMediumSessionID, BookingRequestSource, CalculatedVisitId, CalculatedFullVisitorId, CalculatedBookingMedium, CalculatedBookingWebsiteReferralMediumSessionID, CalculatedBookingSource FROM dfFinal3
       |    """.stripMargin
|
|var dfFinal4 = sql(query)
|dfFinal4.persist
|dfFinal4 =
  createCheckPoint(dfFinal4)
|dfFinal3.unpersist
|
|query =
  |
"""
               |            |select
               |            |        bf.*,
               |            |        coalesce(i.visitid, 0) as inquiryvisitid_TMP,
               |            |        coalesce(i.fullvisitorid, '[Unknown]') as inquiryfullvisitorid_TMP,
               |            |        wrms.websitereferralmedium  as inquirymedium_TMP,
               |            |        wrms.websitereferralmediumsessionid  as inquirywebsitereferralmediumsessionid_TMP
               |            |    from
               |            |        {SOURCE_DF} bf
               |            |    inner join
               |            |        tmpInquiryslafact i
               |            |            on i.inquiryid = bf.inquiryid
               |            |    inner join
               |            |        tmpWebsitereferralmediumsession wrms
               |            |            on wrms.websitereferralmediumsessionid = i.websitereferralmediumsessionid
               |            |    where
               |            |        bf.inquirymedium <> wrms.websitereferralmedium
               |            |        or bf.inquirywebsitereferralmediumsessionid <> wrms.websitereferralmediumsessionid
               |          """.stripMargin
|
|var dfFinal5 = updateDataFrame(spark, dfFinal4, "dfFinal4", query)
|
  dfFinal5 = createCheckPoint(dfFinal5)
|dfFinal5.createOrReplaceTempView(
  "dfFinal5")
|dfFinal4.unpersist
|
|
|query =
  |
"""
               |            |select
               |            |        bf.*,
               |            |        vf.source AS inquirysource_TMP
               |            |    from
               |            |        {SOURCE_DF} bf
               |            |    inner join
               |            |        tmpInquiryslafact i
               |            |            on i.inquiryid = bf.inquiryid
               |            |    inner join
               |            |        tmpVisitorfact vf
               |            |            on vf.dateid = cast(from_unixtime(unix_timestamp(i.inquirydate, 'yyyy-MM-dd'), 'yyyyMMdd') AS int)
               |            |        and vf.visitid = i.visitid
               |            |        and vf.fullvisitorid = i.fullvisitorid
               |            |    where
               |            |        bf.inquirysource <> vf.source
               |            |
               |          """.stripMargin
|
|var dfFinal6 = updateDataFrame(spark,
  dfFinal5, "dfFinal5", query)
|dfFinal6 = createCheckPoint(dfFinal6)
|dfFinal6
.createOrReplaceTempView("dfFinal6")
|dfFinal5.unpersist
|
|query =
  |
"""
               |            |select
               |            |        fq.reservationid ,
               |            |        fq.quoteid
               |            |    from
               |            |        ( select
               |            |            qf.reservationid ,
               |            |            qf.quoteid ,
               |            |            row_number() over (partition by q.reservationid order by qf.quoteid) AS rownum
               |            |        from
               |            |            dfFinal6 bf
               |            |        inner join
               |            |            tmpQuotefact qf
               |            |                on qf.reservationid = bf.reservationid
               |            |        inner join
               |            |            tmpQuote q
               |            |                on qf.quoteid = q.quoteid
               |            |        inner join
               |            |            tmpReservation r
               |            |                on qf.reservationid = r.reservationid
               |            |        where
               |            |            qf.reservationid > -1
               |            |            and q.bookingtypeid in (2 , 3)
               |            |            and r.activeflag = 1 ) fq
               |            |    where
               |            |        fq.rownum = 1
               |          """.stripMargin
|
|var dfFirstQuote = sql(query)
|
  dfFirstQuote.persist
|dfFirstQuote = createCheckPoint(dfFirstQuote)
|dfFirstQuote.
createOrReplaceTempView("dfFirstQuote")
|
|query =
  |
"""
               |            |select
               |            |        ms.reservationid ,
               |            |        ms.quoteid ,
               |            |        ms.visitid ,
               |            |        ms.fullvisitorid ,
               |            |        ms.websitereferralmediumsessionid
               |            |   from
               |            |        ( select
               |            |            mq.reservationid ,
               |            |            qfa.quoteid ,
               |            |            qfa.visitid ,
               |            |            qfa.fullvisitorid ,
               |            |            qfa.websitereferralmediumsessionid ,
               |            |            row_number() over ( partition by mq.reservationid order by qfa.websitereferralmediumsessionid desc ) AS rownum
               |            |        from
               |            |            dfFirstquote mq
               |            |        inner join
               |            |            tmpQuotefact qfa
               |            |                on mq.quoteid = qfa.quoteid ) ms
               |            |    where
               |            |        ms.rownum = 1
               |          """.stripMargin
|
|var dfMaxSession = sql(query)
|dfMaxSession.persist
|dfMaxSession = createCheckPoint(
  dfMaxSession)
|dfMaxSession.
createOrReplaceTempView("dfMaxSession")
|
|
|query =
  |
"""
               |            |select
               |            |        *
               |            |    from
               |            |        ( select
               |            |            br.* ,
               |            |            vf.source ,
               |            |            vf.visitstarttime ,
               |            |            row_number() over (partition by br.reservationid order by vf.visitstarttime desc) AS rownum
               |            |        from
               |            |            dfMaxSession br
               |            |        inner join
               |            |            tmpQuotefact qf
               |            |                on qf.reservationid = br.reservationid
               |            |                and qf.quoteid = br.quoteid
               |            |                and qf.websitereferralmediumsessionid = br.websitereferralmediumsessionid
               |            |        left join
               |            |            tmpVisitorfact vf
               |            |                on qf.visitid = vf.visitid
               |            |                and qf.fullvisitorid = vf.fullvisitorid
               |            |                and qf.quotecreateddate = vf.visitdate ) fs
               |            |    where
               |            |        fs.rownum = 1
               |          """.stripMargin
|
|var dfSession = sql(query)
|dfSession.persist
|dfSession = createCheckPoint(dfSession)
|dfSession.createOrReplaceTempView("dfSession")
|
|
|query =
  |          """
               |            |select
               |            |        bf.*,
               |            |        coalesce ( s.visitid, 0 ) as bookingrequestvisitid_TMP,
               |            |        coalesce ( s.fullvisitorid, '[Unknown]' ) as bookingrequestfullvisitorid_TMP,
               |            |        wrms.websitereferralmedium  as bookingrequestmedium_TMP,
               |            |        wrms.websitereferralmediumsessionid  as bookingrequestwebsitereferralmediumsessionid_TMP,
               |            |        coalesce ( s.source, '[N/A]' ) as bookingrequestsource_TMP
               |            |    from
               |            |        {SOURCE_DF} bf
               |            |    inner join
               |            |        dfSession s
               |            |            on s.reservationid = bf.reservationid
               |            |    inner join
               |            |        tmpWebsitereferralmediumsession wrms
               |            |            on s.websitereferralmediumsessionid = wrms.websitereferralmediumsessionid
               |            |    where
               |            |        bf.bookingrequestwebsitereferralmediumsessionid = -1
               |          """.stripMargin
|
|var dfFinal7 = updateDataFrame(spark, dfFinal6, "dfFinal6", query)
|dfFinal7 = createCheckPoint(dfFinal7)
|dfFinal7.createOrReplaceTempView("dfFinal7")
|dfFinal6.unpersist
|
|query =
  |          """
               |            |select
               |            |        bf.reservationid ,
               |            |        bf.websitereferralmediumsessionid AS bookingwebsitereferralmediumsessionid,
               |            |        wrms.marketingmedium AS bookingmediumcase,
               |            |        coalesce ( vf.source,'[N/A]' ) AS bookingmediumsource
               |            |    from
               |            |        dfFinal7 bf
               |            |    inner join
               |            |        tmpWebsitereferralmediumsession wrms
               |            |            on wrms.websitereferralmediumsessionid = bf.websitereferralmediumsessionid
               |            |    left join
               |            |        tmpVisitorfact vf
               |            |            on bf.visitid = vf.visitid
               |            |            and bf.fullvisitorid = vf.fullvisitorid
               |            |            and bf.bookingdateid = vf.visitdate
               |          """.stripMargin
|
|var dfBookingMedium = sql(query)
|dfBookingMedium.persist
|dfBookingMedium = createCheckPoint(dfBookingMedium)
|dfBookingMedium.createOrReplaceTempView("dfBookingMedium")
|
|query =
  |          """
               |            |select
               |            |        bf.reservationid ,
               |            |        wrmsbr.marketingmedium AS bookingrequestmediumcase,
               |            |        wrmsi.marketingmedium AS inquirymediumcase
               |            |    from
               |            |        dfFinal7 bf
               |            |    inner join
               |            |        tmpWebsitereferralmediumsession wrmsbr
               |            |            on wrmsbr.websitereferralmediumsessionid = bf.bookingrequestwebsitereferralmediumsessionid
               |            |    inner join
               |            |        tmpWebsitereferralmediumsession wrmsi
               |            |            on wrmsi.websitereferralmediumsessionid = bf.inquirywebsitereferralmediumsessionid
               |            |
               |          """.stripMargin
|
|var dfOtherMediums = sql(query)
|dfOtherMediums.persist
|dfOtherMediums = createCheckPoint(dfOtherMediums)
|dfOtherMediums.createOrReplaceTempView("dfOtherMediums")
|
|
|query =
  |          """
               |            |select
               |            |        bf.*,
               |            |        bf.visitid  as calculatedvisitid_TMP,
               |            |        bf.fullvisitorid  as calculatedfullvisitorid_TMP,
               |            |        bm.bookingmediumcase  as calculatedbookingmedium_TMP,
               |            |        bm.bookingmediumsource  as calculatedbookingsource_TMP,
               |            |        bm.bookingwebsitereferralmediumsessionid  as calculatedbookingwebsitereferralmediumsessionid_TMP
               |            |    from
               |            |        {SOURCE_DF} bf
               |            |    inner join
               |            |        dfBookingMedium bm
               |            |            on bm.reservationid = bf.reservationid
               |          """.stripMargin
|
|var dfFinal8 = updateDataFrame(spark, dfFinal7, "dfFinal7", query)
|dfFinal8 = createCheckPoint(dfFinal8)
|dfFinal8.createOrReplaceTempView("dfFinal8")
|dfFinal7.unpersist
|
|
|query =
  |          """
               |            |select
               |            |        bf.*,
               |            |        bf.bookingrequestvisitid  as calculatedvisitid_TMP,
               |            |        bf.bookingrequestfullvisitorid  as calculatedfullvisitorid_TMP,
               |            |        om.bookingrequestmediumcase  as calculatedbookingmedium_TMP,
               |            |        bf.bookingrequestsource  as calculatedbookingsource_TMP,
               |            |        bf.bookingrequestwebsitereferralmediumsessionid  as calculatedbookingwebsitereferralmediumsessionid_TMP
               |            |    from
               |            |        {SOURCE_DF} bf
               |            |    inner join
               |            |        dfOtherMediums om
               |            |            on om.reservationid = bf.reservationid
               |            |    where
               |            |        bf.calculatedbookingmedium = 'Undefined'
               |            |        and om.bookingrequestmediumcase <> 'Undefined'
               |          """.stripMargin
|
|var dfFinal9 = updateDataFrame(spark, dfFinal8, "dfFinal8", query)
|dfFinal9 = createCheckPoint(dfFinal9)
|dfFinal9.createOrReplaceTempView("dfFinal9")
|dfFinal8.unpersist
|
|
|query =
  |          """
               |            |select
               |            |        bf.*,
               |            |        bf.inquiryvisitid  as calculatedvisitid_TMP,
               |            |        bf.inquiryfullvisitorid  as calculatedfullvisitorid_TMP,
               |            |        om.inquirymediumcase  as calculatedbookingmedium_TMP,
               |            |        bf.inquirysource  as calculatedbookingsource_TMP,
               |            |        bf.inquirywebsitereferralmediumsessionid  as calculatedbookingwebsitereferralmediumsessionid_TMP
               |            |    from
               |            |        {SOURCE_DF} bf
               |            |    inner join
               |            |        dfOtherMediums om
               |            |            on om.reservationid = bf.reservationid
               |            |    where
               |            |        bf.calculatedbookingmedium = 'Undefined'
               |            |        and om.inquirymediumcase <> 'Undefined'
               |          """.stripMargin
|
|var dfFinal = updateDataFrame(spark, dfFinal9, "dfFinal9", query)
|dfFinal = createCheckPoint(dfFinal)
|dfFinal.createOrReplaceTempView("dfFinal")
|dfFinal9.unpersist
|
|
|query =
  |          """
               |            |SELECT
               |            |COALESCE(fin.ReservationId, f.ReservationId) AS ReservationId,
               |            |COALESCE(fin.BookingDateId, f.BookingDateId) AS BookingDateId,
               |            |COALESCE(fin.BookingReportedDateId, f.BookingReportedDateId) AS BookingReportedDateId,
               |            |COALESCE(fin.CancelledDateId, f.CancelledDateId) AS CancelledDateId,
               |            |COALESCE(fin.CreateDate, f.ReservationCreateDateId) AS ReservationCreateDateId,
               |            |COALESCE(fin.BookingCategoryId, f.BookingCategoryId) AS BookingCategoryId,
               |            |COALESCE(fin.BookingValueSourceId, f.BookingValueSourceId) AS BookingValueSourceId,
               |            |COALESCE(fin.BrandAttributesId, f.BrandAttributesId) AS BrandAttributesId,
               |            |COALESCE(fin.BrandId, f.BrandId) AS BrandId,
               |            |COALESCE(fin.ListingChannelId, f.ListingChannelId) AS ListingChannelId,
               |            |COALESCE(fin.CurrencyConversionIdBrandCurrency, f.CurrencyConversionIdBrandCurrency) AS CurrencyConversionIdBrandCurrency,
               |            |COALESCE(fin.CurrencyConversionIdUSD, f.CurrencyConversionIdUSD) AS CurrencyConversionIdUSD,
               |            |COALESCE(fin.CustomerAttributesId, f.CustomerAttributesId) AS CustomerAttributesId,
               |            |COALESCE(fin.CustomerId, f.CustomerId) AS CustomerId,
               |            |COALESCE(fin.DeviceCategorySessionId, f.DeviceCategorySessionId) AS DeviceCategorySessionId,
               |            |COALESCE(fin.DisplayRegionId, f.DisplayRegionId) AS DisplayRegionId,
               |            |COALESCE(fin.InquiryId, f.InquiryId) AS InquiryId,
               |            |COALESCE(fin.ListingAttributesId, f.ListingAttributesId) AS ListingAttributesId,
               |            |COALESCE(fin.ListingId, f.ListingId) AS ListingId,
               |            |COALESCE(fin.ListingUnitAttributesId, f.ListingUnitAttributesId) AS ListingUnitAttributesId,
               |            |COALESCE(fin.ListingUnitId, f.ListingUnitId) AS ListingUnitId,
               |            |COALESCE(fin.LocalCurrencyId, f.LocalCurrencyId) AS LocalCurrencyId,
               |            |COALESCE(fin.OnlineBookingProviderTypeId, f.OnlineBookingProviderTypeId) AS OnlineBookingProviderTypeId,
               |            |COALESCE(fin.PaymentTypeId, f.PaymentTypeId) AS PaymentTypeId,
               |            |COALESCE(fin.PersonTypeId, f.PersonTypeId) AS PersonTypeId,
               |            |COALESCE(fin.RegionId, f.RegionId) AS RegionId,
               |            |COALESCE(fin.ReservationAvailabilityStatusTypeId, f.ReservationAvailabilityStatusTypeId) AS ReservationAvailabilityStatusTypeId,
               |            |COALESCE(fin.ReservationPaymentStatusTypeId, f.ReservationPaymentStatusTypeId) AS ReservationPaymentStatusTypeId,
               |            |COALESCE(fin.SiteId, f.SiteId) AS SiteId,
               |            |COALESCE(fin.SubscriptionId, f.SubscriptionId) AS SubscriptionId,
               |            |COALESCE(fin.TravelerEmailId, f.TravelerEmailId) AS TravelerEmailId,
               |            |COALESCE(fin.VisitorId, f.VisitorId) AS VisitorId,
               |            |COALESCE(fin.BookingCount, f.BookingCount) AS BookingCount,
               |            |COALESCE(fin.BookingValue, f.BookingValue) AS BookingValue,
               |            |COALESCE(fin.OrderAmount, f.OrderAmount) AS OrderAmount,
               |            |COALESCE(fin.RefundableDamageDepositAmount, f.RefundableDamageDepositAmount) AS RefundableDamageDepositAmount,
               |            |COALESCE(fin.RentalAmount, f.RentalAmount) AS RentalAmount,
               |            |COALESCE(fin.ServiceFeeAmount, f.ServiceFeeAmount) AS ServiceFeeAmount,
               |            |COALESCE(fin.TaxAmount, f.TaxAmount) AS TaxAmount,
               |            |COALESCE(fin.VasAmount, f.VasAmount) AS VasAmount,
               |            |COALESCE(fin.RefundAmount, f.RefundAmount) AS RefundAmount,
               |            |COALESCE(fin.PaidAmount, f.PaidAmount) AS PaidAmount,
               |            |COALESCE(fin.CommissionAmount, f.CommissionAmount) AS CommissionAmount,
               |            |COALESCE(fin.BookingChannelId, f.BookingChannelId) AS BookingChannelId,
               |            |COALESCE(fin.PaymentMethodTypeId, f.PaymentMethodTypeId) AS PaymentMethodTypeId,
               |            |COALESCE(fin.PaidAmountOffline, f.PaidAmountOffline) AS PaidAmountOffline,
               |            |COALESCE(fin.PaidAmountOnline, f.PaidAmountOnline) AS PaidAmountOnline,
               |            |COALESCE(fin.StrategicDestinationId, f.StrategicDestinationId) AS StrategicDestinationId,
               |            |COALESCE(fin.StrategicDestinationAttributesId, f.StrategicDestinationAttributesId) AS StrategicDestinationAttributesId,
               |            |COALESCE(fin.BookedNightsCount, f.BookedNightsCount) AS BookedNightsCount,
               |            |COALESCE(fin.VisitId, f.VisitId) AS VisitId,
               |            |COALESCE(fin.FullVisitorId, f.FullVisitorId) AS FullVisitorId,
               |            |COALESCE(fin.WebsiteReferralMediumSessionId, f.WebsiteReferralMediumSessionId) AS WebsiteReferralMediumSessionId,
               |            |COALESCE(fin.InquiryVisitId, f.InquiryVisitId) AS InquiryVisitId,
               |            |COALESCE(fin.InquiryFullVisitorId, f.InquiryFullVisitorId) AS InquiryFullVisitorId,
               |            |COALESCE(fin.InquiryMedium, f.InquiryMedium) AS InquiryMedium,
               |            |COALESCE(fin.InquiryWebsiteReferralMediumSessionId, f.InquiryWebsiteReferralMediumSessionId) AS InquiryWebsiteReferralMediumSessionId,
               |            |COALESCE(fin.InquirySource, f.InquirySource) AS InquirySource,
               |            |COALESCE(fin.BookingRequestVisitId, f.BookingRequestVisitId) AS BookingRequestVisitId,
               |            |COALESCE(fin.BookingRequestFullVisitorId, f.BookingRequestFullVisitorId) AS BookingRequestFullVisitorId,
               |            |COALESCE(fin.BookingRequestMedium, f.BookingRequestMedium) AS BookingRequestMedium,
               |            |COALESCE(fin.BookingRequestWebsiteReferralMediumSessionId, f.BookingRequestWebsiteReferralMediumSessionId) AS BookingRequestWebsiteReferralMediumSessionId,
               |            |COALESCE(fin.BookingRequestSource, f.BookingRequestSource) AS BookingRequestSource,
               |            |COALESCE(fin.CalculatedVisitId, f.CalculatedVisitId) AS CalculatedVisitId,
               |            |COALESCE(fin.CalculatedFullVisitorId, f.CalculatedFullVisitorId) AS CalculatedFullVisitorId,
               |            |COALESCE(fin.CalculatedBookingMedium, f.CalculatedBookingMedium) AS CalculatedBookingMedium,
               |            |COALESCE(fin.CalculatedBookingWebsiteReferralMediumSessionId, f.CalculatedBookingWebsiteReferralMediumSessionId) AS CalculatedBookingWebsiteReferralMediumSessionId,
               |            |COALESCE(fin.CalculatedBookingSource, f.CalculatedBookingSource) AS CalculatedBookingSource,
               |            |CURRENT_DATE() AS DWCreateDateTime,
               |            |CURRENT_DATE() AS DWUpdateDateTime
               |            |FROM tmpBookingfact f
               |            |FULL OUTER JOIN dfFinal fin
               |            |ON f.ReservationId = fin.ReservationId AND f.ReservationCreateDateId = fin.CreateDate
               |          """.stripMargin
|
|val dfFinalBookingFact = sql(query)
|
|dfFinalBookingFact.persist
|
|val fCount = dfFinalBookingFact.count
|
|println(s"Final Count: $fCount")
|
|dfFinalBookingFact.write.mode(SaveMode.Overwrite).format("orc").save("/user/aguyyala/bf_tmp")
|
|hdfsRemoveAndMove(dfs, dfc, "/user/aguyyala/bf_tmp", "/user/aguyyala/bf")
|
|checkAndCreateHiveDDL(hiveMetaStore, "aguyyala", "bookingfact", "orc", "/user/aguyyala/bookingfact", getColsFromDF(dfBookingfact))
|