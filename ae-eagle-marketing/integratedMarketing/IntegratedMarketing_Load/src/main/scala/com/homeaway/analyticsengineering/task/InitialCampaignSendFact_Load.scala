package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import java.text.SimpleDateFormat
import java.util.Calendar
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.loader.EDWLoader
import com.homeaway.analyticsengineering.encrypt.main.utilities.Email.{Mail, send}
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.SaveMode


class InitialCampaignSendFact_Load extends Utility {

  def run(): Unit = {

    val jobId = jc.getJobId(conf("ic.jobName"), conf("ic.objectName"), conf("ic.jobName"))
    val (campaignInstanceId, _) = jc.startJob(jobId)

    val checkPointDir = dfs.getHomeDirectory.toString + "/" + spark.sparkContext.applicationId
    spark.sparkContext.setCheckpointDir(checkPointDir)

    val edwLoader = new EDWLoader(spark)

    val attr = "Attributed"
    val sourceDataLocation = conf("s3aUri") + conf("s3Bucket") + "/" + conf("aeTier1DB")+ "/" + conf("dc.sourceDataLocationPrefix")
    val sourceDataFormat = conf("sourceDataFormat")
    val targetDataFormat = conf("targetDataFormat")
    val targetDataLocation = conf("s3aUri") + conf("s3Bucket") + "/" + conf("aeTier1DB")+ "/" + conf("dc.targetDataLocationPrefix")
    val finalDataLocation = targetDataLocation + attr
    val targetDB = conf("aeTier1DB")
    val targetTable = conf("ic.objectName")
    val finalTable = targetTable + "_" + attr

    val sql = spark.sql _

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)

    val cal = Calendar.getInstance()
    val startTimestamp = timeFormatter.format(cal.getTime)


    try {

      jc.logInfo(instanceId, s"START Historical Campaign sendFact Process", -1)

      log.info("Reading Historic Data from Sql Server dw_facts.dbo.campaignactivitydailyfact")
      var query =
        """
          				  |SELECT
          				  |      CAST(FORMAT(a.dateid, 'yyyyMMdd') AS Int) AS send_date
          				  |     ,b.assetname AS source_asset_name
          				  |     ,CAST(a.campaignid AS Int) AS source_campaign_id
          				  |     ,SUM(CAST(a.sendcount AS BigInt)) AS send_count
          				  |     ,SUM(CAST(a.openedcount AS BigInt)) AS unique_open_count
          				  |     ,SUM(CAST(a.clickedcount AS BigInt)) AS unique_click_count
          				  |     ,SUM(CAST(a.bouncedcount AS BigInt)) AS bounce_count
          				  |     ,SUM(CAST(0 AS BigInt)) AS subscribe_count
          				  |     ,SUM(CAST(a.unsubscribedcount AS BigInt)) AS unsubscribe_count
          				  |     ,SUM(CAST(0 AS BigInt)) AS form_submit_count
          				  |     ,SUM(CAST(a.openedcount AS BigInt)) AS total_open_count
          				  |     ,SUM(CAST(a.clickedcount AS BigInt)) AS total_click_count
          				  |FROM dw_facts.dbo.campaignactivitydailyfact a
          				  |LEFT JOIN dw.dbo.campaignasset b
          				  |ON a.campaignassetid = b.campaignassetid
          				  |LEFT JOIN dw.dbo.campaign c
          				  |ON a.campaignid = c.campaignid
          				  |WHERE dateid < '2017-08-01' AND c.campaignSourceId = 2
          				  |GROUP BY assetname, a.campaignid, dateid
        				""".stripMargin

      val historicCampaignData = edwLoader.getData(query).coalesce(1)
      historicCampaignData.createOrReplaceGlobalTempView("historicCampaignData")
      //			historicCampaignData = spark.createDataFrame(historicCampaignData.rdd, StructType(historicCampaignData.schema.map(x => x.copy(nullable = true))))
      //			historicCampaignData.persist()
      //			log.info(s"Historic Data Count: ${historicCampaignData.count}")

      log.info(s"Reading Source Data from location $sourceDataLocation")
      val mae = spark.read.format(sourceDataFormat).load(sourceDataLocation)
      mae.createOrReplaceTempView("mae")

      log.info("Union Historic Data and Current Data")
      query =
        """
          				  |SELECT
          				  |      CAST(send_date AS Int) AS send_date
          				  |     ,source_asset_name
          				  |     ,source_campaign_id
          				  |     ,send_count
          				  |     ,unique_open_count
          				  |     ,unique_click_count
          				  |     ,bounce_count
          				  |     ,subscribe_count
          				  |     ,unsubscribe_count
          				  |     ,form_submit_count
          				  |     ,total_open_count
          				  |     ,total_click_count
          				  |FROM global_temp.historicCampaignData
          				  |UNION ALL
          				  |SELECT
          				  |      CAST(send_date AS Int) AS send_date
          				  |     ,source_asset_name
          				  |     ,source_campaign_id
          				  |     ,COUNT(*) AS send_count
          				  |     ,SUM(CASE WHEN first_open_date IS NOT NULL THEN 1 ELSE 0 END) AS unique_open_count
          				  |     ,SUM(CASE WHEN first_click_date IS NOT NULL THEN 1 ELSE 0 END) AS unique_click_count
          				  |     ,SUM(CASE WHEN first_bounce_date IS NOT NULL THEN 1 ELSE 0 END) AS bounce_count
          				  |     ,SUM(CASE WHEN first_subscribe_date IS NOT NULL THEN 1 ELSE 0 END) AS subscribe_count
          				  |     ,SUM(CASE WHEN first_unsubscribe_date IS NOT NULL THEN 1 ELSE 0 END) AS unsubscribe_count
          				  |     ,SUM(CASE WHEN first_form_submit_date IS NOT NULL THEN 1 ELSE 0 END) AS form_submit_count
          				  |     ,SUM(email_open_count) AS total_open_count
          				  |     ,SUM(email_click_count) AS total_click_count
          				  |FROM mae
          				  |GROUP BY 1, 2, 3
        				""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFact = sql(query).distinct.coalesce(1)
      marketingActivityCampaignSendFact.persist
      marketingActivityCampaignSendFact.createOrReplaceTempView("marketingActivityCampaignSendFact")

      log.info(s"Saving Data to target Location: $targetDataLocation")
      saveDataFrameToDisk(marketingActivityCampaignSendFact, SaveMode.Overwrite, targetDataFormat, targetDataLocation)

      log.info("Check and Create Hive DDL")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(marketingActivityCampaignSendFact))


      log.info("Attributing Unknown Data on source_asset_name and source_campaign_id ")
      query =
        s"""
           			   |SELECT
           			   |      send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM
           			   |(
           			   |SELECT
           			   |      COALESCE(orig_send_date, unknown_send_date) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,orig_send_count + unknown_send_count AS send_count
           			   |     ,orig_unique_open_count + unknown_unique_open_count AS unique_open_count
           			   |     ,orig_unique_click_count + unknown_unique_click_count AS unique_click_count
           			   |     ,orig_bounce_count + unknown_bounce_count AS bounce_count
           			   |     ,orig_subscribe_count + unknown_subscribe_count AS subscribe_count
           			   |     ,orig_unsubscribe_count + unknown_unsubscribe_count AS unsubscribe_count
           			   |     ,orig_form_submit_count + unknown_form_submit_count AS form_submit_count
           			   |     ,orig_total_open_count + unknown_total_open_count AS total_open_count
           			   |     ,orig_total_click_count + unknown_total_click_count AS total_click_count
           			   |     ,row_number() OVER (PARTITION BY source_asset_name, source_campaign_id, COALESCE(unknown_send_date, orig_send_date) ORDER BY orig_send_date DESC) AS rank
           			   |FROM
           			   |(
           			   |SELECT
           			   |      a.send_date AS orig_send_date
           			   |     ,b.send_date AS unknown_send_date
           			   |     ,COALESCE(a.source_asset_name, b.source_asset_name) AS source_asset_name
           			   |     ,COALESCE(a.source_campaign_id, b.source_campaign_id) AS source_campaign_id
           			   |     ,COALESCE(a.send_count, 0) AS orig_send_count
           			   |     ,COALESCE(a.unique_open_count, 0) AS orig_unique_open_count
           			   |     ,COALESCE(a.unique_click_count, 0) AS orig_unique_click_count
           			   |     ,COALESCE(a.bounce_count, 0) AS orig_bounce_count
           			   |     ,COALESCE(a.subscribe_count, 0) AS orig_subscribe_count
           			   |     ,COALESCE(a.unsubscribe_count, 0) AS orig_unsubscribe_count
           			   |     ,COALESCE(a.form_submit_count, 0) AS orig_form_submit_count
           			   |     ,COALESCE(a.total_open_count, 0) AS orig_total_open_count
           			   |     ,COALESCE(a.total_click_count, 0) AS orig_total_click_count
           			   |     ,COALESCE(b.send_count, 0) AS unknown_send_count
           			   |     ,COALESCE(b.unique_open_count, 0) AS unknown_unique_open_count
           			   |     ,COALESCE(b.unique_click_count, 0) AS unknown_unique_click_count
           			   |     ,COALESCE(b.bounce_count, 0) AS unknown_bounce_count
           			   |     ,COALESCE(b.subscribe_count, 0) AS unknown_subscribe_count
           			   |     ,COALESCE(b.unsubscribe_count, 0) AS unknown_unsubscribe_count
           			   |     ,COALESCE(b.form_submit_count, 0) AS unknown_form_submit_count
           			   |     ,COALESCE(b.total_open_count, 0) AS unknown_total_open_count
           			   |     ,COALESCE(b.total_click_count, 0) AS unknown_total_click_count
           			   |FROM
           			   |(SELECT * FROM marketingActivityCampaignSendFact WHERE send_date != ${conf("defaultDate")}) a
           			   |FULL OUTER JOIN
           			   |(SELECT * FROM marketingActivityCampaignSendFact WHERE send_date = ${conf("defaultDate")}) b
           			   |ON a.source_asset_name = b.source_asset_name AND a.source_campaign_id = b.source_campaign_id
           			   |)
           			   |)
           			   |WHERE rank = 1
				""".stripMargin


      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFact_tmp = sql(query).coalesce(1)
      marketingActivityCampaignSendFact_tmp.createOrReplaceTempView("marketingActivityCampaignSendFact_tmp")


      query =
        s"""
           			   |SELECT
           			   |      CAST(send_date AS Int) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM marketingActivityCampaignSendFact_tmp
           			   |UNION ALL
           			   |(
           			   |SELECT
           			   |      CAST(a.send_date AS Int) AS send_date
           			   |     ,a.source_asset_name
           			   |     ,a.source_campaign_id
           			   |     ,a.send_count
           			   |     ,a.unique_open_count
           			   |     ,a.unique_click_count
           			   |     ,a.bounce_count
           			   |     ,a.subscribe_count
           			   |     ,a.unsubscribe_count
           			   |     ,a.form_submit_count
           			   |     ,a.total_open_count
           			   |     ,a.total_click_count
           			   |FROM
           			   |(SELECT * FROM marketingActivityCampaignSendFact WHERE send_date != ${conf("defaultDate")}) a
           			   |LEFT JOIN marketingActivityCampaignSendFact_tmp b
           			   |ON a.send_date = b.send_date AND a.source_asset_name = b.source_asset_name AND a.source_campaign_id = b.source_campaign_id
           			   |WHERE b.send_date IS NULL AND b.source_asset_name IS NULL AND b.source_campaign_id IS NULL
           			   |)
				""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFact_preserved = sql(query).distinct.coalesce(1)
      marketingActivityCampaignSendFact_preserved.createOrReplaceTempView("marketingActivityCampaignSendFact_preserved")


      log.info("Attributing Unknown Data on source_campaign_id")
      query =
        s"""
           			   |SELECT
           			   |      send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM
           			   |(
           			   |SELECT
           			   |      COALESCE(orig_send_date, unknown_send_date) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,orig_send_count + unknown_send_count AS send_count
           			   |     ,orig_unique_open_count + unknown_unique_open_count AS unique_open_count
           			   |     ,orig_unique_click_count + unknown_unique_click_count AS unique_click_count
           			   |     ,orig_bounce_count + unknown_bounce_count AS bounce_count
           			   |     ,orig_subscribe_count + unknown_subscribe_count AS subscribe_count
           			   |     ,orig_unsubscribe_count + unknown_unsubscribe_count AS unsubscribe_count
           			   |     ,orig_form_submit_count + unknown_form_submit_count AS form_submit_count
           			   |     ,orig_total_open_count + unknown_total_open_count AS total_open_count
           			   |     ,orig_total_click_count + unknown_total_click_count AS total_click_count
           			   |     ,row_number() OVER (PARTITION BY source_campaign_id, COALESCE(unknown_send_date, orig_send_date) ORDER BY orig_send_date DESC) AS rank
           			   |FROM
           			   |(
           			   |SELECT
           			   |      a.send_date AS orig_send_date
           			   |     ,b.send_date AS unknown_send_date
           			   |     ,COALESCE(a.source_asset_name, b.source_asset_name) AS source_asset_name
           			   |     ,COALESCE(a.source_campaign_id, b.source_campaign_id) AS source_campaign_id
           			   |     ,COALESCE(a.send_count, 0) AS orig_send_count
           			   |     ,COALESCE(a.unique_open_count, 0) AS orig_unique_open_count
           			   |     ,COALESCE(a.unique_click_count, 0) AS orig_unique_click_count
           			   |     ,COALESCE(a.bounce_count, 0) AS orig_bounce_count
           			   |     ,COALESCE(a.subscribe_count, 0) AS orig_subscribe_count
           			   |     ,COALESCE(a.unsubscribe_count, 0) AS orig_unsubscribe_count
           			   |     ,COALESCE(a.form_submit_count, 0) AS orig_form_submit_count
           			   |     ,COALESCE(a.total_open_count, 0) AS orig_total_open_count
           			   |     ,COALESCE(a.total_click_count, 0) AS orig_total_click_count
           			   |     ,COALESCE(b.send_count, 0) AS unknown_send_count
           			   |     ,COALESCE(b.unique_open_count, 0) AS unknown_unique_open_count
           			   |     ,COALESCE(b.unique_click_count, 0) AS unknown_unique_click_count
           			   |     ,COALESCE(b.bounce_count, 0) AS unknown_bounce_count
           			   |     ,COALESCE(b.subscribe_count, 0) AS unknown_subscribe_count
           			   |     ,COALESCE(b.unsubscribe_count, 0) AS unknown_unsubscribe_count
           			   |     ,COALESCE(b.form_submit_count, 0) AS unknown_form_submit_count
           			   |     ,COALESCE(b.total_open_count, 0) AS unknown_total_open_count
           			   |     ,COALESCE(b.total_click_count, 0) AS unknown_total_click_count
           			   |FROM
           			   |(SELECT * FROM marketingActivityCampaignSendFact_preserved WHERE send_date != ${conf("defaultDate")}) a
           			   |FULL OUTER JOIN
           			   |(
           			   |SELECT
           			   |      send_date
           			   |     ,source_campaign_id
           			   |     ,MAX(source_asset_name) AS source_asset_name
           			   |     ,SUM(send_count) AS send_count
           			   |     ,SUM(unique_open_count) AS unique_open_count
           			   |     ,SUM(unique_click_count) AS unique_click_count
           			   |     ,SUM(bounce_count) AS bounce_count
           			   |     ,SUM(subscribe_count) AS subscribe_count
           			   |     ,SUM(unsubscribe_count) AS unsubscribe_count
           			   |     ,SUM(form_submit_count) AS form_submit_count
           			   |     ,SUM(total_open_count) AS total_open_count
           			   |     ,SUM(total_click_count) AS total_click_count
           			   |FROM marketingActivityCampaignSendFact_preserved
           			   |WHERE send_date = ${conf("defaultDate")}
           			   |GROUP BY 1,2
           			   |) b
           			   |ON a.source_campaign_id = b.source_campaign_id
           			   |)
           			   |)
           			   |WHERE rank = 1
				""".stripMargin


      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFactStage = sql(query).coalesce(1)
      marketingActivityCampaignSendFactStage.createOrReplaceTempView("marketingActivityCampaignSendFactStage")


      query =
        s"""
           			   |SELECT
           			   |      CAST(send_date AS Int) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM marketingActivityCampaignSendFactStage
           			   |UNION ALL
           			   |(
           			   |SELECT
           			   |      CAST(a.send_date AS Int) AS send_date
           			   |     ,a.source_asset_name
           			   |     ,a.source_campaign_id
           			   |     ,a.send_count
           			   |     ,a.unique_open_count
           			   |     ,a.unique_click_count
           			   |     ,a.bounce_count
           			   |     ,a.subscribe_count
           			   |     ,a.unsubscribe_count
           			   |     ,a.form_submit_count
           			   |     ,a.total_open_count
           			   |     ,a.total_click_count
           			   |FROM
           			   |(SELECT * FROM marketingActivityCampaignSendFact_preserved WHERE send_date != ${conf("defaultDate")}) a
           			   |LEFT JOIN marketingActivityCampaignSendFactStage b
           			   |ON a.send_date = b.send_date AND a.source_asset_name = b.source_asset_name AND a.source_campaign_id = b.source_campaign_id
           			   |WHERE b.send_date IS NULL AND b.source_asset_name IS NULL AND b.source_campaign_id IS NULL
           			   |)
				""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFactFinal = sql(query).distinct.coalesce(1)
      marketingActivityCampaignSendFactFinal.createOrReplaceTempView("marketingActivityCampaignSendFactFinal")


      log.info(s"Saving Data to target Location: $finalDataLocation")
      saveDataFrameToDisk(marketingActivityCampaignSendFactFinal, SaveMode.Overwrite, targetDataFormat, finalDataLocation)

      log.info("Check and Create Hive DDL")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, finalTable, targetDataFormat, finalDataLocation, getColsFromDF(marketingActivityCampaignSendFactFinal))

      //			log.info("Add Hive Partitions")
      //			addHivePartitions(hiveMetaStore, targetDB, targetTable, constructHivePartitions(marketingActivityCampaignSendFact, targetPartCols, targetDataLocation))


      jc.logInfo(instanceId, "END Historic Campaign sendFact Process", -1)

      log.info(s"Updating Daily Campaign SendFact Process so that it can pick the Start Timestamp ($startTimestamp) from here")
      val dailyCampaignJobId = jc.getJobId(conf("ic.dailyProcessName"), conf("ic.objectName"), conf("ic.dailyProcessName"))
      val (dailyInstanceId, _) = jc.startJob(dailyCampaignJobId)
      jc.endJob(dailyInstanceId, 1, startTimestamp, startTimestamp)

      jc.endJob(campaignInstanceId, 1)
      jc.endJob(instanceId, 1)


      log.info("Validation")
      query =
        """
          |SELECT
          |      'send_fact_metrics' AS table
          |     ,CAST(SUM(send_count) AS string) AS send_count
          |     ,CAST(SUM(unique_open_count) AS string) AS unique_open_count
          |     ,CAST(SUM(unique_click_count) AS string) AS unique_click_count
          |     ,CAST(SUM(bounce_count) AS string) AS bounce_count
          |     ,CAST(SUM(subscribe_count) AS string) AS subscribe_count
          |     ,CAST(SUM(unsubscribe_count) AS string) AS unsubscribe_count
          |     ,CAST(SUM(form_submit_count) AS string) AS form_submit_count
          |     ,CAST(SUM(total_open_count) AS string) AS total_open_count
          |     ,CAST(SUM(total_click_count) AS string) AS total_click_count
          |FROM marketingActivityCampaignSendFact
        """.stripMargin

      log.info(s"Running Query: $query")
      val send_fact = sql(query)
      send_fact.persist
      send_fact.count

      query =
        """
          |SELECT
          |      'send_fact_attr_metrics' AS table
          |     ,CAST(SUM(send_count) AS string) AS send_count
          |     ,CAST(SUM(unique_open_count) AS string) AS unique_open_count
          |     ,CAST(SUM(unique_click_count) AS string) AS unique_click_count
          |     ,CAST(SUM(bounce_count) AS string) AS bounce_count
          |     ,CAST(SUM(subscribe_count) AS string) AS subscribe_count
          |     ,CAST(SUM(unsubscribe_count) AS string) AS unsubscribe_count
          |     ,CAST(SUM(form_submit_count) AS string) AS form_submit_count
          |     ,CAST(SUM(total_open_count) AS string) AS total_open_count
          |     ,CAST(SUM(total_click_count) AS string) AS total_click_count
          |FROM marketingActivityCampaignSendFactFinal
        """.stripMargin

      log.info(s"Running Query: $query")
      val send_fact_attr = sql(query)
      send_fact_attr.persist
      send_fact_attr.count


      val send_fact_metrics = transposeUDF(send_fact, Seq("table"))
      send_fact_metrics.createOrReplaceTempView("send_fact_metrics")

      val send_fact_attr_metrics = transposeUDF(send_fact_attr, Seq("table"))
      send_fact_attr_metrics.createOrReplaceTempView("send_fact_attr_metrics")

      query =
        s"""
           |SELECT
           |      a.column_name AS metric
           |     ,a.column_value AS send_fact
           |     ,b.column_value AS send_fact_attr
           |     ,(CAST(a.column_value AS LONG) - CAST(b.column_value AS LONG)) AS ${conf("thresholdColumn")}
           |FROM send_fact_metrics a JOIN send_fact_attr_metrics b
           |ON a.column_name = b.column_name
           |WHERE ABS(a.column_value - b.column_value) > ${conf("ic.errorThreshold")}
        """.stripMargin
      log.info(s"Running Query: $query")
      val df = sql(query)
      df.show(false)
      df.columns


      if (df.count > 0) {
        val htmlBody = df.collect.map { row =>
          val value = row.getAs(conf("thresholdColumn")).asInstanceOf[Long].abs; (if (value > conf("ic.errorThreshold").toInt) s"<tr bgcolor=${conf("red")}><td>") + row.toString.stripPrefix("[").stripSuffix("]").split(",").mkString("</td><td>") + "</td></tr>"
        }.mkString

        val rMsg =
          s"""
             |<style>
             |table, th, td {
             |    border-collapse: collapse;
             |    border: 2px solid black;
             |}
             |</style>
             |<table>
             |  <tr bgcolor=${conf("blue")}>
             |    <th>Metric</th>
             |    <th>Send_Fact</th>
             |    <th>Send_Fact_Attr</th>
             |    <th>Diff</th>
             |  </tr>
             |  $htmlBody
             |</table>
           """.stripMargin

        send a Mail(
          from = conf("from") -> conf("fromName"),
          to = conf("to").split(","),
          subject = conf("ic.subject"),
          message = conf("ic.message"),
          richMessage = Option(rMsg)
        )


      }

    }
    catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the InitialCampaignSendFact run for Instance: $campaignInstanceId")
        jc.endJob(campaignInstanceId, -1)
        throw e
    }

  }


}

