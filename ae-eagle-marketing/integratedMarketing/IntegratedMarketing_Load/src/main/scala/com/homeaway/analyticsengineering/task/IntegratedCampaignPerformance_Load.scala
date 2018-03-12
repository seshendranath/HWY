package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import java.text.SimpleDateFormat
import java.util.Calendar
import com.github.nscala_time.time.Imports.DateTime
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.loader.EDWLoader
import com.homeaway.analyticsengineering.encrypt.main.utilities.Email.{Mail, send}
import com.homeaway.analyticsengineering.encrypt.main.utilities.{SqlServer, Utility}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType


class IntegratedCampaignPerformance_Load extends Utility {


  def run(): Unit = {

    val jobId = jc.getJobId(conf("icp.jobName"), conf("icp.objectName"), conf("icp.jobName"))
    val (campaignInstanceId, _) = jc.startJob(jobId)

    val checkPointDir = dfs.getHomeDirectory.toString + "/" + spark.sparkContext.applicationId
    spark.sparkContext.setCheckpointDir(checkPointDir)

    val edwLoader = new EDWLoader(spark)
    val sqlObj = new SqlServer(spark)

    //spark.conf.set("spark.sql.shuffle.partitions","500")

    val sourceDB = conf("aeTier1DB")
    val sourceTable = conf("icp.sourceTable")
    val defaultUnknown = conf("defaultDate")
    val stageDB = conf("aeStageDB")
    val sqlDB = conf("icp.sqlDB")
    val brandTable = conf("icp.brandTable")
    val campaignSourceDataLocation = conf("s3aUri") + conf("omniBucket") + "/" + conf("omniDataLocationPrefix") + "/" + conf("campaignSourceDataLocationPrefix")
    val dwCampaignTable = conf("icp.dwCampaignTable")
    val coreKPIInquiryTable = conf("icp.coreKPIInquiryTable")
    val coreKPIBookingTable = conf("icp.coreKPIBookingTable")
    val coreKPIBookingRequestTable = conf("icp.coreKPIBookingRequestTable")
    val coreKPISalesTable = conf("icp.coreKPISalesTable")
    val rltLookupTable = conf("rlt.objectName")
    val emailLookupTable = conf("el.objectName")
    val gaLookupTable = conf("ga.objectName")
    val calendarTable = conf("icp.calendarTable")
    val targetDataFormat = conf("icp.targetDataFormat")
    val targetDataLocation = conf("s3aUri") + conf("s3Bucket") + "/" + conf("aeTier1DB") + "/" + conf("icp.objectName")
    val targetDB = conf("aeTier1DB")
    val targetTable = conf("icp.objectName")
    val sqlTargetDB = conf("icp.sqlTargetDB")
    val sqlTargetTable = conf("icp.sqlTargetTable")

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)

    val cal = Calendar.getInstance()
    val startTimestamp = timeFormatter.format(cal.getTime)
    val endTimestamp = DateTime.now.toString(timeFormat)

    val sql = spark.sql _

    try {

      jc.logInfo(instanceId, s"START IntegratedCampaignPerformance_Load Process", -1)

      log.info(s"Read marketing $sqlDB.$brandTable")
      val brand = edwLoader.getData(s"SELECT * FROM $sqlDB.$brandTable")
      brand.createOrReplaceGlobalTempView("brand")
      brand.persist
      brand.count


      log.info(s"Read marketing $sqlDB.$calendarTable")
      val calendar = edwLoader.getData(s"SELECT * FROM $sqlDB.$calendarTable")
      calendar.createOrReplaceGlobalTempView("calendar")
      calendar.persist
      calendar.count


      log.info(s"Read marketing campaign data from $sqlDB.$dwCampaignTable")
      val mec_DW = edwLoader.getData(s"SELECT * FROM $sqlDB.$dwCampaignTable")
      mec_DW.createOrReplaceGlobalTempView("mec_DW")
      mec_DW.persist
      mec_DW.count


      log.info(s"Read marketing marketingEloquaCampaigns from S3 Location")
      val mec_s3 = spark.read.parquet(campaignSourceDataLocation)
      mec_s3.createOrReplaceTempView("mec_s3")
      mec_s3.persist
      mec_s3.count


      log.info(s"Combine Campaign data from DW and S3 ")
      var query: String =
        s"""
           |SELECT
           |        COALESCE(a.campaignId, b.campaignId) as campaignId
           |       ,a.frequency as DW_frequency
           |       ,a.customer as DW_customer
           |       ,a.campaignName as DW_campaignName
           |       ,a.brandId as DW_brandId
           |       ,b.frequency as s3_frequency
           |       ,b.customer as s3_customer
           |       ,b.campaignName as s3_campaignName
           |       ,b.brand as s3_brandId
           |FROM global_temp.mec_DW a
           |FULL OUTER JOIN mec_s3 b
           |ON a.campaignId = b.campaignId
        """.stripMargin

      log.info(s"Running Query: $query")
      val mec = sql(query)
      mec.persist
      mec.count
      mec.createOrReplaceTempView("mec")


      log.info(s"Read SendFact from  $sourceDB.$sourceTable")
      query =
        s"""
           |SELECT * FROM $sourceDB.$sourceTable
        """.stripMargin

      log.info(s"Running Query: $query")
      val marketing_activity_send_fact = sql(query)
      marketing_activity_send_fact.persist
      marketing_activity_send_fact.count
      marketing_activity_send_fact.createOrReplaceTempView("marketing_activity_send_fact")


      log.info(s"Attribute Campaign and Brand data to SendFact")
      query =
        """
          |SELECT
          |       d.*
          |       ,COALESCE((CASE WHEN lower(c.regionalOperatingUnit) in ('','unknown','[unknown]','undefined','[undefined]') THEN '' ELSE c.regionalOperatingUnit END), '') AS region
          |       ,COALESCE((CASE WHEN lower(c.brandName) in ('','unknown','[unknown]','undefined','[undefined]') THEN '' ELSE c.brandName END), '') AS brand_name
          |
          |FROM (
          |SELECT
          |     a.*
          |    ,'email' AS marketing_channel
          |    ,COALESCE((CASE WHEN lower(CASE WHEN send_date<'20170801' then b.DW_frequency else b.s3_frequency END) in ('','unknown','[unknown]','undefined','[undefined]') THEN '' ELSE (CASE WHEN send_date<'20170801' then b.DW_frequency else b.s3_frequency END) END), '') AS email_type
          |    ,COALESCE((CASE WHEN lower(CASE WHEN send_date<'20170801' then b.DW_customer else b.s3_customer END)  in ('','unknown','[unknown]','undefined','[undefined]') THEN '' ELSE (CASE WHEN send_date<'20170801' then b.DW_customer else b.s3_customer END) END),'') AS customer_type
          |    ,COALESCE((CASE WHEN lower(CASE WHEN send_date<'20170801' then b.DW_campaignName else b.s3_campaignName END)  in ('','unknown','[unknown]','undefined','[undefined]') THEN '' ELSE (CASE WHEN send_date<'20170801' then b.DW_campaignName else b.s3_campaignName END) END),'') AS campaign_name
          |    ,COALESCE((CASE WHEN lower(CASE WHEN send_date<'20170801' then b.DW_brandid else b.s3_brandid END)  in ('','unknown','[unknown]','undefined','[undefined]') THEN '' ELSE (CASE WHEN send_date<'20170801' then b.DW_brandid else b.s3_brandid END) END),'') AS brandid
          |    ,'Email Marketing' AS marketing_subchannel
          |    ,CASE WHEN send_date<'20170801' THEN (
          |     CASE
          |         WHEN UPPER(b.DW_campaignName) like '%_TRV_%_ACQ_REC_%_WKND_%' OR UPPER(b.DW_campaignName) like '%WEEKEND%' THEN 'Weekender'
          |         WHEN UPPER(b.DW_campaignName) like '%_TRV_ACQ_TYML%' OR  UPPER(b.DW_campaignName) like '%TYML%' THEN 'TYML'
          |         WHEN UPPER(b.DW_campaignName) like '%_TRV_ACQ_VAS%' OR UPPER(b.DW_campaignName) like '%VAS%'  THEN 'VAS'
          |         WHEN UPPER(b.DW_campaignName) like '%_TRV_ACQ_WELCOME%' OR UPPER(b.DW_campaignName) like '%WELCOME%' THEN 'Welcome'
          |         WHEN UPPER(b.DW_campaignName) like '%_TRV_ACQ_PDP%' OR UPPER(b.DW_campaignName) like '%PDP RETARGETING%' THEN 'PDP Retargeting'
          |         WHEN UPPER(b.DW_campaignName) like '%_TRV_ACQ_BKANN%' OR UPPER(b.DW_campaignName) like '%ANNIVER%' THEN 'Anniversary'
          |         WHEN UPPER(b.DW_campaignName) like '%_TRV_ACQ_BLAST%' OR UPPER(b.DW_campaignName) like '%BLAST%' THEN 'Blast'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REN_REN_OLBE%' THEN 'FRBO OLB Enabled Manual Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REN_REN_OLBD%' THEN 'FRBO OLB Disabled Manual Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REN_REN%' THEN 'FRBO Manual Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REN_AUTOREN_OLBE%' THEN 'FRBO OLB Enabled Auto Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REN_AUTOREN_OLBD%' THEN 'FRBO OLB Disabled Auto Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REN_AUTOREN%' THEN 'FRBO Auto Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REN_MPMRENOTH%' THEN 'Micro PM Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REN_MPMRENFTR%' THEN 'Micro PM Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REN_AMPMRENOTH%' THEN 'Additional Micro PM Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REN_AMPMRENFTR%' THEN 'Additional Micro PM Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REN_SMALLREN%' THEN 'Small PM Renewal'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_ACQ_ACQ_PPS%' THEN 'FRBO PPS Acquisition'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_ACQ_ACQ_PPB%' THEN 'FRBO PPB Acquisition'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_ACQ_ACQ_ACC%' THEN 'FRBO Account Creation'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_ACQ_ACQ_PLL%' THEN 'FRBO Push Listing Live'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_ACQ_ACQ_PPSB_36M%' THEN 'FRBO Legacy Partials'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_ACQ_ADD_PPSB_LIV%' THEN 'FRBO Additional Listing'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_ACQ_LEADNURT%' THEN 'Lead Nurturing'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REA_REA%' THEN 'FRBO Reactivation'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REA_MPMREAOTH%' THEN 'Micro PM Reactivation'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REA_MPMREAFTR%' THEN 'Micro PM Reactivation'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REA_AMPMREAOTH%' THEN 'Additional Micro PM Reactivation'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REA_AMPMREAFTR%' THEN 'Additional Micro PM Reactivation'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REA_REA_PPSB_36M%' THEN 'FRBO Legacy Lapsed'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REA_REA_PPB%' THEN 'FRBO Hidden PPB Listing'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REL_MSTATE_%' THEN 'FRBO Monthly Statement'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REL_MSTATE_%' THEN 'PM Monthly Statement'
          |         WHEN UPPER(b.DW_campaignName) like '%_FPM_REL_MHUB_%' THEN 'Monthly Hub'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REL_GOLIVE%' THEN 'FRBO Go Live'
          |         WHEN UPPER(b.DW_campaignName) like '%_FBO_REL_ONB%' THEN 'FRBO Onboarding'
          |         WHEN UPPER(b.DW_campaignName) like '%_PMS_REL_ONB%' THEN 'PM Onboarding'
          |     END )
          |    ELSE (
          |     CASE
          |         WHEN UPPER(b.s3_campaignName) like '%_TRV_%_ACQ_REC_%_WKND_%' OR UPPER(b.s3_campaignName) like '%WEEKEND%' THEN 'Weekender'
          |         WHEN UPPER(b.s3_campaignName) like '%_TRV_ACQ_TYML%' OR  UPPER(b.s3_campaignName) like '%TYML%' THEN 'TYML'
          |         WHEN UPPER(b.s3_campaignName) like '%_TRV_ACQ_VAS%' OR UPPER(b.s3_campaignName) like '%VAS%' THEN 'VAS'
          |         WHEN UPPER(b.s3_campaignName) like '%_TRV_ACQ_WELCOME%' OR UPPER(b.s3_campaignName) like '%WELCOME%' THEN 'Welcome'
          |         WHEN UPPER(b.s3_campaignName) like '%_TRV_ACQ_PDP%'  OR UPPER(b.s3_campaignName) like '%PDP RETARGETING%' THEN 'PDP Retargeting'
          |         WHEN UPPER(b.s3_campaignName) like '%_TRV_ACQ_BKANN%' OR UPPER(b.s3_campaignName) like '%ANNIVER%' THEN 'Anniversary'
          |         WHEN UPPER(b.s3_campaignName) like '%_TRV_ACQ_BLAST%' OR UPPER(b.s3_campaignName) like '%BLAST%' THEN 'Blast'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REN_REN_OLBE%' THEN 'FRBO OLB Enabled Manual Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REN_REN_OLBD%' THEN 'FRBO OLB Disabled Manual Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REN_REN%' THEN 'FRBO Manual Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REN_AUTOREN_OLBE%' THEN 'FRBO OLB Enabled Auto Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REN_AUTOREN_OLBD%' THEN 'FRBO OLB Disabled Auto Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REN_AUTOREN%' THEN 'FRBO Auto Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REN_MPMRENOTH%' THEN 'Micro PM Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REN_MPMRENFTR%' THEN 'Micro PM Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REN_AMPMRENOTH%' THEN 'Additional Micro PM Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REN_AMPMRENFTR%' THEN 'Additional Micro PM Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REN_SMALLREN%' THEN 'Small PM Renewal'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_ACQ_ACQ_PPS%' THEN 'FRBO PPS Acquisition'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_ACQ_ACQ_PPB%' THEN 'FRBO PPB Acquisition'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_ACQ_ACQ_ACC%' THEN 'FRBO Account Creation'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_ACQ_ACQ_PLL%' THEN 'FRBO Push Listing Live'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_ACQ_ACQ_PPSB_36M%' THEN 'FRBO Legacy Partials'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_ACQ_ADD_PPSB_LIV%' THEN 'FRBO Additional Listing'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_ACQ_LEADNURT%' THEN 'Lead Nurturing'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REA_REA%' THEN 'FRBO Reactivation'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REA_MPMREAOTH%' THEN 'Micro PM Reactivation'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REA_MPMREAFTR%' THEN 'Micro PM Reactivation'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REA_AMPMREAOTH%' THEN 'Additional Micro PM Reactivation'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REA_AMPMREAFTR%' THEN 'Additional Micro PM Reactivation'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REA_REA_PPSB_36M%' THEN 'FRBO Legacy Lapsed'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REA_REA_PPB%' THEN 'FRBO Hidden PPB Listing'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REL_MSTATE_%' THEN 'FRBO Monthly Statement'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REL_MSTATE_%' THEN 'PM Monthly Statement'
          |         WHEN UPPER(b.s3_campaignName) like '%_FPM_REL_MHUB_%' THEN 'Monthly Hub'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REL_GOLIVE%' THEN 'FRBO Go Live'
          |         WHEN UPPER(b.s3_campaignName) like '%_FBO_REL_ONB%' THEN 'FRBO Onboarding'
          |         WHEN UPPER(b.s3_campaignName) like '%_PMS_REL_ONB%' THEN 'PM Onboarding'
          |     END )
          |    END AS campaign_type
          |FROM marketing_activity_send_fact a
          |LEFT JOIN mec b
          |ON a.source_campaign_id = b.campaignId
          |) as d
          |LEFT JOIN global_temp.brand c
          |ON d.brandId = c.brandId
        """.stripMargin

      log.info(s"Running Query: $query")
      val sendFact_stage = sql(query)
      sendFact_stage.persist
      sendFact_stage.count
      sendFact_stage.createOrReplaceTempView("sendFact_stage")


      log.info(s"Aggregate SendFact by Send_Date, Campaign_Name and Source_Asset_Name")
      query =
        """
          |SELECT
          |     CAST(send_date AS String) AS send_date
          |    ,CASE WHEN campaign_name ='' THEN (CASE WHEN source_asset_name='' THEN 'unknown' ELSE UPPER(source_asset_name) END) ELSE UPPER(campaign_name) END AS campaign_name
          |    ,CASE WHEN source_asset_name='' THEN 'unknown' ELSE UPPER(source_asset_name) END AS source_asset_name
          |    ,MAX(marketing_channel) AS marketing_channel
          |    ,MAX(marketing_subchannel) AS marketing_subchannel
          |    ,MAX(email_type) as email_type
          |    ,MAX(region) AS region
          |    ,MAX(brand_name) AS brand_name
          |    ,MAX(customer_type) AS customer_type
          |    ,MAX(campaign_type) AS campaign_type
          |    ,SUM(COALESCE(send_count, 0)) AS send_count
          |    ,SUM(COALESCE(total_open_count, 0)) AS total_open_count
          |    ,SUM(COALESCE(total_click_count, 0)) AS total_click_count
          |    ,SUM(COALESCE(bounce_count, 0)) AS bounce_count
          |    ,SUM(COALESCE(subscribe_count, 0)) AS subscribe_count
          |    ,SUM(COALESCE(unsubscribe_count, 0)) AS unsubscribe_count
          |    ,SUM(COALESCE(unique_open_count, 0)) AS unique_open_count
          |    ,SUM(COALESCE(unique_click_count, 0)) AS unique_click_count
          |    ,SUM(COALESCE(form_submit_count, 0)) AS form_submit_count
          |FROM sendFact_stage
          |GROUP BY 1, 2, 3
        """.stripMargin

      log.info(s"Running Query: $query")
      val sendFact = sql(query)
      sendFact.persist
      sendFact.count
      sendFact.createOrReplaceTempView("sendFact")

      log.info(s"Read $coreKPIInquiryTable")
      val vInquiry = sql(s"select * from $coreKPIInquiryTable ")
      vInquiry.createOrReplaceTempView("vInquiry")

      log.info(s"Read $coreKPIBookingTable")
      val vBooking = sql(s"select * from $coreKPIBookingTable  ")
      vBooking.createOrReplaceTempView("vBooking")

      log.info(s"Read $coreKPIBookingRequestTable")
      val vBookingRequest = sql(s"select * from $coreKPIBookingRequestTable  ")
      vBookingRequest.createOrReplaceTempView("vBookingRequest")

      log.info(s"Read $coreKPISalesTable")
      val vSales = sql(s"select * from $coreKPISalesTable  ")
      vSales.createOrReplaceTempView("vSales")


      log.info(s"Get NewListings")
      query =
        """
          |SELECT
          |        'Acquisition' AS goal
          |       ,b.dateid
          |       ,b.fullvisitorid
          |       ,b.visitid
          |       ,count( distinct b.listingid) as new_listing_count
          |       ,SUM(CASE WHEN b.PaymentTypeAbbrv = 'PPS' THEN 1 ELSE 0 END) AS new_listing_pps_count
          |       ,SUM(CASE WHEN b.PaymentTypeAbbrv = 'PPB' THEN 1 ELSE 0 END) AS new_listing_ppb_count
          |       ,SUM(c.revenue) as new_listing_pps_revenue
          |FROM
          |( SELECT
          |        a.dateid
          |       ,CASE WHEN
          |                a.fullvisitorid IS NULL
          |                OR lower(a.fullvisitorid) IN ('[unknown]', 'unknown','EDNFVID')
          |                THEN 'unknown' ELSE a.fullvisitorid END as fullvisitorid
          |       ,CASE WHEN
          |                a.visitid IS NULL
          |                OR lower(a.visitid) IN ('[unknown]', 'unknown','0')
          |                THEN 'unknown' ELSE a.visitid END as visitid
          |       ,a.ListingId
          |       ,a.SubscriptionId
          |       ,a.PaymentTypeAbbrv
          |FROM
          |( SELECT
          |       lof.ListingOnboardingStepFirstDateId as dateid
          |       ,lof.ListingId
          |      ,vsd.fullvisitorid
          |      ,vsd.visitid
          |      ,sub.SubscriptionId
          |      ,sub.PaymentTypeAbbrv
          |      ,sub.Renewal
          |      ,ROW_NUMBER() OVER (PARTITION BY lof.ListingId  ORDER BY sub.renewal ) AS row_num
          |FROM dw_facts.dbo.ListingOnboardingFact lof
          |JOIN dw.dbo.Site s ON lof.SiteId = s.SiteId
          |JOIN dw.dbo.VW_Customer c ON lof.CustomerId = c.CustomerId
          |LEFT JOIN DW_Traveler.dbo.VisitorSessionDetail vsd ON vsd.visitid = lof.visitid AND vsd.fullvisitorid = lof.fullvisitorid
          |LEFT JOIN dw.dbo.subscription sub ON sub.listingid = lof.listingid  AND (sub.PreviousSubscriptionId IS NULL)
          |WHERE lof.ListingOnboardingStepId = 11
          |AND vsd.MarketingSubChannel = 'Email Marketing'
          |) a
          |WHERE a.row_num = 1
          |) b
          |LEFT JOIN
          |( SELECT
          |         psf.subscriptionid
          |        ,SUM(psf.salesamount*cc.conversionrate) as revenue
          |FROM dw.dbo.VW_ProjectedSalesFact psf
          |JOIN dw.dbo.ProductCluster pc ON pc.ProductClusterId = psf.ProductClusterId
          |JOIN dw.dbo.CurrencyConversion cc ON psf.CurrencyConversionIdUSD = cc.CurrencyConversionId
          |JOIN dw.dbo.vw_Subscription vs ON vs.SubscriptionId = psf.SubscriptionId
          |WHERE pc.ProductSuite = 'Subscription'
          |AND vs.RefundedName = 'Not_Refunded'
          |GROUP BY psf.SubscriptionId
          |) c ON c.subscriptionid = b.subscriptionid
          |GROUP BY dateid,fullvisitorid,visitid
        """.stripMargin

      val new_listings = edwLoader.getData(query)
      new_listings.createOrReplaceGlobalTempView("new_listings")
      new_listings.persist
      new_listings.count


      log.info(s"Aggregate CoreKPI Booking by DateId, FullVisitorId and VisitId")
      query =
        """
          |SELECT
          |      DateId
          |     ,CASE WHEN
          |                fullvisitorid IS NULL
          |                OR lower(fullvisitorid) IN ('[unknown]', 'unknown','EDNFVID')
          |                THEN 'unknown' ELSE fullvisitorid END as fullvisitorid
          |     ,CASE WHEN
          |                visitid IS NULL
          |                OR lower(visitid) IN ('[unknown]', 'unknown','0')
          |                THEN 'unknown' ELSE visitid END as visitid
          |     ,SUM(grossBookingCount) AS grossBookingCount
          |     ,SUM(cancelledBookingCount) AS cancelledBookingCount
          |     ,SUM(grossBookingCount) - SUM(cancelledBookingCount) AS netBookingCount
          |     ,SUM(grossBookingValueAmountUSD) AS grossBookingValueAmountUSD
          |     ,SUM(cancelledGrossBookingValueAmountUSD) AS cancelledGrossBookingValueAmountUSD
          |     ,SUM(grossBookingValueAmountUSD) - SUM(cancelledGrossBookingValueAmountUSD) AS netGrossBookingValueAmountUSD
          |FROM vBooking
          |GROUP BY 1, 2, 3
        """.stripMargin

      log.info(s"Running Query: $query")
      val vB = sql(query)
      vB.persist
      vB.count
      vB.createOrReplaceTempView("vB")


      log.info(s"Aggregate CoreKPI BookingRequest by BookingRequestDateId, FullVisitorId and VisitId")
      query =
        """
          |SELECT
          |      BookingRequestDateId
          |     ,CASE WHEN
          |                fullvisitorid IS NULL
          |                OR lower(fullvisitorid) IN ('[unknown]', 'unknown','EDNFVID')
          |                THEN 'unknown' ELSE fullvisitorid END as fullvisitorid
          |     ,CASE WHEN
          |                visitid IS NULL
          |                OR lower(visitid) IN ('[unknown]', 'unknown','0')
          |                THEN 'unknown' ELSE visitid END as visitid
          |     ,SUM(bookingRequestCount) AS bookingRequestCount
          |FROM vBookingRequest
          |GROUP BY 1, 2,3
        """.stripMargin

      log.info(s"Running Query: $query")
      val vBR = sql(query)
      vBR.persist
      vBR.count
      vBR.createOrReplaceTempView("vBR")


      log.info(s"Aggregate CoreKPI Inquiry by InquiryDateId, FullVisitorId and VisitId")
      query =
        """
          |SELECT
          |      InquiryDateId
          |     ,CASE WHEN
          |                fullvisitorid IS NULL
          |                OR lower(fullvisitorid) IN ('[unknown]', 'unknown','EDNFVID')
          |                THEN 'unknown' ELSE fullvisitorid END as fullvisitorid
          |     ,CASE WHEN
          |                visitid IS NULL
          |                OR lower(visitid) IN ('[unknown]', 'unknown','0')
          |                THEN 'unknown' ELSE visitid END as visitid
          |     ,SUM(inquiryCount) AS inquiryCount
          |FROM vInquiry
          |GROUP BY 1, 2, 3
        """.stripMargin

      log.info(s"Running Query: $query")
      val vI = sql(query)
      vI.persist
      vI.count
      vI.createOrReplaceTempView("vI")


      log.info(s"Aggregate CoreKPI Sales by TransactionDateId, FullVisitorId and VisitId")
      query =
        """
          |SELECT
          |      TransactionDateId
          |     ,CASE WHEN
          |                fullvisitorid IS NULL
          |                OR lower(fullvisitorid) IN ('[unknown]', 'unknown','EDNFVID')
          |                THEN 'unknown' ELSE fullvisitorid END as fullvisitorid
          |     ,CASE WHEN
          |                visitid IS NULL
          |                OR lower(visitid) IN ('[unknown]', 'unknown','0')
          |                THEN 'unknown' ELSE visitid END as visitid
          |     ,SUM(transactionalSalesTotalAmountUSD) AS transactionalSalesTotalAmountUSD
          |FROM vSales
          |GROUP BY 1, 2, 3
        """.stripMargin

      log.info(s"Running Query: $query")
      val vS = sql(query)
      vS.persist
      vS.count
      vS.createOrReplaceTempView("vS")


      log.info(s"Combine All Core Metrics")
      query =
        """
          |SELECT
          |      COALESCE(b.DateId, s.TransactionDateId, inq.InquiryDateId, br.BookingRequestDateId, nl.dateid) DateId
          |     ,COALESCE(b.fullvisitorid, s.fullvisitorid, inq.fullvisitorid, br.fullvisitorid, nl.fullvisitorid) fullvisitorid
          |     ,COALESCE(b.visitid, s.visitid, inq.visitid, br.visitid, nl.visitid) visitid
          |     ,COALESCE(b.grossBookingCount, 0) grossBookingCount
          |     ,COALESCE(b.cancelledBookingCount, 0) cancelledBookingCount
          |     ,COALESCE(b.netBookingCount, 0) netBookingCount
          |     ,COALESCE(b.grossBookingValueAmountUSD, 0) grossBookingValueAmountUSD
          |     ,COALESCE(b.cancelledGrossBookingValueAmountUSD, 0) cancelledGrossBookingValueAmountUSD
          |     ,COALESCE(b.netGrossBookingValueAmountUSD, 0) netGrossBookingValueAmountUSD
          |     ,COALESCE(br.bookingRequestCount, 0) bookingRequestCount
          |     ,COALESCE(inq.inquiryCount, 0) inquiryCount
          |     ,COALESCE(s.transactionalSalesTotalAmountUSD, 0) transactionalSalesTotalAmountUSD
          |     ,COALESCE(new_listing_count,0) AS new_listing_count
          |     ,COALESCE(new_listing_pps_count,0) AS new_listing_pps_count
          |     ,COALESCE(new_listing_ppb_count, 0) AS new_listing_ppb_count
          |     ,COALESCE(new_listing_pps_revenue,0) AS new_listing_pps_revenue
          |FROM vB b
          |FULL OUTER JOIN vS s ON b.Dateid = s.TransactionDateId and b.fullvisitorid = s.fullvisitorid and b.visitid = s.visitid
          |FULL OUTER JOIN vI inq ON COALESCE(b.Dateid, s.TransactionDateId) = inq.InquiryDateId and COALESCE(b.fullvisitorid, s.fullvisitorid) = inq.fullvisitorid and COALESCE(b.visitid, s.visitid) = inq.visitid
          |FULL OUTER JOIN vBR br ON COALESCE(b.Dateid, s.TransactionDateId, inq.InquiryDateId) = br.BookingRequestDateId and COALESCE(b.fullvisitorid, s.fullvisitorid, inq.fullvisitorid) = br.fullvisitorid and COALESCE(b.visitid, s.visitid, inq.visitid) = br.visitid
          |FULL OUTER JOIN global_temp.new_listings nl ON COALESCE(b.Dateid, s.TransactionDateId, inq.InquiryDateId,br.BookingRequestDateId) = nl.dateid and COALESCE(b.fullvisitorid, s.fullvisitorid, inq.fullvisitorid,br.fullvisitorid) = nl.fullvisitorid and COALESCE(b.visitid, s.visitid, inq.visitid,br.visitid) = nl.visitid
        """.stripMargin

      log.info(s"Running Query: $query")
      val core_stage = sql(query)
      core_stage.persist
      core_stage.count
      core_stage.createOrReplaceTempView("core_stage")

      log.info(s"Read Visit_RLT_Attribution_GA from $stageDB.$rltLookupTable ")
      query =
        s"""
           |SELECT *
           |FROM $stageDB.$rltLookupTable
        """.stripMargin

      log.info(s"Running Query: $query")
      val rlt = sql(query)
      rlt.createOrReplaceTempView("rlt")
      rlt.persist
      rlt.count

      log.info(s"DeDup RLT on FullVisitorId and VisitId")
      query =
        s"""
           |SELECT a.*
           |FROM (
           |SELECT
           |      fullvisitorid
           |     ,visitid
           |     ,rlt_campaign
           |     ,row_number() OVER (PARTITION BY fullvisitorid, visitid ORDER BY visitid DESC) as rank
           |FROM rlt
           |) a
           |WHERE rank=1
        """.stripMargin

      log.info(s"Running Query: $query")
      val rlt_tmp = sql(query)
      rlt_tmp.createOrReplaceTempView("rlt_tmp")
      rlt_tmp.persist
      rlt_tmp.count


      log.info(s"Filter Core Metrics to include only Email Marketing ")
      query =
        """
          |SELECT
          |      CAST(DATE_FORMAT(CAST(UNIX_TIMESTAMP(CAST(dateid AS String), 'yyyy-MM-dd') AS TIMESTAMP),'yyyyMMdd') AS String) as dateid
          |     ,c.fullvisitorid
          |     ,c.visitid
          |     ,COALESCE(r.rlt_campaign, 'unknown') as rlt_campaign
          |     ,grossBookingCount
          |     ,cancelledBookingCount
          |     ,netBookingCount
          |     ,grossBookingValueAmountUSD
          |     ,cancelledGrossBookingValueAmountUSD
          |     ,netGrossBookingValueAmountUSD
          |     ,bookingRequestCount
          |     ,inquiryCount
          |     ,transactionalSalesTotalAmountUSD
          |     ,new_listing_count
          |     ,new_listing_pps_count
          |     ,new_listing_ppb_count
          |     ,new_listing_pps_revenue
          |FROM core_stage c
          |JOIN rlt_tmp r on c.fullvisitorid=r.fullvisitorid and c.visitid=r.visitid
        """.stripMargin

      log.info(s"Running Query: $query")
      val core = sql(query)
      core.persist
      core.count
      core.createOrReplaceTempView("core")


      log.info(s"Join Core KPI and RLT Data based on fullVisitorId")
      query =
        s"""
           |SELECT
           |      COALESCE(c.dateid,v.dateid) AS coreDate
           |     ,COALESCE(c.fullvisitorid,v.fullvisitorid) AS fullvisitorid
           |     ,COALESCE(c.visitid,v.visitid) AS visitid
           |     ,COALESCE(v.rlt_campaign,c.rlt_campaign) AS rlt_campaign
           |     ,COALESCE(v.totalVisits,0) AS visits
           |     ,COALESCE(v.datedSearch,0) AS datedSearch
           |     ,COALESCE(v.qualifiedvisit,0) AS qualifiedvisit
           |     ,COALESCE(c.grossBookingCount, 0) AS grossBookingCount
           |     ,COALESCE(c.cancelledBookingCount, 0) AS cancelledBookingCount
           |     ,COALESCE(c.netBookingCount, 0) AS netBookingCount
           |     ,COALESCE(c.grossBookingValueAmountUSD, 0) AS grossBookingValueAmountUSD
           |     ,COALESCE(c.cancelledGrossBookingValueAmountUSD, 0) AS cancelledGrossBookingValueAmountUSD
           |     ,COALESCE(c.netGrossBookingValueAmountUSD, 0) AS netGrossBookingValueAmountUSD
           |     ,COALESCE(c.bookingRequestCount, 0) AS bookingRequestCount
           |     ,COALESCE(c.inquiryCount, 0) AS inquiryCount
           |     ,COALESCE(c.transactionalSalesTotalAmountUSD, 0) AS transactionalSalesTotalAmountUSD
           |     ,COALESCE(c.new_listing_count,0) AS new_listing_count
           |     ,COALESCE(c.new_listing_pps_count,0) AS new_listing_pps_count
           |     ,COALESCE(c.new_listing_ppb_count, 0) AS new_listing_ppb_count
           |     ,COALESCE(c.new_listing_pps_revenue,0) AS new_listing_pps_revenue
           |FROM rlt v
           |FULL OUTER JOIN core c ON c.fullvisitorid = v.fullvisitorid AND v.visitid=c.visitid AND  v.dateid= c.DateId
        """.stripMargin

      log.info(s"Running Query: $query")
      val metrics = sql(query)
      metrics.persist
      metrics.count
      metrics.createOrReplaceTempView("metrics")


      log.info(s"Read EnrichedEventsGA from $gaLookupTable ")
      val galookup_df = sql(s"""SELECT * FROM $stageDB.$gaLookupTable""")//.repartition(500)
      log.info(s"Running Query: $query")
      galookup_df.persist
      galookup_df.count
      galookup_df.createOrReplaceTempView("galookup_df")


      log.info(s"Read EmailLookup from $emailLookupTable")
      query =
        s"""
           |SELECT *
           |FROM $stageDB.$emailLookupTable
        """.stripMargin

      log.info(s"Running Query: $query")
      val emaillookup_df = sql(query)
      emaillookup_df.persist
      emaillookup_df.count
      emaillookup_df.createOrReplaceTempView("emaillookup_df")


      log.info(s"Attribute Send Date to haexternalsourceid's from GA ")
      query =
        """
          |SELECT a.*
          |       ,b.activityDate as send_date
          |       ,UPPER(sourceassetname) as sourceassetname
          |       ,UPPER(campaignname) as campaignname
          |FROM galookup_df a
          |LEFT JOIN emaillookup_df b on lower(a.haexternalsourceid)=lower(regexp_replace(b.sourceemailrecipientid,"-",""))
        """.stripMargin

      log.info(s"Running Query: $query")
      val ga_send_attr = sql(query)
      ga_send_attr.persist
      ga_send_attr.count
      ga_send_attr.createOrReplaceTempView("ga_send_attr")


      log.info(s"Attribute Core Metrics with Send Date based on haexternalsourceid")
      query =
        """
          |
          |SELECT a.*
          |       ,b.send_date
          |       ,COALESCE(UPPER(b.sourceassetname),CASE WHEN a.rlt_campaign='unknown' THEN COALESCE(UPPER(b.ga_campaign),'unknown') ELSE UPPER(a.rlt_campaign) END) as asset_name
          |       ,COALESCE(UPPER(b.campaignname),CASE WHEN a.rlt_campaign='unknown' THEN COALESCE(UPPER(b.ga_campaign),'unknown') ELSE UPPER(a.rlt_campaign) END) as campaign_name
          |FROM metrics a
          |LEFT JOIN ga_send_attr b on a.fullvisitorid = b.fullvisitorid AND a.visitid=b.visitid
        """.stripMargin

      log.info(s"Running Query: $query")
      val core_metrics = sql(query)
      core_metrics.persist
      core_metrics.count
      core_metrics.createOrReplaceTempView("core_metrics")


      log.info(s"Core Metrics which have Campaign Send Date")
      query =
        """
          |
          |SELECT
          |        send_date
          |       ,coreDate AS core_date
          |       ,UPPER(campaign_name) as campaign_name
          |       ,UPPER(asset_name) as asset_name
          |       ,SUM(visits) AS total_visit_count
          |       ,COUNT(DISTINCT fullvisitorid) AS unique_visitor_count
          |       ,SUM(datedSearch) AS dated_search_count
          |       ,SUM(qualifiedvisit) AS qualified_visit_count
          |       ,SUM(grossBookingCount) AS gross_booking_count
          |       ,SUM(cancelledBookingCount) AS cancelled_booking_count
          |       ,SUM(netBookingCount) AS net_booking_count
          |       ,SUM(grossBookingValueAmountUSD) AS gross_booking_value_amount_usd
          |       ,SUM(cancelledGrossBookingValueAmountUSD) AS cancelled_booking_value_amount_usd
          |       ,SUM(netGrossBookingValueAmountUSD) AS net_booking_value_amount_usd
          |       ,SUM(bookingRequestCount) AS booking_request_count
          |       ,SUM(inquiryCount) AS inquiry_count
          |       ,SUM(transactionalSalesTotalAmountUSD) AS transactional_sales_value_amount_usd
          |       ,SUM(new_listing_count) AS new_listing_count
          |       ,SUM(new_listing_pps_count) AS new_listing_pps_count
          |       ,SUM(new_listing_ppb_count) AS new_listing_ppb_count
          |       ,SUM(new_listing_pps_revenue) AS new_listing_pps_revenue
          |FROM core_metrics
          |WHERE send_date IS NOT NULL
          |GROUP BY 1, 2, 3, 4
        """.stripMargin

      log.info(s"Running Query: $query")
      val core_metrics_attr = sql(query)
      core_metrics_attr.persist
      core_metrics_attr.count
      core_metrics_attr.createOrReplaceTempView("core_metrics_attr")


      log.info(s"Core Metrics which do not have Campaign Send Date")
      query =
        """
          |
          |SELECT
          |        coreDate AS core_date
          |       ,UPPER(campaign_name) as campaign_name
          |       ,UPPER(asset_name) as asset_name
          |       ,SUM(visits) AS total_visit_count
          |       ,COUNT(DISTINCT fullvisitorid) AS unique_visitor_count
          |       ,SUM(datedSearch) AS dated_search_count
          |       ,SUM(qualifiedvisit) AS qualified_visit_count
          |       ,SUM(grossBookingCount) AS gross_booking_count
          |       ,SUM(cancelledBookingCount) AS cancelled_booking_count
          |       ,SUM(netBookingCount) AS net_booking_count
          |       ,SUM(grossBookingValueAmountUSD) AS gross_booking_value_amount_usd
          |       ,SUM(cancelledGrossBookingValueAmountUSD) AS cancelled_booking_value_amount_usd
          |       ,SUM(netGrossBookingValueAmountUSD) AS net_booking_value_amount_usd
          |       ,SUM(bookingRequestCount) AS booking_request_count
          |       ,SUM(inquiryCount) AS inquiry_count
          |       ,SUM(transactionalSalesTotalAmountUSD) AS transactional_sales_value_amount_usd
          |       ,SUM(new_listing_count) AS new_listing_count
          |       ,SUM(new_listing_pps_count) AS new_listing_pps_count
          |       ,SUM(new_listing_ppb_count) AS new_listing_ppb_count
          |       ,SUM(new_listing_pps_revenue) AS new_listing_pps_revenue
          |FROM core_metrics
          |WHERE send_date IS NULL
          |GROUP BY 1, 2, 3
        """.stripMargin

      log.info(s"Running Query: $query")
      val core_metrics_attr_historic = sql(query)
      core_metrics_attr_historic.persist
      core_metrics_attr_historic.count
      core_metrics_attr_historic.createOrReplaceTempView("core_metrics_attr_historic")


      log.info(s"Join SendFact and Core Metrics using look back attribution")
      query =
        s"""
           |SELECT
           |      send_date
           |     ,core_date
           |     ,campaign_name
           |     ,source_asset_name
           |     ,marketing_channel
           |     ,marketing_subchannel
           |     ,email_type
           |     ,region
           |     ,brand_name
           |     ,customer_type
           |     ,campaign_type
           |     ,CASE WHEN send_rank>1 then 0 ELSE send_count END AS send_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE total_open_count END AS total_open_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE total_click_count END AS total_click_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE bounce_count END AS bounce_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE subscribe_count END AS subscribe_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unsubscribe_count END AS unsubscribe_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unique_open_count END AS unique_open_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unique_click_count END AS unique_click_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE form_submit_count END AS form_submit_count
           |     ,total_visit_count
           |     ,unique_visitor_count
           |     ,gross_booking_count
           |     ,cancelled_booking_count
           |     ,net_booking_count
           |     ,gross_booking_value_amount_usd
           |     ,cancelled_booking_value_amount_usd
           |     ,net_booking_value_amount_usd
           |     ,booking_request_count
           |     ,inquiry_count
           |     ,transactional_sales_value_amount_usd
           |     ,dated_search_count
           |     ,qualified_visit_count
           |     ,new_listing_count
           |     ,new_listing_pps_count
           |     ,new_listing_ppb_count
           |     ,new_listing_pps_revenue
           |FROM(
           | SELECT
           |      send_date
           |     ,core_date
           |     ,campaign_name
           |     ,source_asset_name
           |     ,marketing_channel
           |     ,marketing_subchannel
           |     ,email_type
           |     ,region
           |     ,brand_name
           |     ,customer_type
           |     ,campaign_type
           |     ,send_count
           |     ,total_open_count
           |     ,total_click_count
           |     ,bounce_count
           |     ,subscribe_count
           |     ,unsubscribe_count
           |     ,unique_open_count
           |     ,unique_click_count
           |     ,form_submit_count
           |     ,total_visit_count
           |     ,unique_visitor_count
           |     ,gross_booking_count
           |     ,cancelled_booking_count
           |     ,net_booking_count
           |     ,gross_booking_value_amount_usd
           |     ,cancelled_booking_value_amount_usd
           |     ,net_booking_value_amount_usd
           |     ,booking_request_count
           |     ,inquiry_count
           |     ,transactional_sales_value_amount_usd
           |     ,dated_search_count
           |     ,qualified_visit_count
           |     ,new_listing_count
           |     ,new_listing_pps_count
           |     ,new_listing_ppb_count
           |     ,new_listing_pps_revenue
           |     ,row_number() OVER (PARTITION BY send_date, campaign_name, source_asset_name ORDER BY core_date DESC) AS send_rank
           |FROM(
           |SELECT
           |      COALESCE(f.send_date, m.core_date) AS send_date
           |     ,COALESCE(m.core_date,'') AS core_date
           |     ,COALESCE(f.campaign_name, m.campaign_name) AS campaign_name
           |     ,COALESCE(f.source_asset_name, m.asset_name) AS source_asset_name
           |     ,COALESCE(f.marketing_channel, 'email') AS marketing_channel
           |     ,COALESCE(f.marketing_subchannel, 'Email Marketing') AS marketing_subchannel
           |     ,COALESCE(f.email_type, '') AS email_type
           |     ,COALESCE(f.region, '') AS region
           |     ,COALESCE(f.brand_name, '') AS brand_name
           |     ,COALESCE(f.customer_type, '') AS customer_type
           |     ,COALESCE(f.campaign_type, '') AS campaign_type
           |     ,COALESCE(f.send_count, 0) AS send_count
           |     ,COALESCE(f.total_open_count, 0) AS total_open_count
           |     ,COALESCE(f.total_click_count, 0) AS total_click_count
           |     ,COALESCE(f.bounce_count, 0) AS bounce_count
           |     ,COALESCE(f.subscribe_count, 0) AS subscribe_count
           |     ,COALESCE(f.unsubscribe_count, 0) AS unsubscribe_count
           |     ,COALESCE(f.unique_open_count, 0) AS unique_open_count
           |     ,COALESCE(f.unique_click_count, 0) AS unique_click_count
           |     ,COALESCE(f.form_submit_count, 0) AS form_submit_count
           |     ,COALESCE(m.total_visit_count, 0) AS total_visit_count
           |     ,COALESCE(m.unique_visitor_count, 0) AS unique_visitor_count
           |     ,COALESCE(m.dated_search_count, 0) AS dated_search_count
           |     ,COALESCE(m.qualified_visit_count, 0) AS qualified_visit_count
           |     ,COALESCE(m.gross_booking_count, 0) AS gross_booking_count
           |     ,COALESCE(m.cancelled_booking_count, 0) AS cancelled_booking_count
           |     ,COALESCE(m.net_booking_count, 0) AS net_booking_count
           |     ,COALESCE(m.gross_booking_value_amount_usd, 0) AS gross_booking_value_amount_usd
           |     ,COALESCE(m.cancelled_booking_value_amount_usd, 0) AS cancelled_booking_value_amount_usd
           |     ,COALESCE(m.net_booking_value_amount_usd, 0) AS net_booking_value_amount_usd
           |     ,COALESCE(m.booking_request_count, 0) AS booking_request_count
           |     ,COALESCE(m.inquiry_count, 0) AS inquiry_count
           |     ,COALESCE(m.transactional_sales_value_amount_usd, 0) AS transactional_sales_value_amount_usd
           |     ,COALESCE(m.new_listing_count,0) AS new_listing_count
           |     ,COALESCE(m.new_listing_pps_count,0) AS new_listing_pps_count
           |     ,COALESCE(m.new_listing_ppb_count,0) AS new_listing_ppb_count
           |     ,COALESCE(m.new_listing_pps_revenue,0) AS new_listing_pps_revenue
           |     ,row_number() OVER (PARTITION BY COALESCE(m.core_date, f.send_date), COALESCE(m.campaign_name, f.campaign_name), COALESCE(m.asset_name,f.source_asset_name) ORDER BY f.send_date DESC) AS rank
           |FROM sendFact f
           |FULL OUTER JOIN core_metrics_attr_historic m
           |ON  UPPER(f.source_asset_name) = UPPER(m.asset_name) AND f.send_date <= m.core_date
           |)
           |WHERE rank = 1
           |)
         """.stripMargin

      log.info(s"Running Query: $query")
      val historic_attr_stage = sql(query)
      historic_attr_stage.persist
      historic_attr_stage.count
      historic_attr_stage.createOrReplaceTempView("historic_attr_stage")


      val campaignActivity = spark.createDataFrame(sendFact.rdd, StructType(sendFact.schema.map(x => x.copy(nullable = true))))
      campaignActivity.createOrReplaceTempView("campaignActivity")

      val campaignMetrics = spark.createDataFrame(historic_attr_stage.rdd, StructType(historic_attr_stage.schema.map(x => x.copy(nullable = true))))
      campaignMetrics.createOrReplaceTempView("campaignMetrics")


      log.info(s"Preserve Sends which do not have Core Metrics")
      query =
        s"""

           |SELECT
           |       send_date
           |      ,core_date
           |      ,campaign_name
           |      ,source_asset_name
           |      ,marketing_channel
           |      ,marketing_subchannel
           |      ,email_type
           |      ,region
           |      ,brand_name
           |      ,customer_type
           |      ,campaign_type
           |      ,send_count
           |      ,total_open_count
           |      ,total_click_count
           |      ,bounce_count
           |      ,subscribe_count
           |      ,unsubscribe_count
           |      ,unique_open_count
           |      ,unique_click_count
           |      ,form_submit_count
           |      ,total_visit_count
           |      ,unique_visitor_count
           |      ,gross_booking_count
           |      ,cancelled_booking_count
           |      ,net_booking_count
           |      ,gross_booking_value_amount_usd
           |      ,cancelled_booking_value_amount_usd
           |      ,net_booking_value_amount_usd
           |      ,booking_request_count
           |      ,inquiry_count
           |      ,transactional_sales_value_amount_usd
           |      ,dated_search_count
           |      ,qualified_visit_count
           |      ,new_listing_count
           |      ,new_listing_pps_count
           |      ,new_listing_ppb_count
           |      ,new_listing_pps_revenue
           |FROM historic_attr_stage
           |UNION ALL
           |(SELECT
           |      a.send_date
           |     ,$defaultUnknown AS core_date
           |     ,a.campaign_name
           |     ,a.source_asset_name
           |     ,a.marketing_channel
           |     ,a.marketing_subchannel
           |     ,a.email_type
           |     ,a.region
           |     ,a.brand_name
           |     ,a.customer_type
           |     ,a.campaign_type
           |     ,a.send_count
           |     ,a.total_open_count
           |     ,a.total_click_count
           |     ,a.bounce_count
           |     ,a.subscribe_count
           |     ,a.unsubscribe_count
           |     ,a.unique_open_count
           |     ,a.unique_click_count
           |     ,a.form_submit_count
           |     ,0 AS total_visit_count
           |     ,0 AS unique_visitor_count
           |     ,0 AS gross_booking_count
           |     ,0 AS cancelled_booking_count
           |     ,0 AS net_booking_count
           |     ,0 AS gross_booking_value_amount_usd
           |     ,0 AS cancelled_booking_value_amount_usd
           |     ,0 AS net_booking_value_amount_usd
           |     ,0 AS booking_request_count
           |     ,0 AS inquiry_count
           |     ,0 AS transactional_sales_value_amount_usd
           |     ,0 AS dated_search_count
           |     ,0 AS qualified_visit_count
           |     ,0 AS new_listing_count
           |     ,0 AS new_listing_pps_count
           |     ,0 AS new_listing_ppb_count
           |     ,0 AS new_listing_pps_revenue
           |FROM campaignActivity a LEFT JOIN (SELECT DISTINCT send_date, campaign_name,  source_asset_name FROM campaignMetrics) b ON a.send_date = b.send_date AND a.campaign_name = b.campaign_name AND a.source_asset_name = b.source_asset_name
           |WHERE b.send_date is NULL AND b.campaign_name IS NULL AND b.source_asset_name IS NULL)
        """.stripMargin


      log.info(s"Running Query: $query")
      val historic_attr = sql(query)
      historic_attr.persist
      historic_attr.count
      historic_attr.createOrReplaceTempView("historic_attr")


      log.info(s"Join SendFact and Core Metrics using new attribution")
      query =
        s"""
           |SELECT
           |      send_date
           |     ,core_date
           |     ,campaign_name
           |     ,source_asset_name
           |     ,marketing_channel
           |     ,marketing_subchannel
           |     ,email_type
           |     ,region
           |     ,brand_name
           |     ,customer_type
           |     ,campaign_type
           |     ,CASE WHEN send_rank>1 then 0 ELSE send_count END AS send_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE total_open_count END AS total_open_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE total_click_count END AS total_click_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE bounce_count END AS bounce_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE subscribe_count END AS subscribe_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unsubscribe_count END AS unsubscribe_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unique_open_count END AS unique_open_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unique_click_count END AS unique_click_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE form_submit_count END AS form_submit_count
           |     ,total_visit_count
           |     ,unique_visitor_count
           |     ,gross_booking_count
           |     ,cancelled_booking_count
           |     ,net_booking_count
           |     ,gross_booking_value_amount_usd
           |     ,cancelled_booking_value_amount_usd
           |     ,net_booking_value_amount_usd
           |     ,booking_request_count
           |     ,inquiry_count
           |     ,transactional_sales_value_amount_usd
           |     ,dated_search_count
           |     ,qualified_visit_count
           |     ,new_listing_count
           |     ,new_listing_pps_count
           |     ,new_listing_ppb_count
           |     ,new_listing_pps_revenue
           |FROM
           |(
           |SELECT
           |      COALESCE(f.send_date, m.send_date) AS send_date
           |     ,COALESCE(m.core_date,'') AS core_date
           |     ,COALESCE(f.campaign_name, m.campaign_name)  AS campaign_name
           |     ,COALESCE(f.source_asset_name, m.asset_name)  AS source_asset_name
           |     ,COALESCE(f.marketing_channel, 'email') AS marketing_channel
           |     ,COALESCE(f.marketing_subchannel, 'Email Marketing')AS marketing_subchannel
           |     ,COALESCE(f.email_type, '') AS email_type
           |     ,COALESCE(f.region, '') AS region
           |     ,COALESCE(f.brand_name, '') AS brand_name
           |     ,COALESCE(f.customer_type, '') AS customer_type
           |     ,COALESCE(f.campaign_type, '') AS campaign_type
           |     ,COALESCE(f.send_count, 0) AS send_count
           |     ,COALESCE(f.total_open_count, 0) AS total_open_count
           |     ,COALESCE(f.total_click_count, 0) AS total_click_count
           |     ,COALESCE(f.bounce_count, 0) AS bounce_count
           |     ,COALESCE(f.subscribe_count, 0) AS subscribe_count
           |     ,COALESCE(f.unsubscribe_count, 0) AS unsubscribe_count
           |     ,COALESCE(f.unique_open_count, 0) AS unique_open_count
           |     ,COALESCE(f.unique_click_count, 0) AS unique_click_count
           |     ,COALESCE(f.form_submit_count, 0) AS form_submit_count
           |     ,COALESCE(m.total_visit_count, 0) AS total_visit_count
           |     ,COALESCE(m.unique_visitor_count, 0) AS unique_visitor_count
           |     ,COALESCE(m.dated_search_count, 0) AS dated_search_count
           |     ,COALESCE(m.qualified_visit_count, 0) AS qualified_visit_count
           |     ,COALESCE(m.gross_booking_count, 0) AS gross_booking_count
           |     ,COALESCE(m.cancelled_booking_count, 0) AS cancelled_booking_count
           |     ,COALESCE(m.net_booking_count, 0) AS net_booking_count
           |     ,COALESCE(m.gross_booking_value_amount_usd, 0) AS gross_booking_value_amount_usd
           |     ,COALESCE(m.cancelled_booking_value_amount_usd, 0) AS cancelled_booking_value_amount_usd
           |     ,COALESCE(m.net_booking_value_amount_usd, 0) AS net_booking_value_amount_usd
           |     ,COALESCE(m.booking_request_count, 0) AS booking_request_count
           |     ,COALESCE(m.inquiry_count, 0) AS inquiry_count
           |     ,COALESCE(m.transactional_sales_value_amount_usd, 0) AS transactional_sales_value_amount_usd
           |     ,COALESCE(m.new_listing_count,0) AS new_listing_count
           |     ,COALESCE(m.new_listing_pps_count,0) AS new_listing_pps_count
           |     ,COALESCE(m.new_listing_ppb_count,0) AS new_listing_ppb_count
           |     ,COALESCE(m.new_listing_pps_revenue,0) AS new_listing_pps_revenue
           |     ,row_number() OVER (PARTITION BY COALESCE(f.send_date, m.send_date), COALESCE(f.campaign_name, m.campaign_name), COALESCE(f.source_asset_name, m.asset_name) ORDER BY m.core_date DESC) AS send_rank
           |FROM sendFact f
           |RIGHT OUTER JOIN core_metrics_attr m
           |ON f.send_date = m.send_date AND UPPER(f.campaign_name)=UPPER(m.campaign_name) AND UPPER(f.source_asset_name) = UPPER(m.asset_name)
           |)
        """.
          stripMargin

      log.info(s"Running Query: $query")
      val ongoing_attr = sql(query)
      ongoing_attr.persist
      ongoing_attr.count
      ongoing_attr.createOrReplaceTempView("ongoing_attr")


      log.info(s"Join historic attributed send fact with on-going attributed")
      query =
        s"""
           |SELECT
           |      send_date
           |     ,COALESCE((CASE WHEN core_date = '' THEN $defaultUnknown ELSE core_date END), $defaultUnknown)  AS core_date
           |     ,campaign_name
           |     ,source_asset_name
           |     ,marketing_channel
           |     ,marketing_subchannel
           |     ,email_type
           |     ,region
           |     ,brand_name
           |     ,customer_type
           |     ,campaign_type
           |     ,CASE WHEN send_rank>1 then 0 ELSE send_count END AS send_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE total_open_count END AS total_open_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE total_click_count END AS total_click_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE bounce_count END AS bounce_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE subscribe_count END AS subscribe_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unsubscribe_count END AS unsubscribe_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unique_open_count END AS unique_open_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE unique_click_count END AS unique_click_count
           |     ,CASE WHEN send_rank>1 then 0 ELSE form_submit_count END AS form_submit_count
           |     ,total_visit_count
           |     ,unique_visitor_count
           |     ,gross_booking_count
           |     ,cancelled_booking_count
           |     ,net_booking_count
           |     ,gross_booking_value_amount_usd
           |     ,cancelled_booking_value_amount_usd
           |     ,net_booking_value_amount_usd
           |     ,booking_request_count
           |     ,inquiry_count
           |     ,transactional_sales_value_amount_usd
           |     ,dated_search_count
           |     ,qualified_visit_count, send_rank
           |     ,new_listing_count
           |     ,new_listing_pps_count
           |     ,new_listing_ppb_count
           |     ,new_listing_pps_revenue
           |FROM
           |(
           |SELECT
           |      send_date
           |     ,core_date
           |     ,campaign_name
           |     ,source_asset_name
           |     ,marketing_channel
           |     ,marketing_subchannel
           |     ,email_type
           |     ,region
           |     ,brand_name
           |     ,customer_type
           |     ,campaign_type
           |     ,send_count
           |     ,total_open_count
           |     ,total_click_count
           |     ,bounce_count
           |     ,subscribe_count
           |     ,unsubscribe_count
           |     ,unique_open_count
           |     ,unique_click_count
           |     ,form_submit_count
           |     ,total_visit_count
           |     ,unique_visitor_count
           |     ,gross_booking_count
           |     ,cancelled_booking_count
           |     ,net_booking_count
           |     ,gross_booking_value_amount_usd
           |     ,cancelled_booking_value_amount_usd
           |     ,net_booking_value_amount_usd
           |     ,booking_request_count
           |     ,inquiry_count
           |     ,transactional_sales_value_amount_usd
           |     ,dated_search_count
           |     ,qualified_visit_count
           |     ,new_listing_count
           |     ,new_listing_pps_count
           |     ,new_listing_ppb_count
           |     ,new_listing_pps_revenue
           |     ,row_number() OVER (PARTITION BY send_date, campaign_name, source_asset_name ORDER BY send_count DESC, core_date DESC) AS send_rank
           |FROM (
           |SELECT
           |      COALESCE(a.send_date, b.send_date) AS send_date
           |     ,COALESCE(a.core_date, b.core_date) AS core_date
           |     ,COALESCE(a.campaign_name, b.campaign_name) AS campaign_name
           |     ,COALESCE(a.source_asset_name, b.source_asset_name)  AS source_asset_name
           |     ,COALESCE(a.marketing_channel, b.marketing_channel) AS marketing_channel
           |     ,COALESCE(a.marketing_subchannel, b.marketing_subchannel) AS marketing_subchannel
           |     ,COALESCE(a.email_type, b.email_type) AS email_type
           |     ,COALESCE(a.region, b.region) AS region
           |     ,COALESCE(a.brand_name, b.brand_name) AS brand_name
           |     ,COALESCE(a.customer_type, b.customer_type) AS customer_type
           |     ,COALESCE(a.campaign_type, b.campaign_type) AS campaign_type
           |     ,COALESCE(a.send_count, b.send_count) AS send_count
           |     ,COALESCE(a.total_open_count, b.total_open_count) AS total_open_count
           |     ,COALESCE(a.total_click_count, b.total_click_count) AS total_click_count
           |     ,COALESCE(a.bounce_count, b.bounce_count) AS bounce_count
           |     ,COALESCE(a.subscribe_count, b.subscribe_count) AS subscribe_count
           |     ,COALESCE(a.unsubscribe_count, b.unsubscribe_count) AS unsubscribe_count
           |     ,COALESCE(a.unique_open_count, b.unique_open_count) AS unique_open_count
           |     ,COALESCE(a.unique_click_count, b.unique_click_count) AS unique_click_count
           |     ,COALESCE(a.form_submit_count, b.form_submit_count) AS form_submit_count
           |     ,COALESCE(a.total_visit_count, 0) + COALESCE(b.total_visit_count, 0) AS total_visit_count
           |     ,COALESCE(a.unique_visitor_count, 0) + COALESCE(b.unique_visitor_count, 0) AS unique_visitor_count
           |     ,COALESCE(a.dated_search_count, 0) + COALESCE(b.dated_search_count, 0) AS dated_search_count
           |     ,COALESCE(a.qualified_visit_count, 0) + COALESCE(b.qualified_visit_count, 0) AS qualified_visit_count
           |     ,COALESCE(a.gross_booking_count, 0) + COALESCE(b.gross_booking_count, 0) AS gross_booking_count
           |     ,COALESCE(a.cancelled_booking_count, 0) + COALESCE(b.cancelled_booking_count, 0) AS cancelled_booking_count
           |     ,COALESCE(a.net_booking_count, 0) + COALESCE(b.net_booking_count, 0) AS net_booking_count
           |     ,COALESCE(a.gross_booking_value_amount_usd, 0) + COALESCE(b.gross_booking_value_amount_usd, 0) AS gross_booking_value_amount_usd
           |     ,COALESCE(a.cancelled_booking_value_amount_usd, 0) + COALESCE(b.cancelled_booking_value_amount_usd, 0) AS cancelled_booking_value_amount_usd
           |     ,COALESCE(a.net_booking_value_amount_usd, 0) + COALESCE(b.net_booking_value_amount_usd, 0) AS net_booking_value_amount_usd
           |     ,COALESCE(a.booking_request_count, 0) + COALESCE(b.booking_request_count, 0) AS booking_request_count
           |     ,COALESCE(a.inquiry_count, 0) + COALESCE(b.inquiry_count, 0) AS inquiry_count
           |     ,COALESCE(a.transactional_sales_value_amount_usd, 0) + COALESCE(b.transactional_sales_value_amount_usd, 0) AS transactional_sales_value_amount_usd
           |     ,COALESCE(a.new_listing_count,0) + COALESCE(b.new_listing_count,0) AS new_listing_count
           |     ,COALESCE(a.new_listing_pps_count,0) + COALESCE(b.new_listing_pps_count,0) AS new_listing_pps_count
           |     ,COALESCE(a.new_listing_ppb_count,0) + COALESCE(b.new_listing_ppb_count,0) AS new_listing_ppb_count
           |     ,COALESCE(a.new_listing_pps_revenue,0) + COALESCE(b.new_listing_pps_revenue,0) AS new_listing_pps_revenue
           |FROM historic_attr a
           |FULL OUTER JOIN ongoing_attr b
           |ON a.send_date = b.send_date AND a.core_date = b.core_date AND a.campaign_name = b.campaign_name AND a.source_asset_name = b.source_asset_name
           |))
        """.
          stripMargin

      log.info(s"Running Query: $query")
      val marketing_campaign_performance_tmp = sql(query)
      marketing_campaign_performance_tmp.persist
      marketing_campaign_performance_tmp.count
      marketing_campaign_performance_tmp.createOrReplaceTempView("marketing_campaign_performance_tmp")


      log.info(s"Apply final calculations on marketing_campaign_performance_tmp ")
      query =
        s"""
           |SELECT
           |        send_date AS send_date
           |       ,core_date AS core_date
           |       ,campaign_name
           |       ,source_asset_name
           |       ,marketing_channel
           |       ,marketing_subchannel
           |       ,email_type
           |       ,region
           |       ,brand_name
           |       ,campaign_type
           |       ,customer_type
           |       ,send_count AS total_send_count
           |       ,send_count - bounce_count AS total_delivered_count
           |       ,total_open_count AS total_open_count
           |       ,unique_open_count AS unique_open_count
           |       ,ROUND((total_open_count/(send_count - bounce_count)) * 100, 2) AS total_open_rate
           |       ,ROUND((unique_open_count/(send_count - bounce_count)) * 100, 2) AS unique_open_rate
           |       ,total_click_count AS total_click_count
           |       ,unique_click_count AS unique_click_count
           |       ,ROUND((total_click_count/(send_count - bounce_count)) * 100, 2) AS total_click_rate
           |       ,ROUND((unique_click_count/(send_count - bounce_count)) * 100, 2) AS unique_click_rate
           |       ,ROUND((total_click_count/total_open_count) * 100, 2) AS total_click_to_open_rate
           |       ,ROUND((unique_click_count/unique_open_count) * 100, 2) AS unique_click_to_open_rate
           |       ,bounce_count AS total_bounce_count
           |       ,ROUND((bounce_count/send_count) * 100, 2) AS total_bounce_rate
           |       ,subscribe_count AS total_subscribe_count
           |       ,ROUND((subscribe_count/(send_count - bounce_count)) * 100, 2) AS total_subscribe_rate
           |       ,unsubscribe_count + form_submit_count AS total_unsubscribe_count
           |       ,ROUND(((unsubscribe_count + form_submit_count)/(send_count - bounce_count)) * 100, 2) AS total_unsubscribe_rate
           |       ,form_submit_count AS total_form_submit_count
           |       ,total_visit_count
           |       ,unique_visitor_count
           |       ,gross_booking_count
           |       ,cancelled_booking_count AS cancelled_booking_count
           |       ,net_booking_count AS net_booking_count
           |       ,gross_booking_value_amount_usd AS gross_booking_value
           |       ,cancelled_booking_value_amount_usd AS cancelled_booking_value
           |       ,net_booking_value_amount_usd AS net_booking_value
           |       ,booking_request_count
           |       ,inquiry_count
           |       ,transactional_sales_value_amount_usd AS transactional_sales_value
           |       ,dated_search_count
           |       ,qualified_visit_count
           |       ,new_listing_count
           |       ,new_listing_pps_count
           |       ,new_listing_ppb_count
           |       ,new_listing_pps_revenue
           |FROM
           |(
           |SELECT
           |       send_date
           |      ,core_date
           |      ,TRIM(COALESCE((CASE WHEN lower(campaign_name) in ('','unknown','[unknown]','undefined','[undefined]') THEN 'unknown' ELSE campaign_name END), 'unknown')) AS campaign_name
           |      ,TRIM(COALESCE((CASE WHEN lower(source_asset_name) in ('','unknown','[unknown]','undefined','[undefined]') THEN 'unknown' ELSE source_asset_name END), 'unknown')) AS source_asset_name
           |      ,TRIM(MAX(marketing_channel)) AS marketing_channel
           |      ,TRIM(MAX(marketing_subchannel)) AS marketing_subchannel
           |      ,TRIM(MAX(COALESCE((CASE WHEN lower(email_type) in ('','unknown','[unknown]','undefined','[undefined]') THEN 'unknown' ELSE email_type END), 'unknown'))) AS email_type
           |      ,TRIM(MAX(COALESCE((CASE WHEN lower(region) in ('','unknown','[unknown]','undefined','[undefined]') THEN 'unknown' ELSE region END), 'unknown'))) AS region
           |      ,TRIM(MAX(COALESCE((CASE WHEN lower(brand_name) in ('','unknown','[unknown]','undefined','[undefined]') THEN 'unknown' ELSE brand_name END), 'unknown'))) AS brand_name
           |      ,TRIM(MAX(COALESCE((CASE WHEN lower(campaign_type) in ('','unknown','[unknown]','undefined','[undefined]') THEN 'unknown' ELSE campaign_type END), 'unknown'))) AS campaign_type
           |      ,TRIM(MAX(COALESCE((CASE WHEN lower(customer_type) in ('','unknown','[unknown]','undefined','[undefined]') THEN 'unknown' ELSE customer_type END), 'unknown'))) AS customer_type
           |      ,SUM(send_count) AS send_count
           |      ,SUM(total_open_count) AS total_open_count
           |      ,SUM(total_click_count) AS total_click_count
           |      ,SUM(bounce_count) AS bounce_count
           |      ,SUM(subscribe_count) AS subscribe_count
           |      ,SUM(unsubscribe_count) AS unsubscribe_count
           |      ,SUM(unique_open_count) AS unique_open_count
           |      ,SUM(unique_click_count) AS unique_click_count
           |      ,SUM(form_submit_count) AS form_submit_count
           |      ,SUM(total_visit_count) AS total_visit_count
           |      ,SUM(unique_visitor_count) AS unique_visitor_count
           |      ,SUM(gross_booking_count) AS gross_booking_count
           |      ,SUM(cancelled_booking_count) AS cancelled_booking_count
           |      ,SUM(net_booking_count) AS net_booking_count
           |      ,SUM(gross_booking_value_amount_usd) AS gross_booking_value_amount_usd
           |      ,SUM(cancelled_booking_value_amount_usd) AS cancelled_booking_value_amount_usd
           |      ,SUM(net_booking_value_amount_usd) AS net_booking_value_amount_usd
           |      ,SUM(booking_request_count) AS booking_request_count
           |      ,SUM(inquiry_count) AS inquiry_count
           |      ,SUM(transactional_sales_value_amount_usd) AS transactional_sales_value_amount_usd
           |      ,SUM(dated_search_count) AS dated_search_count
           |      ,SUM(qualified_visit_count) AS qualified_visit_count
           |      ,SUM(new_listing_count) AS new_listing_count
           |      ,SUM(new_listing_pps_count) AS new_listing_pps_count
           |      ,SUM(new_listing_ppb_count) AS new_listing_ppb_count
           |      ,SUM(new_listing_pps_revenue) AS new_listing_pps_revenue
           |FROM marketing_campaign_performance_tmp
           |GROUP BY 1,2,3,4
           |)
        """.stripMargin
      val marketing_campaign_performance_stage = sql(query)
      marketing_campaign_performance_stage.persist
      marketing_campaign_performance_stage.count
      marketing_campaign_performance_stage.createOrReplaceTempView("marketing_campaign_performance_stage")



      log.info(s"Join with Calendar Table ")
      query =
        s"""
           |SELECT
           |        c.*
           |       ,d.dateId as core_date_filter
           |       ,d.weekenddate as core_date_week_end_date
           |       ,d.weekofyear as core_date_week_of_year
           |       ,COALESCE(d.isoweekbegindate, d.dateId) AS core_date_iso_week_begin_date
           |       ,COALESCE(d.isoweekenddate, d.dateId) AS core_date_iso_week_end_date
           |       ,COALESCE(d.isoweeknumber, CONCAT('Week ', d.weekofyear) ) AS core_date_iso_week_number
           |       ,COALESCE(d.isoyearnumber, substring(d.dateId,1,4) ) AS core_iso_year_number
           |FROM
           |(
           |SELECT
           |        a.*
           |       ,b.dateId as send_date_filter
           |       ,b.weekenddate as send_date_week_end_date
           |       ,b.weekofyear as send_date_week_of_year
           |       ,COALESCE(b.isoweekbegindate, b.dateId) AS send_date_iso_week_begin_date
           |       ,COALESCE(b.isoweekenddate, b.dateId) AS send_date_iso_week_end_date
           |       ,COALESCE(b.isoweeknumber, CONCAT('Week ', b.weekofyear) ) AS send_date_iso_week_number
           |       ,COALESCE(b.isoyearnumber, substring(b.dateId,1,4) ) AS send_iso_year_number
           |FROM marketing_campaign_performance_stage as a
           |LEFT JOIN global_temp.calendar b ON a.send_date = b.datenum
           |) as c
           |LEFT JOIN global_temp.calendar d ON c.core_date = d.datenum
        """.stripMargin

      log.info(s"Running Query: $query")
      val marketing_campaign_performance_cal = sql(query)
      marketing_campaign_performance_cal.createOrReplaceTempView("marketing_campaign_performance_cal")
      marketing_campaign_performance_cal.persist
      marketing_campaign_performance_cal.count


      log.info(s"Rounding the USD values on marketing_campaign_performance_cal ")
      query =
        s"""
           |SELECT
           |        send_date
           |       ,core_date
           |       ,campaign_name
           |       ,source_asset_name
           |       ,marketing_channel
           |       ,marketing_subchannel
           |       ,email_type
           |       ,region
           |       ,brand_name
           |       ,campaign_type
           |       ,customer_type
           |       ,total_send_count
           |       ,total_delivered_count
           |       ,total_open_count
           |       ,unique_open_count
           |       ,total_open_rate
           |       ,unique_open_rate
           |       ,total_click_count
           |       ,unique_click_count
           |       ,total_click_rate
           |       ,unique_click_rate
           |       ,total_click_to_open_rate
           |       ,unique_click_to_open_rate
           |       ,total_bounce_count
           |       ,total_bounce_rate
           |       ,total_subscribe_count
           |       ,total_subscribe_rate
           |       ,total_unsubscribe_count
           |       ,total_unsubscribe_rate
           |       ,total_form_submit_count
           |       ,total_visit_count
           |       ,unique_visitor_count
           |       ,gross_booking_count
           |       ,cancelled_booking_count
           |       ,net_booking_count
           |       ,ROUND(gross_booking_value) AS gross_booking_value
           |       ,ROUND(cancelled_booking_value) AS cancelled_booking_value
           |       ,ROUND(net_booking_value) AS net_booking_value
           |       ,booking_request_count
           |       ,inquiry_count
           |       ,ROUND(transactional_sales_value,2) AS transactional_sales_value
           |       ,dated_search_count
           |       ,qualified_visit_count
           |       ,new_listing_count
           |       ,new_listing_pps_count
           |       ,new_listing_ppb_count
           |       ,new_listing_pps_revenue
           |       ,send_date_filter
           |       ,send_date_week_end_date
           |       ,send_date_week_of_year
           |       ,send_date_iso_week_begin_date
           |       ,send_date_iso_week_end_date
           |       ,send_date_iso_week_number
           |       ,send_iso_year_number
           |       ,core_date_filter
           |       ,core_date_week_end_date
           |       ,core_date_week_of_year
           |       ,core_date_iso_week_begin_date
           |       ,core_date_iso_week_end_date
           |       ,core_date_iso_week_number
           |       ,core_iso_year_number
           |FROM marketing_campaign_performance_cal
         """.stripMargin

      val marketing_campaign_performance = sql(query)
      marketing_campaign_performance.persist
      marketing_campaign_performance.count
      marketing_campaign_performance.createOrReplaceTempView("marketing_campaign_performance")


      log.info(s"Save marketing_campaign_performance to $targetDataLocation ")
      saveDataFrameToDisk(marketing_campaign_performance, SaveMode.Overwrite, targetDataFormat, targetDataLocation)

      log.info(s"Check and create hive table")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(marketing_campaign_performance))



      log.info("Format dataframe column names for target SQL Table")
      query =
        """
           |SELECT
           |         marketing_channel as MarketingChannel
           |        ,marketing_subchannel as MarketingSubchannel
           |        ,email_type as EmailType
           |        ,region as Region
           |        ,brand_name as BrandName
           |        ,campaign_type as CampaignType
           |        ,regexp_replace(campaign_name, '(^\\s+|\\s+$)', '') as CampaignName
           |        ,source_asset_name as SourceAssetName
           |        ,customer_type as CustomerType
           |        ,COALESCE(total_send_count,0) as TotalSendCount
           |        ,COALESCE(total_delivered_count,0) as TotalDeliveredCount
           |        ,COALESCE(total_open_count ,0) as TotalOpenCount
           |        ,COALESCE(unique_open_count,0) as UniqueOpenCount
           |        ,COALESCE(total_open_rate,0) as TotalOpenrate
           |        ,COALESCE(unique_open_rate,0) as UniqueOpenRate
           |        ,COALESCE(total_click_count,0) as TotalClickCount
           |        ,COALESCE(unique_click_count,0) as UniqueClickCount
           |        ,COALESCE(total_click_rate,0) as TotalClickRate
           |        ,COALESCE(unique_click_rate,0) as UniqueClickRate
           |        ,COALESCE(total_click_to_open_rate,0) as TotalClickToOpenRate
           |        ,COALESCE(unique_click_to_open_rate,0) as UniqueClickToOpenRate
           |        ,COALESCE(total_bounce_count,0) as TotalBounceCount
           |        ,COALESCE(total_bounce_rate,0) as TotalBounceRate
           |        ,COALESCE(total_subscribe_count,0) as TotalSubscribeCount
           |        ,COALESCE(total_subscribe_rate,0) as TotalSubscribeRate
           |        ,COALESCE(total_unsubscribe_count,0) as TotalUnsubscribeCount
           |        ,COALESCE(total_unsubscribe_rate,0) as TotalUnsubscribeRate
           |        ,COALESCE(total_form_submit_count,0) as TotalFormSubmitCount
           |        ,COALESCE(total_visit_count,0) as TotalVisitCount
           |        ,COALESCE(unique_visitor_count,0) as UniqueVisitorCount
           |        ,COALESCE(gross_booking_count,0) as GrossBookingCount
           |        ,COALESCE(cancelled_booking_count,0) as CancelledBookingCount
           |        ,COALESCE(net_booking_count,0) as NetBookingCount
           |        ,COALESCE(gross_booking_value,0) as GrossBookingValue
           |        ,COALESCE(cancelled_booking_value,0) as CancelledBookingValue
           |        ,COALESCE(net_booking_value,0) as NetBookingValue
           |        ,COALESCE(booking_request_count,0) as BookingRequestCount
           |        ,COALESCE(inquiry_count,0) as InquiryCount
           |        ,COALESCE(transactional_sales_value,0) as TransactionalSalesValue
           |        ,COALESCE(dated_search_count,0) as DatedSearchCount
           |        ,COALESCE(qualified_visit_count,0) as QualifiedVisitCount
           |        ,COALESCE(new_listing_count,0) as NewListingCount
           |        ,COALESCE(new_listing_pps_count,0) as NewListingPPSCount
           |        ,COALESCE(new_listing_ppb_count,0) as NewListingPPBCount
           |        ,COALESCE(new_listing_pps_revenue,0) as NewListingPPSRevenue
           |        ,send_date as SendDate
           |        ,core_date as CoreDate
           |        ,send_date_filter as SendDateFilter
           |        ,send_date_week_end_date as SendDateWeekEndDate
           |        ,send_date_week_of_year as SendDateWeekOfYear
           |        ,send_date_iso_week_begin_date AS SendDateISOWeekBeginDate
           |        ,send_date_iso_week_end_date AS SendDateISOWeekEndDate
           |        ,send_date_iso_week_number AS SendDateISOWeekNumber
           |        ,send_iso_year_number AS SendISOYearNumber
           |        ,core_date_filter as CoreDateFilter
           |        ,core_date_week_end_date as CoreDateWeekEndDate
           |        ,core_date_week_of_year as CoreDateWeekOfYear
           |        ,core_date_iso_week_begin_date AS CoreDateISOWeekBeginDate
           |        ,core_date_iso_week_end_date AS CoreDateISOWeekEndDate
           |        ,core_date_iso_week_number AS CoreDateISOWeekNumber
           |        ,core_iso_year_number AS CoreISOYearNumber
           |FROM marketing_campaign_performance
        """.stripMargin

      log.info(s"Running Query: $query")
      val marketing_campaign_performance_sql = sql(query)
      marketing_campaign_performance_sql.persist
      marketing_campaign_performance_sql.count
      marketing_campaign_performance_sql.createOrReplaceTempView("marketing_campaign_performance_sql")


      log.info(s"Delete from SQL table  $sqlTargetDB.$sqlTargetTable")
      sqlObj.executeUpdate(s"DELETE FROM $sqlTargetDB.$sqlTargetTable ")

      log.info(s"Write to SQL table  $sqlTargetDB.$sqlTargetTable")
      edwLoader.writeData(marketing_campaign_performance_sql,sqlTargetDB,sqlTargetTable)



      log.info("Validate against Source: Eloqua and Core KPI's")
      log.info("Metrics from Sources: Campaign Send Fact, Core KPI's, RLT")
      query =
        """
          |SELECT
          |        a.*
          |       ,b.gross_booking_count
          |       ,b.cancelled_booking_count
          |       ,b.net_booking_count
          |       ,b.gross_booking_value
          |       ,b.cancelled_booking_value
          |       ,b.net_booking_value
          |       ,b.booking_request_count
          |       ,b.inquiry_count
          |       ,b.transactional_sales_value
          |       ,c.dated_search_count
          |       ,c.qualified_visit_count
          |       ,c.total_visit_count
          |FROM (
          |SELECT
          |      'source_metrics' AS table
          |     ,CAST(SUM(send_count) AS string) AS total_send_count
          |     ,CAST(SUM(total_open_count) AS string) AS total_open_count
          |     ,CAST(SUM(bounce_count) AS string) AS total_bounce_count
          |     ,CAST(SUM(total_click_count) AS string) AS total_click_count
          |     ,CAST(SUM(subscribe_count) AS string) AS total_subscribe_count
          |     ,CAST((SUM(unsubscribe_count)+SUM(form_submit_count)) AS string) AS total_unsubscribe_count
          |     ,CAST(SUM(unique_open_count) AS string) AS unique_open_count
          |     ,CAST(SUM(unique_click_count) AS string) AS unique_click_count
          |     ,CAST(SUM(form_submit_count) AS string) AS total_form_submit_count
          |FROM marketing_activity_send_fact
          |) a
          |JOIN
          |(
          |SELECT
          |      'source_metrics' AS table
          |     ,CAST(SUM(grossBookingCount) AS string) AS gross_booking_count
          |     ,CAST(SUM(cancelledBookingCount) AS string) AS cancelled_booking_count
          |     ,CAST(SUM(netBookingCount) AS string) AS net_booking_count
          |     ,CAST(SUM(grossBookingValueAmountUSD) AS string) AS gross_booking_value
          |     ,CAST(SUM(cancelledGrossBookingValueAmountUSD) AS string) AS cancelled_booking_value
          |     ,CAST(SUM(netGrossBookingValueAmountUSD) AS string) AS net_booking_value
          |     ,CAST(SUM(bookingRequestCount) AS string) AS booking_request_count
          |     ,CAST(SUM(inquiryCount) AS string) AS inquiry_count
          |     ,CAST(SUM(transactionalSalesTotalAmountUSD) AS string) AS transactional_sales_value
          |     ,CAST(SUM(new_listing_count) AS string) AS new_listing_count
          |FROM core
          |) b
          |ON a.table = b.table
          |JOIN
          |(
          |SELECT
          |      'source_metrics' AS table
          |     ,CAST(SUM(datedSearch) AS string) AS dated_search_count
          |     ,CAST(SUM(qualifiedVisit) AS string) AS qualified_visit_count
          |     ,CAST(SUM(totalVisits) AS string) AS total_visit_count
          |FROM rlt
          |) c
          |ON b.table = c.table
        """.stripMargin

      log.info(s"Running Query: $query")
      val source_metrics_tmp = sql(query)
      source_metrics_tmp.persist
      source_metrics_tmp.count


      log.info("Final Metrics")
      query =
        """
          |SELECT
          |      'final_metrics' AS table
          |     ,CAST(SUM(total_send_count) AS string) AS total_send_count
          |     ,CAST(SUM(total_open_count) AS string) AS total_open_count
          |     ,CAST(SUM(total_bounce_count) AS string) AS total_bounce_count
          |     ,CAST(SUM(total_click_count) AS string) AS total_click_count
          |     ,CAST(SUM(total_subscribe_count) AS string) AS total_subscribe_count
          |     ,CAST(SUM(total_unsubscribe_count) AS string) AS total_unsubscribe_count
          |     ,CAST(SUM(unique_open_count) AS string) AS unique_open_count
          |     ,CAST(SUM(unique_click_count) AS string) AS unique_click_count
          |     ,CAST(SUM(total_form_submit_count) AS string) AS total_form_submit_count
          |     ,CAST(SUM(gross_booking_count) AS string) AS gross_booking_count
          |     ,CAST(SUM(cancelled_booking_count) AS string) AS cancelled_booking_count
          |     ,CAST(SUM(net_booking_count) AS string) AS net_booking_count
          |     ,CAST(SUM(gross_booking_value) AS string) AS gross_booking_value
          |     ,CAST(SUM(cancelled_booking_value) AS string) AS cancelled_booking_value
          |     ,CAST(SUM(net_booking_value) AS string) AS net_booking_value
          |     ,CAST(SUM(booking_request_count) AS string) AS booking_request_count
          |     ,CAST(SUM(inquiry_count) AS string) AS inquiry_count
          |     ,CAST(SUM(transactional_sales_value) AS string) AS transactional_sales_value
          |     ,CAST(SUM(dated_search_count) AS string) AS dated_search_count
          |     ,CAST(SUM(qualified_visit_count) AS string) AS qualified_visit_count
          |     ,CAST(SUM(total_visit_count) AS string) AS total_visit_count
          |     ,CAST(SUM(new_listing_count) AS string) AS new_listing_count
          |FROM marketing_campaign_performance_cal
        """.stripMargin

      log.info(s"Running Query: $query")
      val marketing_campaign_performance_metrics = sql(query)
      marketing_campaign_performance_metrics.persist
      marketing_campaign_performance_metrics.count


      val source_metrics = transposeUDF(source_metrics_tmp, Seq("table"))
      source_metrics.createOrReplaceTempView("source_metrics")

      val final_metrics = transposeUDF(marketing_campaign_performance_metrics, Seq("table"))
      final_metrics.createOrReplaceTempView("final_metrics")

      query =
        s"""
           |SELECT
           |      a.column_name AS metric
           |     ,a.column_value AS source_metrics
           |     ,b.column_value AS final_metrics
           |     ,(CAST(a.column_value AS Decimal(38,18)) - CAST(b.column_value AS Decimal(38,18))) AS ${conf("thresholdColumn")}
           |FROM source_metrics a JOIN final_metrics b
           |ON a.column_name = b.column_name
           |WHERE ABS(CAST(a.column_value AS Decimal(38,18)) - CAST(b.column_value AS Decimal(38,18))) > ${conf("icp.sourceValidationErrorThreshold")}
        """.stripMargin
      log.info(s"Running Query: $query")
      val final_metrics_diff = sql(query)
      final_metrics_diff.columns


      if (final_metrics_diff.count > 0) {
        val htmlBody = final_metrics_diff.collect.map { row =>
          s"<tr bgcolor=${conf("red")}><td>" + row.toString.stripPrefix("[").stripSuffix("]").split(",").mkString("</td><td>") + "</td></tr>"
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
             |    <th>Source_Metrics</th>
             |    <th>Final_Metrics</th>
             |    <th>Diff</th>
             |  </tr>
             |  $htmlBody
             |</table>
           """.stripMargin

        send a Mail(
          from = conf("from") -> conf("fromName"),
          to = conf("to").split(","),
          subject = conf("icp.sourceValidationSubject"),
          message = conf("icp.message"),
          richMessage = Option(rMsg)
        )
      }


      log.info("Validate against HeartBeatRLT")
      query =
        """
          |SELECT
          |      Year(DateId) AS year
          |     ,CAST(SUM(VisitCount) AS varchar) AS total_visit_count
          |     ,CAST(SUM(InquiryCount) AS varchar) AS inquiry_count
          |     ,CAST(SUM(BookingRequestCount) AS varchar) AS booking_request_count
          |     ,CAST(SUM(BookingsGrossCount) AS varchar) AS gross_booking_count
          |     ,CAST(SUM(BookingsCancelledCount) AS varchar) AS cancelled_booking_count
          |     ,CAST(SUM(BookingsGrossCount - BookingsCancelledCount) AS varchar) AS net_booking_count
          |     ,CAST(SUM(BookingValueGrossAmountUSD) AS varchar) AS gross_booking_value
          |     ,CAST(SUM(BookingValueCancelledAmountUSD) AS varchar) AS cancelled_booking_value
          |     ,CAST(SUM(BookingValueGrossAmountUSD - BookingValueCancelledAmountUSD) AS varchar) AS net_booking_value
          |     ,CAST(SUM(TransactionalSalesAmountUSD) AS varchar) AS transactional_sales_value
          |     ,CAST(SUM(NewEnabledListingCount) AS varchar) AS new_listing_count
          |FROM dw_facts.dbo.HeartbeatDailyFact_RLT
          |WHERE MarketingChannel='Email' AND MarketingSubChannel='Email Marketing'
          |GROUP BY Year(DateId)
        """.stripMargin
      val hb = edwLoader.getData(query)
      hb.persist
      hb.count

      query =
        """
          |SELECT
          |      core_iso_year_number AS year
          |     ,CAST(SUM(total_visit_count) AS string) AS total_visit_count
          |     ,CAST(SUM(inquiry_count) AS string) AS inquiry_count
          |     ,CAST(SUM(booking_request_count) AS string) AS booking_request_count
          |     ,CAST(SUM(gross_booking_count) AS string) AS gross_booking_count
          |     ,CAST(SUM(cancelled_booking_count) AS string) AS cancelled_booking_count
          |     ,CAST(SUM(net_booking_count) AS string) AS net_booking_count
          |     ,CAST(SUM(gross_booking_value) AS string) AS gross_booking_value
          |     ,CAST(SUM(cancelled_booking_value) AS string) AS cancelled_booking_value
          |     ,CAST(SUM(net_booking_value) AS string) AS net_booking_value
          |     ,CAST(SUM(transactional_sales_value) AS string) AS transactional_sales_value
          |     ,CAST(SUM(new_listing_count) AS string) AS new_listing_count
          |FROM marketing_campaign_performance
          |WHERE core_iso_year_number >= 2016 AND core_iso_year_number <= 2018 GROUP BY 1
        """.stripMargin

      val im = sql(query)
      im.persist
      im.count


      val hb_metrics = transposeUDF(hb, Seq("year"))
      hb_metrics.createOrReplaceGlobalTempView("hb_metrics")

      val im_metrics = transposeUDF(im, Seq("year"))
      im_metrics.createOrReplaceTempView("im_metrics")

      query =
        s"""
           |SELECT
           |      a.year
           |     ,a.column_name AS metric
           |     ,a.column_value AS core
           |     ,b.column_value AS im
           |     ,ROUND(((CAST(a.column_value AS Double) - CAST(b.column_value AS Double))/CAST(a.column_value AS Double)) * 100, 2) AS ${conf("thresholdColumn")}
           |FROM global_temp.hb_metrics a JOIN im_metrics b
           |ON a.year = b.year AND a.column_name = b.column_name
           |ORDER BY year DESC
        """.stripMargin
      val df = sql(query)
      df.columns


      val htmlBody = df.collect.map { row => val value = row.getAs(conf("thresholdColumn")).asInstanceOf[Double].abs; (if (value > conf("icp.errorThreshold").toInt) s"<tr bgcolor=${conf("red")}><td>" else if (value > conf("icp.warningThreshold").toInt) s"<tr bgcolor=${conf("yellow")}><td>" else s"<tr bgcolor=${conf("green")}><td>") + row.toString.stripPrefix("[").stripSuffix("]").split(",").mkString("</td><td>") + "</td></tr>" }.mkString

      val rMsg =
        s"""
           |<style>
           |table, th, td {
           |    border-collapse: collapse;
           |    border: 2px solid black;
           |}
           |</style>
           |
           |<table>
           |  <tr bgcolor=${conf("blue")}>
           |    <th>Year</th>
           |    <th>Metric</th>
           |    <th>Core</th>
           |    <th>B&IM</th>
           |    <th>% Diff</th>
           |  </tr>
           |  $htmlBody
           |</table>
      """.stripMargin

      send a Mail(
        from = conf("from") -> conf("fromName"),
        to = conf("to").split(","),
        subject = conf("icp.subject"),
        message = conf("icp.message"),
        richMessage = Option(rMsg)
      )

      jc.logInfo(campaignInstanceId, "END Integrated Campaign Performance Process", -1)

      jc.endJob(campaignInstanceId, 1, startTimestamp, endTimestamp)

      jc.endJob(instanceId, 1)

    } catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the integrated campaign performance load run for Instance: $campaignInstanceId")
        jc.endJob(campaignInstanceId, -1)
        throw e
    }

  }

}


