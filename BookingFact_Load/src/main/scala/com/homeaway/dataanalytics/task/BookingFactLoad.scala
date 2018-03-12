package com.homeaway.dataanalytics.task

/**
  * Created by aguyyala on 5/28/17.
  */

import org.springframework.boot.{ApplicationArguments, ApplicationRunner}
import org.springframework.context.annotation.ImportResource
import org.springframework.core.annotation.Order
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types._
import com.homeaway.dataanalytics.AnalyticstaskApp._
import com.homeaway.dataanalytics.encrypt.main.utilities.Utility
import com.homeaway.dataanalytics.encrypt.main.loader.EDWLoader


@Order(1)
@ImportResource(Array("classpath:sparkContext.xml"))
class BookingFactLoad extends ApplicationRunner with Utility {

	override def run(args: ApplicationArguments) {
		setConf(args, spark)
		val checkPointDir = dfs.getHomeDirectory.toString + "/" + spark.sparkContext.applicationId
		spark.sparkContext.setCheckpointDir(checkPointDir)

		val edwLoader = new EDWLoader(spark)

		val parsedDate = new SimpleDateFormat("yyyy-MM-dd")
		val parsedDateInt = new SimpleDateFormat("yyyyMMdd")

		val finalFormat = spark.conf.get("finalFormat")
		val finalBaseLoc = spark.conf.get("finalLocation")
		val finalLocation = spark.conf.get("finalLocation") + "/" + parsedDateInt.format(Calendar.getInstance.getTime)
		val tmpLocation = spark.conf.get("tmpLocation")
		val debugLocation = spark.conf.get("debugLocation")
		val targetDB = spark.conf.get("targetDB")
		val targetTable = spark.conf.get("targetTable")

		logSparkConf(spark)

		val sql = spark.sql _

		val cal = Calendar.getInstance()
		var currentDate = parsedDate.format(cal.getTime)
		cal.add(Calendar.DATE, -3)
		val lsrstartDate = lsr.getOrElse("lastSuccessfulWaterMarkStartDate", parsedDate.format(cal.getTime))
		cal.setTime(parsedDate.parse(lsrstartDate))
		cal.add(Calendar.DATE, 1)
		var startDate = parsedDate.format(cal.getTime)
		var startDateInt = parsedDateInt.format(cal.getTime)

		val endDate = "9999-12-31"

		val twoYearsBack = Calendar.getInstance()
		twoYearsBack.set(twoYearsBack.get(Calendar.YEAR) - 2, 0, 1)
		val rollingTwoYears = parsedDate.format(twoYearsBack.getTime)

		val sixMonthsBackCal = Calendar.getInstance()
		sixMonthsBackCal.setTime(cal.getTime)
		sixMonthsBackCal.add(Calendar.MONTH, -6)
		val sixMonthsBack = parsedDateInt.format(sixMonthsBackCal.getTime)

		val fourDaysBackCal = Calendar.getInstance()
		fourDaysBackCal.setTime(fourDaysBackCal.getTime)
		fourDaysBackCal.add(Calendar.DATE, -3)
		val bookingFactRetentionDays = parsedDateInt.format(fourDaysBackCal.getTime)

		/* Check the startDate argument if passed in spark */
		if (args.getOptionValues("startDate") != null) {

			cal.setTime(parsedDate.parse(args.getOptionValues("startDate").get(0)))
			startDate = parsedDate.format(cal.getTime)
			startDateInt = parsedDateInt.format(cal.getTime)
			cal.add(Calendar.DATE, 1)
			currentDate = parsedDate.format(cal.getTime)

		}

		try {

			if (isRun(args)) {

				jc.logInfo(jobId, instanceId, s"START BookingFact Process - startDate: $startDate ; endDate: $endDate", -1)


				val sqlBookingfact = s"SELECT * FROM $targetDB.$targetTable"

				log.info(s"Running Query: $sqlBookingfact")
				val dfBookingfact = spark.read.format(finalFormat).load(getHiveTableLocation(hiveMetaStore, targetDB, targetTable))

				dfBookingfact.persist

				dfBookingfact.createOrReplaceTempView("tmpBookingfact")

				//		val sqlBookingfact = s"SELECT * FROM dw_facts.dbo.bookingfact"
				//
				//		log.info(s"Running Query: $sqlBookingfact")
				//		val dfBookingfact = edwLoader.getEDWData(sqlBookingfact, 40)
				//
				//		dfBookingfact.persist
				//
				//		dfBookingfact.createOrReplaceTempView("tmpBookingfact")


				val sqlReservation = s"SELECT reservationguid, bookingcategoryid, departuredate, reservationpaymentstatustypeid, traveleremailid, brandid, reservationid, createdate, inquiryserviceentryguid, arrivaldate, cancelleddate, activeflag, dwupdatedatetime, devicecategorysessionid, siteid, fullvisitorid, bookingdate, visitorid, websitereferralmediumsessionid, reservationavailabilitystatustypeid, regionid, listingunitid, visitid, bookingreporteddate, onlinebookingprovidertypeid FROM dw.dbo.reservation"

				log.info(s"Running Query: $sqlReservation")
				val dfReservation = edwLoader.getEDWData(sqlReservation, 40)

				dfReservation.persist

				dfReservation.createOrReplaceTempView("tmpReservation")


				val sqlQuoteitem = s"SELECT quoteitemid, name, quoteitemtype, taxamount FROM dw.dbo.quoteitem with (NOLOCK)"

				log.info(s"Running Query: $sqlQuoteitem")
				val dfQuoteitem = edwLoader.getEDWData(sqlQuoteitem, 40)

				dfQuoteitem.persist

				dfQuoteitem.createOrReplaceTempView("tmpQuoteitem")


				val sqlQuote = s"SELECT reservationid, quoteguid, currencycode, quoteid, bookingtypeid, calculatedtaxamount, paymentmethodtypeid, taxamount, dwlastupdatedate FROM dw.dbo.quote with (NOLOCK)"

				log.info(s"Running Query: $sqlQuote")
				val dfQuote = edwLoader.getEDWData(sqlQuote, 40)

				dfQuote.createOrReplaceTempView("tmpQuote")


				val sqlQuotefact = s"SELECT paidamountlocalcurrency, dwcreatedate, reservationid, currencycode, displayregionid, refundedamountlocalcurrency, quoteid, quotestatusid, quoteitemactiveflag, quotecreateddate, quoteitemid, quoteitemcreateddate, amountlocalcurrency, fullvisitorid, websitereferralmediumsessionid, visitid, dwlastupdatedate, travelerproductid, quoteactiveflag FROM dw.dbo.quotefact with (NOLOCK)"

				log.info(s"Running Query: $sqlQuotefact")
				val dfQuotefact = edwLoader.getEDWData(sqlQuotefact, 40)

				dfQuotefact.persist

				dfQuotefact.createOrReplaceTempView("tmpQuotefact")


				val sqlTravelerproduct = s"SELECT travelerproductid, quoteitemtype, refquoteitemdescription FROM dw.dbo.travelerproduct with (NOLOCK)"

				log.info(s"Running Query: $sqlTravelerproduct")
				val dfTravelerproduct = edwLoader.getEDWData(sqlTravelerproduct)

				dfTravelerproduct.persist

				dfTravelerproduct.createOrReplaceTempView("tmpTravelerproduct")


				val sqlTravelerorderpayment = s"SELECT travelerorderpaymenttypename, travelerorderpaymentid, travelerorderpaymentstatusname, paymentmethodtypeid, travelerorderid, currencycode, paymentdate, amount FROM dw_traveler.dbo.travelerorderpayment with (NOLOCK)"

				log.info(s"Running Query: $sqlTravelerorderpayment")
				val dfTravelerorderpayment = edwLoader.getEDWData(sqlTravelerorderpayment)

				dfTravelerorderpayment.persist

				dfTravelerorderpayment.createOrReplaceTempView("tmpTravelerorderpayment")


				val sqlInquiry = s"SELECT inquiryserviceentryguid, listingid, inquiryid FROM dw.dbo.inquiry with (NOLOCK)"

				log.info(s"Running Query: $sqlInquiry")
				val dfInquiry = edwLoader.getEDWData(sqlInquiry, 40)

				dfInquiry.createOrReplaceTempView("tmpInquiry")


				val sqlTravelerorderpaymentdistributiontype = s"SELECT travelerorderpaymentdistributiontypeid, travelerorderpaymentdistributiontypename FROM dw_traveler.dbo.travelerorderpaymentdistributiontype with (NOLOCK)"

				log.info(s"Running Query: $sqlTravelerorderpaymentdistributiontype")
				val dfTravelerorderpaymentdistributiontype = edwLoader.getData(sqlTravelerorderpaymentdistributiontype)

				dfTravelerorderpaymentdistributiontype.persist

				dfTravelerorderpaymentdistributiontype.createOrReplaceTempView("tmpTravelerorderpaymentdistributiontype")


				val sqlPaymentmethodtype = s"SELECT paymentcollectiontype, paymentmethodtypeid FROM dw_traveler.dbo.paymentmethodtype with (NOLOCK)"

				log.info(s"Running Query: $sqlPaymentmethodtype")
				val dfPaymentmethodtype = edwLoader.getEDWData(sqlPaymentmethodtype)

				dfPaymentmethodtype.persist

				dfPaymentmethodtype.createOrReplaceTempView("tmpPaymentmethodtype")


				val sqlInquiryslafact = s"SELECT inquiryid, listingid, fullvisitorid, websitereferralmediumsessionid, visitid, inquirydate FROM dw.dbo.inquiryslafact with (NOLOCK)"

				log.info(s"Running Query: $sqlInquiryslafact")
				val dfInquiryslafact = edwLoader.getEDWData(sqlInquiryslafact, 40)

				dfInquiryslafact.persist

				dfInquiryslafact.createOrReplaceTempView("tmpInquiryslafact")


				val sqlCurrency = s"SELECT currencyid, currencycode FROM dw.dbo.currency with (NOLOCK)"

				log.info(s"Running Query: $sqlCurrency")
				val dfCurrency = edwLoader.getEDWData(sqlCurrency)

				dfCurrency.persist

				dfCurrency.createOrReplaceTempView("tmpCurrency")


				val sqlWebsitereferralmediumsession = s"SELECT marketingmedium, websitereferralmediumsessionid, websitereferralmedium FROM dw.dbo.websitereferralmediumsession with (NOLOCK)"

				log.info(s"Running Query: $sqlWebsitereferralmediumsession")
				val dfWebsitereferralmediumsession = edwLoader.getEDWData(sqlWebsitereferralmediumsession)

				dfWebsitereferralmediumsession.persist

				dfWebsitereferralmediumsession.createOrReplaceTempView("tmpWebsitereferralmediumsession")


				val sqlCustomerattributes = s"SELECT customerattributesid, rowstartdate, customerid, persontype, rowenddate FROM dw.dbo.customerattributes with (NOLOCK)"

				log.info(s"Running Query: $sqlCustomerattributes")
				val dfCustomerattributes = edwLoader.getEDWData(sqlCustomerattributes)

				dfCustomerattributes.persist

				dfCustomerattributes.createOrReplaceTempView("tmpCustomerattributes")


				val sqlCustomer = s"SELECT customerid, advertiseruuid FROM dw.dbo.customer with (NOLOCK)"

				log.info(s"Running Query: $sqlCustomer")
				val dfCustomer = edwLoader.getEDWData(sqlCustomer)

				dfCustomer.persist

				dfCustomer.createOrReplaceTempView("tmpCustomer")


				val sqlListingunitattributes = s"SELECT listingunitattributesid, listingunitid, rowstartdate, rowenddate FROM dw.dbo.listingunitattributes with (NOLOCK)"

				log.info(s"Running Query: $sqlListingunitattributes")
				val dfListingunitattributes = edwLoader.getEDWData(sqlListingunitattributes)

				dfListingunitattributes.persist

				dfListingunitattributes.createOrReplaceTempView("tmpListingunitattributes")


				val sqlProduct = s"SELECT productid, productguid, grossbookingvalueproductcategory FROM dw_traveler.dbo.product with (NOLOCK)"

				log.info(s"Running Query: $sqlProduct")
				val dfProduct = edwLoader.getData(sqlProduct)

				dfProduct.persist

				dfProduct.createOrReplaceTempView("tmpProduct")


				val sqlOnlinebookingprovidertype = s"SELECT onlinebookingprovidertypeid, olbprovidergatewaytypedescription, listingsourcename FROM dw.dbo.onlinebookingprovidertype with (NOLOCK)"

				log.info(s"Running Query: $sqlOnlinebookingprovidertype")
				val dfOnlinebookingprovidertype = edwLoader.getEDWData(sqlOnlinebookingprovidertype)

				dfOnlinebookingprovidertype.persist

				dfOnlinebookingprovidertype.createOrReplaceTempView("tmpOnlinebookingprovidertype")


				val sqlPersontype = s"SELECT persontypeid, persontypename FROM dw.dbo.persontype with (NOLOCK)"

				log.info(s"Running Query: $sqlPersontype")
				val dfPersontype = edwLoader.getData(sqlPersontype)

				dfPersontype.persist

				dfPersontype.createOrReplaceTempView("tmpPersontype")


				val sqlListingunit = s"SELECT listingunituuid, rentalunitid, customerid, listingid, regionid, listingunitid, paymenttypeid FROM dw.dbo.listingunit with (NOLOCK)"

				log.info(s"Running Query: $sqlListingunit")
				val dfListingunit = edwLoader.getEDWData(sqlListingunit)

				dfListingunit.persist

				dfListingunit.createOrReplaceTempView("tmpListingunit")


				val sqlTravelerorderpaymentdistributionamounttype = s"SELECT travelerorderpaymentdistributionamounttypeid, travelerorderpaymentdistributionamounttypename FROM dw_traveler.dbo.travelerorderpaymentdistributionamounttype with (NOLOCK)"

				log.info(s"Running Query: $sqlTravelerorderpaymentdistributionamounttype")
				val dfTravelerorderpaymentdistributionamounttype = edwLoader.getData(sqlTravelerorderpaymentdistributionamounttype)

				dfTravelerorderpaymentdistributionamounttype.persist

				dfTravelerorderpaymentdistributionamounttype.createOrReplaceTempView("tmpTravelerorderpaymentdistributionamounttype")


				val sqlChannel = s"SELECT channelid, channelname, listingsource FROM dw.dbo.channel with (NOLOCK)"

				log.info(s"Running Query: $sqlChannel")
				val dfChannel = edwLoader.getData(sqlChannel)

				dfChannel.persist

				dfChannel.createOrReplaceTempView("tmpChannel")


				val sqlTravelerorderitem = s"SELECT travelerorderid, currencycode, productid, amount, travelerorderitemstatus, travelerorderitemid FROM dw_traveler.dbo.travelerorderitem with (NOLOCK)"

				log.info(s"Running Query: $sqlTravelerorderitem")
				val dfTravelerorderitem = edwLoader.getEDWData(sqlTravelerorderitem)

				dfTravelerorderitem.persist

				dfTravelerorderitem.createOrReplaceTempView("tmpTravelerorderitem")


				val sqlProductfulfillment = s"SELECT ExternalRefId, ExternalRefType, ProductTypeGuid FROM DW_Traveler.dbo.ProductFulfillment with (NOLOCK)"

				log.info(s"Running Query: $sqlProductfulfillment")
				val dfProductfulfillment = edwLoader.getEDWData(sqlProductfulfillment)

				dfProductfulfillment.createOrReplaceTempView("tmpProductfulfillment")


				val sqlVw_quotegrossbookingvalue =
					"""
					  |SELECT qf.ReservationId ,
					  |       qf.CurrencyCode ,
					  |       qf.QuoteId ,
					  |       qf.QuoteCreatedDate ,
					  |       qf.QuoteStatusId ,
					  |       q.PaymentMethodTypeId ,
					  |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType NOT IN ('COMMISSION', 'OFFLINE_RDD', 'DEPOSIT', 'CSA_PDP', 'MANUAL_PDP', 'Product', 'PROTECTION', 'Stay_Tax_Fee', 'TAX', 'Traveler Fee') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS RentalAmount,
					  |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType IN ('DEPOSIT', 'OFFLINE_RDD') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS RefundableDamageDepositAmount,
					  |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType = 'Traveler Fee' THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS ServiceFeeAmount,
					  |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType IN ('Stay_Tax_Fee', 'TAX') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) + coalesce(q.CalculatedTaxAmount,0) AS TaxAmount,
					  |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType IN ('PROTECTION','Product', 'MANUAL_PDP', 'CSA_PDP') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS VasAmount,
					  |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType = 'Commission' THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) AS CommissionAmount,
					  |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType NOT IN ('Commission') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) + coalesce(q.CalculatedTaxAmount, 0) AS OrderAmount,
					  |       SUM(qf.PaidAmountLocalCurrency) AS PaidAmount,
					  |       SUM(qf.RefundedAmountLocalCurrency) AS RefundAmount,
					  |       SUM(CASE WHEN qf.QuoteItemActiveFlag = 1 THEN CASE WHEN qi.AdjustedQuoteItemType NOT IN ('DEPOSIT', 'OFFLINE_RDD', 'Commission') THEN qf.AmountLocalCurrency ELSE 0 END ELSE 0 END) + coalesce(q.CalculatedTaxAmount, 0) AS BookingValue
					  |FROM tmpQuote q
					  |JOIN tmpQuoteFact qf ON qf.QuoteId = q.QuoteId
					  |JOIN (
					  |       SELECT qi2.QuoteItemId ,
					  |              CASE WHEN qi2.Name IN ('Booking Commission', 'IPM Booking Commission', 'PPS IPM Booking Commission') THEN 'COMMISSION' WHEN (tp.refQuoteItemDescription IN ('Service Fee', 'Traveler Fee')) THEN 'Traveler Fee' ELSE qi2.QuoteItemType END AS AdjustedQuoteItemType
					  |       FROM tmpQuoteItem qi2
					  |       JOIN tmpquotefact qf ON qi2.QuoteItemId = qf.QuoteItemId
					  |       JOIN tmpTravelerProduct tp ON qf.TravelerProductId=tp.TravelerProductId
					  |     ) qi ON qi.QuoteItemId = qf.QuoteItemId
					  |WHERE 1 = 1 AND qf.QuoteActiveFlag = 1 AND qf.AmountLocalCurrency < 999999999999 and qf.QuoteCreatedDate >='2013-01-01' /* these are TEST quotes that skew the data */
					  |GROUP BY qf.ReservationId ,
					  | qf.CurrencyCode ,
					  | qf.QuoteId ,
					  | qf.QuoteCreatedDate ,
					  | qf.QuoteStatusId ,
					  | q.PaymentMethodTypeId ,
					  | q.CalculatedTaxAmount
					""".stripMargin

				log.info(s"Running Query: $sqlVw_quotegrossbookingvalue")
				val dfVw_quotegrossbookingvalue = sql(sqlVw_quotegrossbookingvalue)

				dfVw_quotegrossbookingvalue.persist

				dfVw_quotegrossbookingvalue.createOrReplaceTempView("tmpVw_quotegrossbookingvalue")


				val sqlTravelerorderpaymentdistribution = s"SELECT travelerorderpaymentid, travelerorderitemid, updatedate, distributedamount, travelerorderpaymentdistributiontypeid, travelerorderpaymentdistributiontoaccounttypeid, travelerorderpaymentdistributionamounttypeid FROM dw_traveler.dbo.travelerorderpaymentdistribution with (NOLOCK)"

				log.info(s"Running Query: $sqlTravelerorderpaymentdistribution")
				val dfTravelerorderpaymentdistribution = edwLoader.getEDWData(sqlTravelerorderpaymentdistribution)

				dfTravelerorderpaymentdistribution.persist

				dfTravelerorderpaymentdistribution.createOrReplaceTempView("tmpTravelerorderpaymentdistribution")


				val sqlBookingvaluesource = s"SELECT * FROM dw.dbo.bookingvaluesource with (NOLOCK)"

				log.info(s"Running Query: $sqlBookingvaluesource")
				val dfBookingvaluesource = edwLoader.getData(sqlBookingvaluesource)

				dfBookingvaluesource.persist

				dfBookingvaluesource.createOrReplaceTempView("tmpBookingvaluesource")


				val sqlVisitorfact = s"SELECT DISTINCT dateid, source, visitdate, fullvisitorid, visitstarttime, visitid FROM bq_exporter.visitorfact WHERE env='prod' and pdateid >= $sixMonthsBack"

				log.info(s"Running Query: $sqlVisitorfact")
				val dfVisitorfact = sql(sqlVisitorfact).repartition(100)

				dfVisitorfact.persist

				dfVisitorfact.createOrReplaceTempView("tmpVisitorfact")


				val sqlBrand = s"SELECT reportingcurcode, brandid FROM dw.dbo.brand with (NOLOCK)"

				log.info(s"Running Query: $sqlBrand")
				val dfBrand = edwLoader.getEDWData(sqlBrand)

				dfBrand.persist

				dfBrand.createOrReplaceTempView("tmpBrand")


				val sqlTravelerorder = s"SELECT reservationid, travelerorderid, quoteid FROM dw_traveler.dbo.travelerorder with (NOLOCK)"

				log.info(s"Running Query: $sqlTravelerorder")
				val dfTravelerorder = edwLoader.getEDWData(sqlTravelerorder)

				dfTravelerorder.persist

				dfTravelerorder.createOrReplaceTempView("tmpTravelerorder")


				val sqlAverageknownnightlybookingvalue = s"SELECT * FROM dw.dbo.averageknownnightlybookingvalue with (NOLOCK)"

				log.info(s"Running Query: $sqlAverageknownnightlybookingvalue")
				val dfAverageknownnightlybookingvalue = edwLoader.getEDWData(sqlAverageknownnightlybookingvalue)

				dfAverageknownnightlybookingvalue.persist

				dfAverageknownnightlybookingvalue.createOrReplaceTempView("tmpAverageknownnightlybookingvalue")


				val sqlTravelerorderpaymentdistributiontoaccounttype = s"SELECT travelerorderpaymentdistributiontoaccounttypeid, travelerorderpaymentdistributiontoaccounttypename FROM dw_traveler.dbo.travelerorderpaymentdistributiontoaccounttype with (NOLOCK)"

				log.info(s"Running Query: $sqlTravelerorderpaymentdistributiontoaccounttype")
				val dfTravelerorderpaymentdistributiontoaccounttype = edwLoader.getData(sqlTravelerorderpaymentdistributiontoaccounttype)

				dfTravelerorderpaymentdistributiontoaccounttype.persist

				dfTravelerorderpaymentdistributiontoaccounttype.createOrReplaceTempView("tmpTravelerorderpaymentdistributiontoaccounttype")


				val sqlBrandattributes = s"SELECT brandattributesid, brandid, rowstartdate, rowenddate FROM dw.dbo.brandattributes with (NOLOCK)"

				log.info(s"Running Query: $sqlBrandattributes")
				val dfBrandattributes = edwLoader.getEDWData(sqlBrandattributes)

				dfBrandattributes.persist

				dfBrandattributes.createOrReplaceTempView("tmpBrandattributes")


				val sqlCurrencyconversion = s"SELECT tocurrency, rowstartdate, fromcurrency, rowenddate, currencyconversionid, conversionrate FROM dw.dbo.currencyconversion with (NOLOCK)"

				log.info(s"Running Query: $sqlCurrencyconversion")
				val dfCurrencyconversion = edwLoader.getEDWData(sqlCurrencyconversion)

				dfCurrencyconversion.persist

				dfCurrencyconversion.createOrReplaceTempView("tmpCurrencyconversion")


				val sqlBookingCategory = s"SELECT bookingcategoryid, bookingindicator, knownbookingindicator FROM dw.dbo.bookingcategory with (NOLOCK)"

				log.info(s"Running Query: $sqlBookingCategory")
				val dfBookingCategory = edwLoader.getData(sqlBookingCategory)

				dfBookingCategory.persist

				dfBookingCategory.createOrReplaceTempView("tmpBookingCategory")


				val sqlListingattributes = s"SELECT listingattributesid, listingid, rowstartdate, rowenddate FROM dw.dbo.listingattributes with (NOLOCK)"

				log.info(s"Running Query: $sqlListingattributes")
				val dfListingattributes = edwLoader.getEDWData(sqlListingattributes, 40)

				dfListingattributes.createOrReplaceTempView("tmpListingattributes")


				val sqlCalendar = s"SELECT quarterenddate, yearnumber, dateid FROM dw.dbo.calendar with (NOLOCK)"

				log.info(s"Running Query: $sqlCalendar")
				val dfCalendar = edwLoader.getData(sqlCalendar)

				dfCalendar.persist

				dfCalendar.createOrReplaceTempView("tmpCalendar")


				val sqlListing = s"SELECT appid, rentalid, apiuuid, rentalnumber, city, strategicdestinationid, country, listingid, postalcode, subscriptionid, paymenttypeid, bedroomnum FROM dw.dbo.listing with (NOLOCK)"

				log.info(s"Running Query: $sqlListing")
				val dfListing = edwLoader.getEDWData(sqlListing)

				dfListing.persist

				dfListing.createOrReplaceTempView("tmpListing")


				val sqlDestinationattributes = s"SELECT destinationattributesid, destinationid, rowstartdate, rowenddate FROM dw.dbo.destinationattributes with (NOLOCK)"

				log.info(s"Running Query: $sqlDestinationattributes")
				val dfDestinationattributes = edwLoader.getEDWData(sqlDestinationattributes)

				dfDestinationattributes.persist

				dfDestinationattributes.createOrReplaceTempView("tmpDestinationattributes")


				val sqlSubscription = s"SELECT subscriptionid, paymenttypeid, listingid, subscriptionstartdate, subscriptionenddate, subscriptionkey  FROM dw.dbo.subscription with (NOLOCK)"

				log.info(s"Running Query: $sqlSubscription")
				val dfSubscription = edwLoader.getEDWData(sqlSubscription)

				dfSubscription.persist

				dfSubscription.createOrReplaceTempView("tmpSubscription")


				val sqlSite = s"SELECT bookingchannelid, siteid  FROM dw.dbo.site with (NOLOCK)"

				log.info(s"Running Query: $sqlSite")
				val dfSite = edwLoader.getEDWData(sqlSite)

				dfSite.persist

				dfSite.createOrReplaceTempView("tmpSite")

				var query =
					s"""
					   |select
					   |            distinct qf.reservationid AS reservationid
					   |        from
					   |            tmpQuotefact qf
					   |            inner join (SELECT *
					   |                        FROM tmpReservation
					   |                        WHERE activeflag = 1
					   |                        and CAST(createdate AS DATE) >= '$rollingTwoYears'
					   |                        and bookingcategoryid != 1) r
					   |            on qf.reservationid = r.reservationid
					   |        where
					   |            1 = 1
					   |            and CAST(qf.quotecreateddate AS DATE) >= '$rollingTwoYears'
					   |            and qf.quoteitemactiveflag = 1
					   |            and qf.quoteactiveflag = 1
					   |            and qf.quotestatusid >= -1
					   |            and (
					   |                CAST(qf.dwcreatedate AS DATE) between '$startDate' and '$endDate'
					   |                or CAST(dwlastupdatedate AS DATE) between '$startDate' and '$endDate'
					   |            )
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfResList1 = sql(query)
				dfResList1.createOrReplaceTempView("tmpDfResList1")


				query =
					s"""
					   |select
					   |            r.reservationid AS reservationid
					   |        from
					   |            tmpReservation r
					   |            LEFT JOIN tmpDfResList1 r2
					   |            ON r2.reservationid = r.reservationid
					   |        where
					   |            CAST(r.createdate AS DATE) >= '$rollingTwoYears'
					   |            and r.activeflag = 1
					   |            and r.bookingcategoryid != 1
					   |            and CAST(r.dwupdatedatetime AS DATE) between '$startDate' and '$endDate'
					   |            and r2.reservationid IS NULL
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfResList2 = sql(query)
				dfResList2.createOrReplaceTempView("tmpDfResList2")


				query =
					s"""
					   |SELECT reservationid from tmpDfResList1
					   |UNION ALL
					   |SELECT reservationid from tmpDfResList2
                     """.stripMargin

				log.info(s"Running Query: $query")
				val cmb_tmpDfResList1 = sql(query)
				cmb_tmpDfResList1.createOrReplaceTempView("cmb_tmpDfResList1")

				query =
					s"""
					   |select
					   |            distinct tto.reservationid AS reservationid
					   |        from
					   |            tmpTravelerorder tto
					   |        inner join
					   |            tmpTravelerorderitem toi
					   |                on tto.travelerorderid = toi.travelerorderid
					   |        inner join
					   |            tmpTravelerorderpayment topp
					   |                on tto.travelerorderid = topp.travelerorderid
					   |        inner join
					   |            tmpTravelerorderpaymentdistribution topd
					   |                on topp.travelerorderpaymentid = topd.travelerorderpaymentid
					   |                and topd.travelerorderitemid = topd.travelerorderitemid
					   |        INNER JOIN (SELECT *
					   |                    FROM tmpReservation
					   |                    where
					   |                    activeflag = 1
					   |                    and bookingcategoryid != 1
					   |                    and CAST(createdate AS DATE) >= '$rollingTwoYears') r
					   |        ON r.reservationid = tto.reservationid
					   |        LEFT JOIN cmb_tmpDfResList1 r2
					   |        ON r2.reservationid = tto.reservationid
					   |        where
					   |            topp.travelerorderpaymenttypename = 'PAYMENT'
					   |            and topp.travelerorderpaymentstatusname in (
					   |                'PAID' , 'SETTLED' , 'REMITTED'
					   |            )
					   |            and CAST(topd.updatedate AS DATE) between '$startDate' and '$endDate'
					   |            and r2.reservationid IS NULL
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfResList3 = sql(query)
				dfResList3.createOrReplaceTempView("tmpDfResList3")

				query =
					s"""
					   |SELECT reservationid from cmb_tmpDfResList1
					   |UNION ALL
					   |SELECT reservationid from tmpDfResList3
                     """.stripMargin

				log.info(s"Running Query: $query")
				val cmb_tmpDfResList2 = sql(query)
				cmb_tmpDfResList2.createOrReplaceTempView("cmb_tmpDfResList2")


				query =
					s"""
					   |select
					   |        r.arrivaldate ,
					   |        r.createdate  ,
					   |        r.bookingcategoryid ,
					   |        r.bookingdate AS bookingdateid,
					   |        r.bookingreporteddate AS bookingreporteddateid,
					   |        r.cancelleddate AS cancelleddateid,
					   |        r.brandid ,
					   |        r.listingunitid ,
					   |        r.reservationid ,
					   |        r.siteid ,
					   |        r.traveleremailid ,
					   |        coalesce ( r.visitorid , 0 ) AS visitorid,
					   |        r.onlinebookingprovidertypeid ,
					   |        r.reservationavailabilitystatustypeid AS reservationavailabilitystatustypeid,
					   |        r.reservationpaymentstatustypeid  AS reservationpaymentstatustypeid,
					   |        r.devicecategorysessionid AS devicecategorysessionid,
					   |        r.inquiryserviceentryguid ,
					   |        0 AS inquiryid ,
					   |        lu.customerid ,
					   |        lu.listingid ,
					   |        b.reportingcurcode as brandcurrencycode ,
					   |        lu.regionid ,
					   |        case
					   |            when r.arrivaldate = r.departuredate then 1
					   |            else datediff (r.departuredate, r.arrivaldate)
					   |        end AS bookednightscount,
					   |        0 AS brandattributesid,
					   |        0 AS customerattributesid,
					   |        0 AS listingattributesid ,
					   |        -1 AS listingunitattributesid,
					   |        -1 AS subscriptionid,
					   |        -1 AS paymenttypeid,
					   |        -1 AS listingchannelid,
					   |        -1 AS persontypeid,
					   |        1 AS bookingcount,
					   |        -1 AS bookingchannelid,
					   |        0 AS strategicdestinationid,
					   |        0 AS strategicdestinationattributesid,
					   |        r.visitid ,
					   |        r.fullvisitorid ,
					   |        r.websitereferralmediumsessionid
					   |    from
					   |        tmpReservation r
					   |    inner join
					   |        tmpListingunit lu
					   |            on r.listingunitid = lu.listingunitid
					   |    inner join
					   |        tmpBrand b
					   |            on r.brandid = b.brandid
					   |    inner join
					   |        cmb_tmpDfResList2 rl
					   |            on r.reservationid = rl.reservationid
                     """.stripMargin


				log.info(s"Running Query: $query")
				var dfRes1 = sql(query)

				val dfResNullableCols = Set("listingchannelid")
				dfRes1 = spark.createDataFrame(dfRes1.rdd, StructType(dfRes1.schema.map(x => if (dfResNullableCols.contains(x.name)) x.copy(nullable = true) else x)))

				dfRes1.persist
				dfRes1.createOrReplaceTempView("dfRes1")


				query =
					s"""
					   |select
					   |       r.*, i.inquiryid  as inquiryid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |        (select * from (select *, row_number() OVER (PARTITION BY inquiryserviceentryguid, listingid ORDER BY inquiryserviceentryguid, listingid) AS rank FROM tmpInquiry) tmp WHERE rank = 1) i
					   |            on lower(r.inquiryserviceentryguid) = lower(i.inquiryserviceentryguid)
					   |            and r.listingid = i.listingid
                     """.stripMargin


				log.info(s"Running Query: $query")
				var dfRes2 = updateDataFrame(spark, jc, dfRes1, "dfRes1", query, "Update #res.InquiryId")
				dfRes2 = createCheckPoint(spark, jc, dfRes2)
				dfRes1.unpersist
				dfRes2.createOrReplaceTempView("dfRes2")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes2, "dfRes2")


				query =
					s"""
					   |select
					   |        r.*, ch.channelid  as listingchannelid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |        tmpOnlinebookingprovidertype obpt
					   |            on r.onlinebookingprovidertypeid = obpt.onlinebookingprovidertypeid
					   |    left join
					   |        tmpChannel ch
					   |            on case
					   |                when obpt.olbprovidergatewaytypedescription like 'Clear Stay%' then 'clearstay'
					   |                else obpt.listingsourcename
					   |            end = ch.channelname
					   |            and obpt.listingsourcename = ch.listingsource
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfRes3 = updateDataFrame(spark, jc, dfRes2, "dfRes2", query, "Update #res.ListingChannelId")
				// var dfRes3 = updateDataFrame(spark, jc, dfRes2, "dfRes2", query, "Update #res.ListingChannelId")
				// dfRes3 = createCheckPoint(spark, jc, dfRes3)
				dfRes2.unpersist
				dfRes3.createOrReplaceTempView("dfRes3")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes3, "dfRes3")


				query =
					s"""
					   |    select
					   |        r.*, la.listingattributesid  as listingattributesid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |        tmpListingattributes la
					   |            on r.listingid = la.listingid
					   |            and r.createdate between la.rowstartdate and la.rowenddate
					   |
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfRes4 = updateDataFrame(spark, jc, dfRes3, "dfRes3", query, "Update #res.ListingAttributesId")
				// var dfRes4 = updateDataFrame(spark, jc, dfRes3, "dfRes3", query, "Update #res.ListingAttributesId")
				// dfRes4 = createCheckPoint(spark, jc, dfRes4)
				dfRes3.unpersist
				dfRes4.createOrReplaceTempView("dfRes4")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes4, "dfRes4")


				query =
					s"""
					   |select * from
					   |(
					   |select
					   |        r.*,
					   |        row_number() OVER (PARTITION BY reservationid ORDER BY r.bookingdateid desc) as rank,
					   |        s.subscriptionid  as subscriptionid_TMP,
					   |        s.paymenttypeid  as paymenttypeid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |         tmpSubscription s
					   |            on r.listingid = s.listingid
					   |    where
					   |        r.bookingdateid between s.subscriptionstartdate and s.subscriptionenddate
					   |) a where rank =1
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfRes5 = updateDataFrame(spark, jc, dfRes4, "dfRes4", query, "Update #res.Subscriptionid.PaymentTypeId")
				// var dfRes5 = updateDataFrame(spark, jc, dfRes4, "dfRes4", query, "Update #res.Subscriptionid.PaymentTypeId")
				// dfRes5 = createCheckPoint(spark, jc, dfRes5)
				dfRes4.unpersist
				dfRes5.createOrReplaceTempView("dfRes5")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes5, "dfRes5")


				query =
					s"""
					   |select
					   |        r.*, l.strategicdestinationid  as strategicdestinationid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |        tmpListing l
					   |            on r.listingid = l.listingid
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfRes6 = updateDataFrame(spark, jc, dfRes5, "dfRes5", query, "Update #res.StrategicDestinationId")
				// var dfRes6 = updateDataFrame(spark, jc, dfRes5, "dfRes5", query, "Update #res.StrategicDestinationId")
				// dfRes6 = createCheckPoint(spark, jc, dfRes6)
				dfRes5.unpersist
				dfRes6.createOrReplaceTempView("dfRes6")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes6, "dfRes6")


				query =
					s"""
					   |select
					   |        r.*, da.destinationattributesid  as strategicdestinationattributesid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |        tmpDestinationattributes da
					   |            on r.strategicdestinationid = da.destinationid
					   |            and r.createdate between da.rowstartdate and da.rowenddate
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfRes7 = updateDataFrame(spark, jc, dfRes6, "dfRes6", query, "Update #res.StrategicDestinationAttributesId")
				// var dfRes7 = updateDataFrame(spark, jc, dfRes6, "dfRes6", query, "Update #res.StrategicDestinationAttributesId")
				// dfRes7 = createCheckPoint(spark, jc, dfRes7)
				dfRes6.unpersist
				dfRes7.createOrReplaceTempView("dfRes7")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes7, "dfRes7")


				query =
					s"""
					   |select
					   |        r.*, ba.brandattributesid  as brandattributesid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |        tmpBrandattributes ba
					   |            on r.brandid = ba.brandid
					   |            and r.createdate between ba.rowstartdate and ba.rowenddate
					 """.stripMargin

				log.info(s"Running Query: $query")
				val dfRes8 = updateDataFrame(spark, jc, dfRes7, "dfRes7", query, "Update #res.BrandAttributesId")
				// var dfRes8 = updateDataFrame(spark, jc, dfRes7, "dfRes7", query, "Update #res.BrandAttributesId")
				// dfRes8 = createCheckPoint(spark, jc, dfRes8)
				dfRes7.unpersist
				dfRes8.createOrReplaceTempView("dfRes8")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes8, "dfRes8")


				query =
					s"""
					   |select
					   |        r.*,
					   |        ca.customerattributesid  as customerattributesid_TMP,
					   |        coalesce ( pt.persontypeid, -1 ) as persontypeid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |        tmpCustomerattributes ca
					   |            on r.customerid = ca.customerid
					   |            and r.createdate between ca.rowstartdate and ca.rowenddate
					   |    left join
					   |        tmpPersontype pt
					   |            on coalesce ( ca.persontype ,'Unknown' ) = pt.persontypename
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfRes9 = updateDataFrame(spark, jc, dfRes8, "dfRes8", query, "Update #res.CustomerAttributesId.PersonTypeId")
				// var dfRes9 = updateDataFrame(spark, jc, dfRes8, "dfRes8", query, "Update #res.CustomerAttributesId.PersonTypeId")
				// dfRes9 = createCheckPoint(spark, jc, dfRes9)
				dfRes8.unpersist
				dfRes9.createOrReplaceTempView("dfRes9")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes9, "dfRes9")


				query =
					s"""
					   |select
					   |        r.*, lua.listingunitattributesid  as listingunitattributesid_TMP
					   |    from
					   |        {SOURCE_DF} r
					   |    inner join
					   |        tmpListingunitattributes lua
					   |            on r.listingunitid = lua.listingunitid
					   |            and r.createdate between lua.rowstartdate and lua.rowenddate
                     """.stripMargin

				log.info(s"Running Query: $query")
				val dfRes10 = updateDataFrame(spark, jc, dfRes9, "dfRes9", query, "Update #res.ListingUnitAttributesId")
				// var dfRes10 = updateDataFrame(spark, jc, dfRes9, "dfRes9", query, "Update #res.ListingUnitAttributesId")
				// dfRes10 = createCheckPoint(spark, jc, dfRes10)
				dfRes9.unpersist
				dfRes10.createOrReplaceTempView("dfRes10")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes10, "dfRes10")


				query =
					s"""
					   |select
					   |        r.*, s.bookingchannelid  as bookingchannelid_TMP
					   |    from
					   |       {SOURCE_DF} r
					   |    inner join
					   |        tmpSite s
					   |            on s.siteid = r.siteid
					   |    where
					   |        s.bookingchannelid > -1
                     """.stripMargin

				log.info(s"Running Query: $query")
				var dfRes = updateDataFrame(spark, jc, dfRes10, "dfRes10", query, "Update #res.BookingChannelId")
				dfRes = createCheckPoint(spark, jc, dfRes)
				dfRes10.unpersist
				dfRes.createOrReplaceTempView("dfRes")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfRes, "dfRes")


				query =
					s"""
					   |select
					   |        r.reservationid ,
					   |        gbv.currencycode ,
					   |        case when gbv.quotestatusid = -1 then 'Quote Unk' else 'Quote' end AS valuesource,
					   |        gbv.rentalamount ,
					   |        gbv.refundabledamagedepositamount ,
					   |        gbv.servicefeeamount ,
					   |        gbv.taxamount ,
					   |        gbv.vasamount ,
					   |        gbv.orderamount ,
					   |        gbv.paidamount ,
					   |        gbv.refundamount ,
					   |        gbv.bookingvalue ,
					   |        gbv.commissionamount ,
					   |        gbv.paymentmethodtypeid ,
					   |        row_number() over (partition by gbv.reservationid order by case when gbv.quotestatusid = -1 then 'Quote Unk' else 'Quote' end, gbv.quoteid) AS rownum
					   |    from
					   |        tmpVw_quotegrossbookingvalue gbv
					   |    inner join
					   |        dfRes r
					   |            on gbv.reservationid = r.reservationid
                     """.stripMargin

				log.info(s"Running Query: $query")
				var dfQgbv = sql(query)
				dfQgbv.persist
				dfQgbv = createCheckPoint(spark, jc, dfQgbv, "Insert #Qgbv from Quote")
				dfQgbv.createOrReplaceTempView("dfQgbv")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfQgbv, "dfQgbv")


				query =
					s"""
					   |select
					   |        gbv.reservationid ,
					   |        gbv.currencycode ,
					   |        gbv.rentalamount ,
					   |        gbv.refundabledamagedepositamount ,
					   |        gbv.servicefeeamount ,
					   |        gbv.taxamount ,
					   |        gbv.vasamount ,
					   |        gbv.orderamount ,
					   |        gbv.paidamount ,
					   |        gbv.refundamount ,
					   |        gbv.bookingvalue ,
					   |        gbv.valuesource ,
					   |        -1 AS displayregionid,
					   |        gbv.commissionamount ,
					   |        0 AS paymentmethodtypeid,
					   |        0 AS paidamountoffline,
					   |        0 AS paidamountonline
					   |    from
					   |        (select
					   |        reservationid ,
					   |        currencycode ,
					   |        valuesource ,
					   |        sum ( rentalamount ) AS rentalamount,
					   |        sum ( refundabledamagedepositamount ) AS refundabledamagedepositamount,
					   |        sum ( servicefeeamount ) AS servicefeeamount,
					   |        sum ( taxamount ) AS taxamount,
					   |        sum ( vasamount ) AS vasamount,
					   |        sum ( orderamount ) AS orderamount,
					   |        sum ( paidamount ) AS paidamount,
					   |        sum ( refundamount ) AS refundamount,
					   |        sum ( bookingvalue ) AS bookingvalue,
					   |        sum ( commissionamount ) AS commissionamount
					   |    from
					   |        dfQgbv
					   |    group by
					   |        reservationid ,
					   |        currencycode ,
					   |        valuesource ) AS gbv
                     """.stripMargin

				log.info(s"Running Query: $query")
				var dfGbv1 = sql(query)
				dfGbv1.persist
				dfGbv1 = createCheckPoint(spark, jc, dfGbv1, "Insert #gbv")
				dfGbv1.createOrReplaceTempView("dfGbv1")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfGbv1, "dfGbv1")


				query =
					s"""
					   |SELECT * FROM (
					   |    SELECT *, row_number() OVER (PARTITION BY ReservationId ORDER BY ValueSource ASC) rank FROM dfGbv1
					   | ) tmp WHERE rank = 1
                     """.stripMargin

				log.info(s"Running Query: $query")
				var dfGbv2 = sql(query).drop("rank")
				dfGbv2.persist
				dfGbv2 = createCheckPoint(spark, jc, dfGbv2, "#gbv Final")
				dfGbv2.createOrReplaceTempView("dfGbv2")
				dfGbv1.unpersist

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfGbv2, "dfGbv2")


				query =
					s"""
					   |select
					   |        gbv.*, x.paymentmethodtypeid  as paymentmethodtypeid_TMP
					   |    from
					   |        {SOURCE_DF} gbv
					   |    inner join
					   |        dfQgbv x
					   |            on x.reservationid = gbv.reservationid
					   |            and x.valuesource = gbv.valuesource
					   |    where
					   |        x.rownum = 1
					   |        and x.paymentmethodtypeid > 0
					 """.stripMargin

				log.info(s"Running Query: $query")
				val dfGbv3 = updateDataFrame(spark, jc, dfGbv2, "dfGbv2", query, "Update #gbv.PaymentMethodTypeId")
				// var dfGbv3 = updateDataFrame(spark, jc, dfGbv2, "dfGbv2", query, "Update #gbv.PaymentMethodTypeId")
				// dfGbv3 = createCheckPoint(spark, jc, dfGbv3)
				dfGbv3.createOrReplaceTempView("dfGbv3")
				dfGbv2.unpersist

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfGbv3, "dfGbv3")


				query =
					s"""
					   |select
					   |    tot.reservationid ,
					   |    g.currencycode ,
					   |    case
					   |        when g.currencycode != topp.currencycode and cc.conversionrate is not null then topp.amount * cc.conversionrate
					   |        else topp.amount
					   |    end AS amount,
					   |    pmt.paymentcollectiontype
					   |from
					   |    tmpTravelerorderpayment topp
					   |inner join
					   |    tmpTravelerorder tot
					   |        on topp.travelerorderid = tot.travelerorderid
					   |inner join
					   |    tmpPaymentmethodtype pmt
					   |        on pmt.paymentmethodtypeid = topp.paymentmethodtypeid
					   |inner join
					   |    dfGbv3 g
					   |        on tot.reservationid = g.reservationid
					   |left join
					   |    tmpCurrencyconversion cc
					   |        on g.currencycode = cc.tocurrency
					   |        and topp.currencycode = cc.fromcurrency
					   |        and topp.paymentdate between cc.rowstartdate and cc.rowenddate
					   |where
					   |    topp.amount <> 0
					   |    and tot.quoteid > 0
					   |    and topp.travelerorderpaymenttypename in ( 'PAYMENT' )
					   |    and topp.travelerorderpaymentstatusname in (  'PAID', 'SETTLED', 'REMITTED' )
                     """.stripMargin

				log.info(s"Running Query: $query")
				val pivotDfGbv1 = sql(query)
				val pivotDfGbv2 = pivotDfGbv1.groupBy("reservationid", "currencycode").pivot("paymentcollectiontype", Array("ONLINE", "OFFLINE", "UNKNOWN")).agg(sum(round(col("amount"), 3)))
				pivotDfGbv2.createOrReplaceTempView("pivotDfGbv2")


				query =
					s"""
					   |SELECT
					   |        p.ReservationId
					   |       ,p.CurrencyCode
					   |       ,COALESCE(p.ONLINE, 0) AS PaidAmountOnline
					   |       ,COALESCE(p.OFFLINE, 0) + COALESCE(p.UNKNOWN, 0) AS PaidAmountOffline
					   |FROM pivotDfGbv2 p
                     """.stripMargin

				log.info(s"Running Query: $query")
				var GBV2 = sql(query)
				GBV2.persist
				GBV2 = createCheckPoint(spark, jc, GBV2, "Insert #GBV2")
				GBV2.createOrReplaceTempView("GBV2")


				query =
					"""
					  |select
					  |        g.*, r.displayregionid AS displayregionid_TMP
					  |    from
					  |        {SOURCE_DF} g
					  |    inner join
					  |        ( select
					  |        r.reservationid ,
					  |        max ( q.displayregionid ) as displayregionid
					  |    from
					  |        tmpQuotefact q
					  |    inner join
					  |        dfRes r
					  |            on q.reservationid = r.reservationid
					  |    group by
					  |        r.reservationid ) r
					  |            on g.reservationid = r.reservationid
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfGbv4 = updateDataFrame(spark, jc, dfGbv3, "dfGbv3", query, "Update #gbv.DisplayRegionId")
				// var dfGbv4 = updateDataFrame(spark, jc, dfGbv3, "dfGbv3", query, "Update #gbv.DisplayRegionId")
				// dfGbv4 = createCheckPoint(spark, jc, dfGbv4)
				dfGbv4.createOrReplaceTempView("dfGbv4")
				dfGbv3.unpersist

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfGbv4, "dfGbv4")


				query =
					"""
					  |select
					  |        tto.reservationid ,
					  |        toi.currencycode ,
					  |        sum ( topd.distributedamount ) AS servicefeeamount,
					  |        sum ( case when topd.travelerorderpaymentdistributiontoaccounttypeid = 2 then topd.distributedamount else 0 end ) AS servicefeevatamount,
					  |        sum ( case when topd.travelerorderpaymentdistributiontypeid in (10, 5, 4, 2, 1) then topd.distributedamount else 0 end ) AS servicefeeprocessingfeeamount
					  |    from
					  |        tmpTravelerorder tto
					  |    inner join
					  |        tmpTravelerorderitem toi
					  |            on tto.travelerorderid = toi.travelerorderid
					  |    inner join
					  |        tmpProduct p
					  |            on toi.productid = p.productid
					  |    inner join
					  |        tmpTravelerorderpayment topp
					  |            on tto.travelerorderid = topp.travelerorderid
					  |    inner join
					  |        tmpTravelerorderpaymentdistribution topd
					  |            on topp.travelerorderpaymentid = topd.travelerorderpaymentid
					  |            and toi.travelerorderitemid = topd.travelerorderitemid
					  |    inner join
					  |        dfGbv4 gbv
					  |            on tto.reservationid = gbv.reservationid
					  |    where
					  |        p.grossbookingvalueproductcategory = 'TRAVELER_FEE'
					  |        and topp.travelerorderpaymenttypename = 'PAYMENT'
					  |        and topp.travelerorderpaymentstatusname in ('PAID', 'SETTLED', 'REMITTED')
					  |    group by
					  |        tto.reservationid, toi.currencycode
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfSf = sql(query)
				dfSf.persist
				dfSf = createCheckPoint(spark, jc, dfSf, "#sf insert")
				dfSf.createOrReplaceTempView("dfSf")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfSf, "dfSf")


				query =
					"""
					  |select
					  |        gbv.*, 'Quote and TravelerOrder'  as valuesource_TMP,
					  |        case when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount else gbv.servicefeeamount end as servicefeeamount_TMP,
					  |        gbv.taxamount + sf.servicefeevatamount as taxamount_TMP, gbv.orderamount - gbv.servicefeeamount - gbv.taxamount + case when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount else gbv.servicefeeamount end + gbv.taxamount + sf.servicefeevatamount as orderamount_TMP,
					  |        gbv.bookingvalue - gbv.servicefeeamount - gbv.taxamount + case when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount else gbv.servicefeeamount end + gbv.taxamount + sf.servicefeevatamount as bookingvalue_TMP
					  |    from
					  |        {SOURCE_DF} gbv
					  |    inner join
					  |        dfSf sf
					  |            on sf.reservationid = gbv.reservationid
					  |            and sf.currencycode = gbv.currencycode
					  |    where
					  |        gbv.servicefeeamount != case
					  |            when gbv.servicefeeamount = 0 then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount
					  |            else gbv.servicefeeamount
					  |        end
					  |        or sf.servicefeevatamount != 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfGbv5 = updateDataFrame(spark, jc, dfGbv4, "dfGbv4", query, "#gbv SF update")
				// var dfGbv5 = updateDataFrame(spark, jc, dfGbv4, "dfGbv4", query, "#gbv SF update")
				// dfGbv5 = createCheckPoint(spark, jc, dfGbv5)
				dfGbv5.createOrReplaceTempView("dfGbv5")
				dfGbv4.unpersist

				if (spark.conf.getOption("debug").getOrElse("false") == "true") debug(dfGbv5, "dfGbv5")


				query =
					"""
					  |select
					  |        r.reservationid ,
					  |        r.listingunitid ,
					  |        r.regionid ,
					  |        r.arrivaldate ,
					  |        cal.quarterenddate as arrivalquarterenddate ,
					  |        cal.yearnumber as arrivalyearnumber ,
					  |        r.brandcurrencycode ,
					  |        r.brandid ,
					  |        r.bookednightscount ,
					  |        l.bedroomnum ,
					  |        l.postalcode ,
					  |        l.city ,
					  |        l.country ,
					  |        calb.quarterenddate AS bookingquarterenddate,
					  |        calb.yearnumber AS bookingyearnumber,
					  |        'USD' AS currencycode,
					  |        cast (0 as decimal(18, 6)) AS bookingvalue,
					  |        0 AS step
					  |    from
					  |        dfRes r
					  |    inner join
					  |        tmpBookingCategory bc
					  |            on r.bookingcategoryid = bc.bookingcategoryid
					  |    inner join
					  |        tmpListing l
					  |            on r.listingid = l.listingid
					  |    inner join
					  |        tmpCalendar cal
					  |            on r.arrivaldate = cal.dateid
					  |    inner join
					  |        tmpCalendar calb
					  |            on r.bookingdateid = calb.dateid
					  |    left join
					  |        dfGbv5 g
					  |            on  r.reservationid = g.reservationid
					  |    where
					  |        bc.bookingindicator = 1
					  |        and bc.knownbookingindicator = 0
					  |        and g.reservationid is null
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfEst1 = sql(query)
				dfEst1.persist
				dfEst1 = createCheckPoint(spark, jc, dfEst1, "Insert #est")
				dfEst1.createOrReplaceTempView("dfEst1")

				if (spark.conf.getOption("debug").getOrElse("false") == "true") {
					log.info(s"EST Debug Count: ${sql(query).count}")
					debug(dfBookingCategory, "dfBookingCategory")
					debug(dfListing, "dfListing")
					debug(dfCalendar, "dfCalendar")
					debug(dfEst1, "dfEst1")
				}


				query =
					"""
					  |select
					  |        f.*,
					  |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        1  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 1
					  |            and f.listingunitid = x.listingunitid
					  |            and f.arrivalquarterenddate = x.arrivalquarterenddate
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst2 = updateDataFrame(spark, jc, dfEst1, "dfEst1", query, "#est 1")
				// var dfEst2 = updateDataFrame(spark, jc, dfEst1, "dfEst1", query, "#est 1")
				// dfEst2 = createCheckPoint(spark, jc, dfEst2)
				dfEst2.createOrReplaceTempView("dfEst2")
				dfEst1.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        2  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 2
					  |            and f.listingunitid = x.listingunitid
					  |            and f.arrivalyearnumber = x.arrivalyearnumber
					  |    where
					  |        f.step=0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst3 = updateDataFrame(spark, jc, dfEst2, "dfEst2", query, "#est 2")
				// var dfEst3 = updateDataFrame(spark, jc, dfEst2, "dfEst2", query, "#est 2")
				// dfEst3 = createCheckPoint(spark, jc, dfEst3)
				dfEst3.createOrReplaceTempView("dfEst3")
				dfEst2.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        3  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 3
					  |            and f.listingunitid = x.listingunitid
					  |    where
					  |        f.step=0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst4 = updateDataFrame(spark, jc, dfEst3, "dfEst3", query, "#est 3")
				// var dfEst4 = updateDataFrame(spark, jc, dfEst3, "dfEst3", query, "#est 3")
				// dfEst4 = createCheckPoint(spark, jc, dfEst4)
				dfEst4.createOrReplaceTempView("dfEst4")
				dfEst3.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        4  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 4
					  |            and f.brandid = x.brandid
					  |            and f.bedroomnum = x.bedroomnum
					  |            and f.postalcode = x.postalcode
					  |            and f.bookingquarterenddate = x.bookingquarterenddate
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst5 = updateDataFrame(spark, jc, dfEst4, "dfEst4", query, "#est 4")
				// var dfEst5 = updateDataFrame(spark, jc, dfEst4, "dfEst4", query, "#est 4")
				// dfEst5 = createCheckPoint(spark, jc, dfEst5)
				dfEst5.createOrReplaceTempView("dfEst5")
				dfEst4.unpersist


				query =
					"""
					  |select
					  |        f.*,
					  |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        5  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 5
					  |            and f.brandid = x.brandid
					  |            and f.bedroomnum = x.bedroomnum
					  |            and f.postalcode = x.postalcode
					  |            and f.arrivalyearnumber = x.arrivalyearnumber
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst6 = updateDataFrame(spark, jc, dfEst5, "dfEst5", query, "#est 5")
				// var dfEst6 = updateDataFrame(spark, jc, dfEst5, "dfEst5", query, "#est 5")
				// dfEst6 = createCheckPoint(spark, jc, dfEst6)
				dfEst6.createOrReplaceTempView("dfEst6")
				dfEst5.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        6  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 6
					  |            and f.brandid = x.brandid
					  |            and f.bedroomnum = x.bedroomnum
					  |            and f.postalcode = x.postalcode
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst7 = updateDataFrame(spark, jc, dfEst6, "dfEst6", query, "#est 6")
				// var dfEst7 = updateDataFrame(spark, jc, dfEst6, "dfEst6", query, "#est 6")
				// dfEst7 = createCheckPoint(spark, jc, dfEst7)
				dfEst7.createOrReplaceTempView("dfEst7")
				dfEst6.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        7  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 7
					  |            and f.brandid = x.brandid
					  |            and f.bedroomnum = x.bedroomnum
					  |            and f.city = x.city
					  |            and f.country = x.country
					  |            and f.bookingquarterenddate = x.bookingquarterenddate
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst8 = updateDataFrame(spark, jc, dfEst7, "dfEst7", query, "#est 7")
				// var dfEst8 = updateDataFrame(spark, jc, dfEst7, "dfEst7", query, "#est 7")
				// dfEst8 = createCheckPoint(spark, jc, dfEst8)
				dfEst8.createOrReplaceTempView("dfEst8")
				dfEst7.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round (x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        8  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 8
					  |            and f.brandid = x.brandid
					  |            and f.bedroomnum = x.bedroomnum
					  |            and f.city = x.city
					  |            and f.country = x.country
					  |            and f.arrivalyearnumber = x.arrivalyearnumber
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst9 = updateDataFrame(spark, jc, dfEst8, "dfEst8", query, "#est 8")
				// var dfEst9 = updateDataFrame(spark, jc, dfEst8, "dfEst8", query, "#est 8")
				// dfEst9 = createCheckPoint(spark, jc, dfEst9)
				dfEst9.createOrReplaceTempView("dfEst9")
				dfEst8.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round(x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        9  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 9
					  |            and f.brandid = x.brandid
					  |            and f.bedroomnum = x.bedroomnum
					  |            and f.city = x.city
					  |            and f.country = x.country
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst10 = updateDataFrame(spark, jc, dfEst9, "dfEst9", query, "#est 9")
				// var dfEst10 = updateDataFrame(spark, jc, dfEst9, "dfEst9", query, "#est 9")
				// dfEst10 = createCheckPoint(spark, jc, dfEst10)
				dfEst10.createOrReplaceTempView("dfEst10")
				dfEst9.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round(x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        estimationstepnum  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 10
					  |            and f.brandid = x.brandid
					  |            and f.bedroomnum = x.bedroomnum
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst11 = updateDataFrame(spark, jc, dfEst10, "dfEst10", query, "#est 10")
				// var dfEst11 = updateDataFrame(spark, jc, dfEst10, "dfEst10", query, "#est 10")
				// dfEst11 = createCheckPoint(spark, jc, dfEst11)
				dfEst11.createOrReplaceTempView("dfEst11")
				dfEst10.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round(x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        estimationstepnum  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 11
					  |            and f.bedroomnum = x.bedroomnum
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfEst12 = updateDataFrame(spark, jc, dfEst11, "dfEst11", query, "#est 11")
				// var dfEst12 = updateDataFrame(spark, jc, dfEst11, "dfEst11", query, "#est 11")
				// dfEst12 = createCheckPoint(spark, jc, dfEst12)
				dfEst12.createOrReplaceTempView("dfEst12")
				dfEst11.unpersist

				query =
					"""
					  |select
					  |        f.*,
					  |        round(x.averagebookingvaluepernightusd * f.bookednightscount, 6) as bookingvalue_TMP,
					  |        estimationstepnum  as step_TMP
					  |    from
					  |        {SOURCE_DF} f
					  |    inner join
					  |        tmpAverageknownnightlybookingvalue x
					  |            on x.estimationstepnum = 12
					  |            and f.brandid = x.brandid
					  |    where
					  |        f.step = 0
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfEst = updateDataFrame(spark, jc, dfEst12, "dfEst12", query, "#est 12")
				dfEst = createCheckPoint(spark, jc, dfEst)
				dfEst.createOrReplaceTempView("dfEst")
				dfEst12.unpersist


				query =
					"""
					  |SELECT
					  |       ReservationId
					  |       ,CurrencyCode
					  |       ,BookingValue AS RentalAmount
					  |       ,0 AS RefundableDamageDepositAmount
					  |       ,0 AS ServiceFeeAmount
					  |       ,0 AS TaxAmount
					  |       ,0 AS VasAmount
					  |       ,BookingValue AS OrderAmount
					  |       ,0 AS PaidAmount
					  |       ,0 AS RefundAmount
					  |       ,BookingValue
					  |       ,CONCAT('Estimate Step ', Step) AS ValueSource
					  |       ,-1 AS displayregionid
					  |       ,0 AS CommissionAmount
					  |       ,0 AS PaymentMethodTypeId
					  |       ,0 AS PaidAmountOffline
					  |       ,0 AS PaidAmountOnline
					  |   FROM
					  |       dfEst
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfGbv6 = sql(query)
				dfGbv6.persist
				dfGbv6 = createCheckPoint(spark, jc, dfGbv6, "Insert #gbv From #est")
				dfGbv6.createOrReplaceTempView("dfGbv6")

				query =
					"""
					  |SELECT ReservationId, CurrencyCode, RentalAmount, RefundableDamageDepositAmount, ServiceFeeAmount, TaxAmount, VasAmount, OrderAmount, PaidAmount, RefundAmount, BookingValue, ValueSource, displayregionid, CommissionAmount, PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline FROM dfGbv5
					  |UNION ALL
					  |SELECT ReservationId, CurrencyCode, RentalAmount, RefundableDamageDepositAmount, ServiceFeeAmount, TaxAmount, VasAmount, OrderAmount, PaidAmount, RefundAmount, BookingValue, ValueSource, displayregionid, CommissionAmount, PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline FROM dfGbv6
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfGbv = sql(query)
				dfGbv.persist
				dfGbv = createCheckPoint(spark, jc, dfGbv)
				dfGbv.createOrReplaceTempView("dfGbv")
				dfGbv5.unpersist
				dfGbv6.unpersist


				query =
					"""
					  |SELECT r.*,
					  |       COALESCE (s.bookingvaluesourceid, 1)          AS bookingvaluesourceid,
					  |       COALESCE (g.displayregionid, -1)              AS displayregionid,
					  |       COALESCE (g.rentalamount, 0)                  AS rentalamount,
					  |       COALESCE (g.refundabledamagedepositamount, 0) AS refundabledamagedepositamount,
					  |       COALESCE (g.servicefeeamount, 0)              AS servicefeeamount,
					  |       COALESCE (g.taxamount, 0)                     AS taxamount,
					  |       COALESCE (g.vasamount, 0)                     AS vasamount,
					  |       COALESCE (g.orderamount, 0)                   AS orderamount,
					  |       COALESCE (g.paidamount, 0)                    AS paidamount,
					  |       COALESCE (g.refundamount, 0)                  AS refundamount,
					  |       COALESCE (g.bookingvalue, 0)                  AS bookingvalue,
					  |       COALESCE (c.currencyid, 154)                  AS localcurrencyid,
					  |       COALESCE (ccusd.currencyconversionid, -1)     AS currencyconversionidusd,
					  |       COALESCE (ccbc.currencyconversionid, -1)      AS currencyconversionidbrandcurrency,
					  |       COALESCE (g.commissionamount, 0)              AS commissionamount,
					  |       COALESCE (g.paymentmethodtypeid, 0)           AS paymentmethodtypeid,
					  |       COALESCE (g2.paidamountonline, 0)             AS paidamountonline,
					  |       COALESCE (g2.paidamountoffline, 0)            AS paidamountoffline,
					  |       0                                             AS inquiryvisitid,
					  |       '[Unknown]'                                   AS inquiryfullvisitorid,
					  |       'undefined'                                   AS inquirymedium,
					  |       -1                                            AS inquirywebsitereferralmediumsessionid,
					  |       '[N/A]'                                       AS inquirysource,
					  |       0                                             AS bookingrequestvisitid,
					  |       '[Unknown]'                                   AS bookingrequestfullvisitorid,
					  |       'Undefined'                                   AS bookingrequestmedium,
					  |       -1                                            AS bookingrequestwebsitereferralmediumsessionid,
					  |       '[N/A]'                                       AS bookingrequestsource,
					  |       0                                             AS calculatedvisitid,
					  |       '[Unknown]'                                   AS calculatedfullvisitorid,
					  |       'Undefined'                                   AS calculatedbookingmedium,
					  |       -1                                            AS calculatedbookingwebsitereferralmediumsessionid,
					  |       '[N/A]'                                       AS calculatedbookingsource
					  |FROM   dfRes r
					  |       LEFT JOIN dfgbv g
					  |              ON r.reservationid = g.reservationid
					  |       LEFT JOIN GBV2 g2
					  |              ON r.reservationid = g2.reservationid
					  |       LEFT JOIN tmpbookingvaluesource s
					  |              ON g.valuesource = s.bookingvaluesourcename
					  |       LEFT JOIN tmpcurrency c
					  |              ON g.currencycode = c.currencycode
					  |       LEFT JOIN tmpcurrencyconversion ccusd
					  |              ON g.currencycode = ccusd.fromcurrency
					  |                 AND ccusd.tocurrency = 'USD'
					  |                 AND r.createdate BETWEEN ccusd.rowstartdate AND
					  |                                          ccusd.rowenddate
					  |       LEFT JOIN tmpcurrencyconversion ccbc
					  |              ON r.brandcurrencycode = ccbc.tocurrency
					  |                 AND g.currencycode = ccbc.fromcurrency
					  |                 AND r.createdate BETWEEN ccbc.rowstartdate AND ccbc.rowenddate
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfFinal1 = sql(query)
				dfFinal1.persist
				dfFinal1 = createCheckPoint(spark, jc, dfFinal1, "Insert #final")
				dfFinal1.createOrReplaceTempView("dfFinal1")


				query =
					"""
					  |SELECT tto.reservationid,
					  |       toi.travelerorderid,
					  |       r.brandcurrencycode,
					  |       COALESCE (cc.conversionrate, 1)                    AS conversionrate,
					  |       COALESCE (q.currencycode, toi.currencycode)        AS localcurrencycode,
					  |       Sum (toi.amount * COALESCE (cc.conversionrate, 1)) AS standalonevasamt
					  |FROM   tmptravelerorder tto
					  |       INNER JOIN tmptravelerorderitem toi
					  |               ON toi.travelerorderid = tto.travelerorderid
					  |       INNER JOIN tmpproduct p
					  |               ON toi.productid = p.productid
					  |       INNER JOIN tmpquote q
					  |               ON tto.quoteid = q.quoteid
					  |       INNER JOIN tmpproductfulfillment pf
					  |               ON pf.externalrefid = q.quoteguid
					  |                  AND pf.externalreftype = 'quote'
					  |                  AND pf.producttypeguid = p.productguid
					  |       INNER JOIN dfRes r
					  |               ON q.reservationid = r.reservationid
					  |       LEFT JOIN tmpcurrencyconversion cc
					  |              ON toi.currencycode = cc.fromcurrency
					  |                 AND q.currencycode = cc.tocurrency
					  |                 AND r.createdate BETWEEN cc.rowstartdate AND cc.rowenddate
					  |       LEFT JOIN (SELECT q.quoteid,
					  |                                      q.quoteitemid,
					  |                                      q.quoteitemcreateddate,
					  |                                      tp.quoteitemtype,
					  |                                      q.amountlocalcurrency,
					  |                                      q.reservationid
					  |                               FROM   tmpquotefact q
					  |                                      INNER JOIN tmpTravelerproduct tp
					  |                                              ON q.travelerproductid = tp.travelerproductid
					  |                                      INNER JOIN dfRes r
					  |                                              ON q.reservationid = r.reservationid
					  |                               WHERE  q.quotecreateddate >= '2013-01-01'
					  |                                      AND q.quoteactiveflag = 1
					  |                                      AND q.quoteitemactiveflag = 1) qf
					  |       ON tto.reservationid = qf.reservationid AND qf.quoteitemtype = p.productguid
					  |WHERE  p.productguid NOT IN ('TRAVELER_PROTECTION', 'COMMISSION')
					  |       AND p.grossbookingvalueproductcategory = 'VAS'
					  |       AND toi.travelerorderitemstatus = 'PAID'
					  |       AND qf.reservationid IS NULL AND qf.quoteitemtype IS NULL
					  |GROUP  BY tto.reservationid,
					  |          toi.travelerorderid,
					  |          COALESCE (cc.conversionrate, 1),
					  |          COALESCE (q.currencycode, toi.currencycode),
					  |          r.brandcurrencycode
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfVas = sql(query)
				dfVas.persist
				dfVas = createCheckPoint(spark, jc, dfVas, "#vas")
				dfVas.createOrReplaceTempView("dfVas")


				query =
					"""
					  |SELECT v.reservationid,
					  |       v.localcurrencycode,
					  |       v.brandcurrencycode,
					  |       Round (Sum (v.standalonevasamt), 6) AS standalonevasamt,
					  |       COALESCE (Round (Sum (topp.paidamount * v.conversionrate), 6), 0) AS paidamount,
					  |       COALESCE (Min (topp.paymentmethodtypeid), 0) AS paymentmethodtypeid,
					  |       COALESCE (Round (Sum (topp.paidamountonline * v.conversionrate), 6), 0) AS paidamountonline,
					  |       COALESCE (Round (Sum (topp.paidamountoffline * v.conversionrate), 6), 0) AS paidamountoffline
					  |FROM   dfVas v
					  |       LEFT JOIN (SELECT tp.travelerorderid,
					  |                         Min (tp.paymentmethodtypeid) AS paymentmethodtypeid,
					  |                         Sum (topd.distributedamount) AS paidamount,
					  |                         Sum (CASE  WHEN pmt.paymentcollectiontype = 'online' THEN topd.distributedamount ELSE 0 END) AS paidamountonline,
					  |                         Sum (CASE  WHEN pmt.paymentcollectiontype = 'offline' THEN topd.distributedamount ELSE 0 END) AS paidamountoffline
					  |                             FROM tmpTravelerorderpayment tp
					  |                             JOIN tmpTravelerorderpaymentdistribution topd
					  |                             ON topd.travelerorderpaymentid = tp.travelerorderpaymentid
					  |                             JOIN tmpTravelerorderpaymentdistributionamounttype at
					  |                             ON topd.travelerorderpaymentdistributionamounttypeid = at.travelerorderpaymentdistributionamounttypeid
					  |                             JOIN tmpTravelerorderpaymentdistributiontype topdt
					  |                             ON topd.travelerorderpaymentdistributiontypeid = topdt.travelerorderpaymentdistributiontypeid
					  |                             JOIN tmpTravelerorderpaymentdistributiontoaccounttype act
					  |                             ON topd.travelerorderpaymentdistributiontoaccounttypeid = act.travelerorderpaymentdistributiontoaccounttypeid
					  |                             JOIN tmpPaymentmethodtype pmt
					  |                             ON pmt.paymentmethodtypeid = tp.paymentmethodtypeid
					  |                             WHERE  at.travelerorderpaymentdistributionamounttypename = 'credit'
					  |                             AND act.travelerorderpaymentdistributiontoaccounttypename = 'vendor'
					  |                             AND tp.travelerorderpaymenttypename = 'payment'
					  |                             AND tp.travelerorderpaymentstatusname IN ( 'settled', 'remitted', 'paid' )
					  |                             AND topdt.travelerorderpaymentdistributiontypename = 'service'
					  |                             GROUP  BY tp.travelerorderid
					  |                             ) topp
					  |ON v.travelerorderid = topp.travelerorderid
					  |GROUP  BY v.reservationid, v.localcurrencycode, v.brandcurrencycode
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfVas2 = sql(query)
				dfVas2.persist
				dfVas2 = createCheckPoint(spark, jc, dfVas2, "#vas2")
				dfVas2.createOrReplaceTempView("dfvas2")


				query =
					"""
					  |select
					  |        bf.*,
					  |        bf.vasamount + standalonevasamt as vasamount_TMP,
					  |        bf.orderamount + standalonevasamt as orderamount_TMP,
					  |        bf.bookingvalue + standalonevasamt as bookingvalue_TMP,
					  |        11  as bookingvaluesourceid_TMP,
					  |        bf.paidamount + v.paidamount as baidamount_TMP
					  |    from
					  |        {SOURCE_DF} bf
					  |    join
					  |        tmpCurrency c
					  |            on bf.localcurrencyid = c.currencyid
					  |    join
					  |        dfVas2 v
					  |            on bf.reservationid = v.reservationid
					  |            and c.currencycode = v.localcurrencycode
					  |    join
					  |        tmpBookingvaluesource bvs
					  |            on bf.bookingvaluesourceid = bvs.bookingvaluesourceid
					  |    where
					  |        bvs.bookingvaluesourcetype = 'Order'
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal2 = updateDataFrame(spark, jc, dfFinal1, "dfFinal1", query, "#final vas update same currency")
				// var dfFinal2 = updateDataFrame(spark, jc, dfFinal1, "dfFinal1", query, "#final vas update same currency")
				// dfFinal2 = createCheckPoint(spark, jc, dfFinal2)
				dfFinal2.createOrReplaceTempView("dfFinal2")
				dfFinal1.unpersist

				query =
					"""
					  |select
					  |        bf.*,
					  |        bf.vasamount + standalonevasamt as vasamount_TMP,
					  |        bf.orderamount + standalonevasamt as orderamount_TMP,
					  |        bf.bookingvalue + standalonevasamt as bookingvalue_TMP,
					  |        10  as bookingvaluesourceid_TMP,
					  |        vc.currencyid  as localcurrencyid_TMP,
					  |        cc.currencyconversionid  as currencyconversionidusd_TMP,
					  |        v.paymentmethodtypeid  as paymentmethodtypeid_TMP,
					  |        bf.paidamount + v.paidamount as paidamount_TMP,
					  |        bc.currencyconversionid  as currencyconversionidbrandcurrency_TMP,
					  |        bf.paidamountonline + v.paidamountonline as paidamountonline_TMP,
					  |        bf.paidamountoffline + v.paidamountoffline as paidamountoffline_TMP
					  |    from
					  |        {SOURCE_DF} bf
					  |    inner join
					  |        tmpCurrency c
					  |            on bf.localcurrencyid = c.currencyid
					  |    inner join
					  |        dfVas2 v
					  |            on bf.reservationid = v.reservationid
					  |            and c.currencycode != v.localcurrencycode
					  |    inner join
					  |        tmpCurrency vc
					  |            on v.localcurrencycode = vc.currencycode
					  |    inner join
					  |        tmpCurrencyconversion cc
					  |            on vc.currencycode = cc.fromcurrency
					  |            and cc.tocurrency = 'USD'
					  |            and bf.bookingdateid between cc.rowstartdate and cc.rowenddate
					  |    inner join
					  |        tmpCurrencyconversion bc
					  |            on v.brandcurrencycode = bc.tocurrency
					  |            and v.localcurrencycode = bc.fromcurrency
					  |            and bf.bookingdateid between bc.rowstartdate and bc.rowenddate
					  |    where
					  |        bf.bookingvaluesourceid = 1
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal3 = updateDataFrame(spark, jc, dfFinal2, "dfFinal2", query, "#final vas update diff currency")
				// var dfFinal3 = updateDataFrame(spark, jc, dfFinal2, "dfFinal2", query, "#final vas update diff currency")
				// dfFinal3 = createCheckPoint(spark, jc, dfFinal3)
				dfFinal3.createOrReplaceTempView("dfFinal3")
				dfFinal2.unpersist


				query =
					s"""
					   |select
					   |            r.arrivaldate ,
					   |            r.createdate ,
					   |            r.bookingcategoryid ,
					   |            null AS bookingdateid,
					   |            r.bookingreporteddate AS bookingreporteddateid,
					   |            null AS cancelleddateid,
					   |            f.brandid ,
					   |            f.listingunitid ,
					   |            r.reservationid ,
					   |            f.siteid ,
					   |            f.traveleremailid ,
					   |            f.visitorid ,
					   |            f.onlinebookingprovidertypeid ,
					   |            f.reservationavailabilitystatustypeid ,
					   |            f.reservationpaymentstatustypeid ,
					   |            f.devicecategorysessionid ,
					   |            r.inquiryserviceentryguid ,
					   |            f.inquiryid ,
					   |            f.customerid ,
					   |            f.listingid ,
					   |            'UNK' AS brandcurrencycode,
					   |            f.regionid ,
					   |            0 AS bookednightscount,
					   |            f.brandattributesid ,
					   |            f.customerattributesid ,
					   |            f.listingattributesid ,
					   |            f.listingunitattributesid ,
					   |            f.subscriptionid ,
					   |            f.paymenttypeid ,
					   |            f.listingchannelid ,
					   |            f.persontypeid ,
					   |            0 AS bookingcount,
					   |            f.bookingvaluesourceid ,
					   |            f.displayregionid ,
					   |            0 AS rentalamount,
					   |            0 AS refundabledamagedepositamount,
					   |            0 AS servicefeeamount,
					   |            0 AS taxamount,
					   |            0 AS vasamount,
					   |            0 AS orderamount,
					   |            0 AS paidamount,
					   |            0 AS refundamount,
					   |            0 AS bookingvalue,
					   |            f.localcurrencyid ,
					   |            f.currencyconversionidusd ,
					   |            f.currencyconversionidbrandcurrency ,
					   |            0 AS commissionamount,
					   |            f.bookingchannelid AS bookingchannelid,
					   |            f.paymentmethodtypeid AS paymentmethodtypeid,
					   |            0 AS paidamountoffline,
					   |            0 AS paidamountonline,
					   |            f.strategicdestinationid ,
					   |            f.strategicdestinationattributesid ,
					   |            f.visitid ,
					   |            f.fullvisitorid ,
					   |            f.websitereferralmediumsessionid ,
					   |            f.inquiryvisitid ,
					   |            f.inquiryfullvisitorid ,
					   |            f.inquirymedium ,
					   |            f.inquirywebsitereferralmediumsessionid ,
					   |            f.inquirysource ,
					   |            f.bookingrequestvisitid ,
					   |            f.bookingrequestfullvisitorid ,
					   |            f.bookingrequestmedium ,
					   |            f.bookingrequestwebsitereferralmediumsessionid ,
					   |            f.bookingrequestsource ,
					   |            f.calculatedvisitid ,
					   |            f.calculatedfullvisitorid ,
					   |            f.calculatedbookingmedium ,
					   |            f.calculatedbookingwebsitereferralmediumsessionid ,
					   |            f.calculatedbookingsource
					   |        from
					   |            tmpReservation r
					   |        inner join
					   |            tmpBookingfact f
					   |                on r.reservationid = f.reservationid
					   |                and r.createdate = f.reservationcreatedateid
					   |        where
					   |            r.createdate >= '$rollingTwoYears'
					   |            and (
					   |                r.activeflag = 0
					   |                or (
					   |                    r.activeflag = 1
					   |                    and r.bookingcategoryid = 1
					   |                )
					   |            )
					   |            and r.dwupdatedatetime between '$startDate' and '$endDate'
					   |            and f.bookingcount = 1
                     """.stripMargin

				log.info(s"Running Query: $query")
				var tmpFinal = sql(query)
				tmpFinal.persist
				tmpFinal = createCheckPoint(spark, jc, tmpFinal, "Insert #final BookingFact")
				tmpFinal.createOrReplaceTempView("tmpFinal")


				query =
					"""
					  |SELECT ArrivalDate, CreateDate, BookingCategoryId, BookingDateId, BookingReportedDateId, CancelledDateId, BrandId, ListingUnitId, ReservationId, SiteId, TravelerEmailId, VisitorId, OnlineBookingProviderTypeId, ReservationAvailabilityStatusTypeId, ReservationPaymentStatusTypeId, DeviceCategorySessionId, InquiryServiceEntryGUID, InquiryId, CustomerId, ListingId, BrandCurrencyCode, RegionId, BookedNightsCount, BrandAttributesId, CustomerAttributesId, ListingAttributesId, ListingUnitAttributesId, Subscriptionid, PaymentTypeId, ListingChannelId, PersonTypeId, BookingCount, BookingValueSourceId, DisplayRegionId, RentalAmount, RefundableDamageDepositAmount, ServiceFeeAmount, TaxAmount, VasAmount, OrderAmount, PaidAmount, RefundAmount, BookingValue, LocalCurrencyId, CurrencyConversionIdUSD, CurrencyConversionIdBrandCurrency, CommissionAmount, BookingChannelId, PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline, StrategicDestinationId, StrategicDestinationAttributesId, VisitId, FullVisitorId, WebsiteReferralMediumSessionId, InquiryVisitId, InquiryFullVisitorId, InquiryMedium, InquiryWebsiteReferralMediumSessionId, InquirySource, BookingRequestVisitId, BookingRequestFullVisitorId, BookingRequestMedium, BookingRequestWebsiteReferralMediumSessionID, BookingRequestSource, CalculatedVisitId, CalculatedFullVisitorId, CalculatedBookingMedium, CalculatedBookingWebsiteReferralMediumSessionID, CalculatedBookingSource FROM tmpFinal
					  |UNION ALL
					  |SELECT ArrivalDate, CreateDate, BookingCategoryId, BookingDateId, BookingReportedDateId, CancelledDateId, BrandId, ListingUnitId, ReservationId, SiteId, TravelerEmailId, VisitorId, OnlineBookingProviderTypeId, ReservationAvailabilityStatusTypeId, ReservationPaymentStatusTypeId, DeviceCategorySessionId, InquiryServiceEntryGUID, InquiryId, CustomerId, ListingId, BrandCurrencyCode, RegionId, BookedNightsCount, BrandAttributesId, CustomerAttributesId, ListingAttributesId, ListingUnitAttributesId, Subscriptionid, PaymentTypeId, ListingChannelId, PersonTypeId, BookingCount, BookingValueSourceId, DisplayRegionId, RentalAmount, RefundableDamageDepositAmount, ServiceFeeAmount, TaxAmount, VasAmount, OrderAmount, PaidAmount, RefundAmount, BookingValue, LocalCurrencyId, CurrencyConversionIdUSD, CurrencyConversionIdBrandCurrency, CommissionAmount, BookingChannelId, PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline, StrategicDestinationId, StrategicDestinationAttributesId, VisitId, FullVisitorId, WebsiteReferralMediumSessionId, InquiryVisitId, InquiryFullVisitorId, InquiryMedium, InquiryWebsiteReferralMediumSessionId, InquirySource, BookingRequestVisitId, BookingRequestFullVisitorId, BookingRequestMedium, BookingRequestWebsiteReferralMediumSessionID, BookingRequestSource, CalculatedVisitId, CalculatedFullVisitorId, CalculatedBookingMedium, CalculatedBookingWebsiteReferralMediumSessionID, CalculatedBookingSource FROM dfFinal3
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal4 = sql(query)
				// var dfFinal4 = sql(query)
				dfFinal4.persist
				// dfFinal4 = createCheckPoint(spark, jc, dfFinal4)
				dfFinal3.unpersist

				query =
					"""
					  |select
					  |        bf.*,
					  |        coalesce(i.visitid, 0) as inquiryvisitid_TMP,
					  |        coalesce(i.fullvisitorid, '[Unknown]') as inquiryfullvisitorid_TMP,
					  |        wrms.websitereferralmedium  as inquirymedium_TMP,
					  |        wrms.websitereferralmediumsessionid  as inquirywebsitereferralmediumsessionid_TMP
					  |    from
					  |        {SOURCE_DF} bf
					  |    inner join
					  |        tmpInquiryslafact i
					  |            on i.inquiryid = bf.inquiryid
					  |    inner join
					  |        tmpWebsitereferralmediumsession wrms
					  |            on wrms.websitereferralmediumsessionid = i.websitereferralmediumsessionid
					  |    where
					  |        bf.inquirymedium <> wrms.websitereferralmedium
					  |        or bf.inquirywebsitereferralmediumsessionid <> wrms.websitereferralmediumsessionid
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal5 = updateDataFrame(spark, jc, dfFinal4, "dfFinal4", query, "Update #final InquiryMedium, InquiryWebsiteReferralMediumSessionID")
				// var dfFinal5 = updateDataFrame(spark, jc, dfFinal4, "dfFinal4", query, "Update #final InquiryMedium, InquiryWebsiteReferralMediumSessionID")
				// dfFinal5 = createCheckPoint(spark, jc, dfFinal5)
				dfFinal5.createOrReplaceTempView("dfFinal5")
				dfFinal4.unpersist


				query =
					"""
					  |select
					  |        bf.*,
					  |        vf.source AS inquirysource_TMP
					  |    from
					  |        {SOURCE_DF} bf
					  |    inner join
					  |        tmpInquiryslafact i
					  |            on i.inquiryid = bf.inquiryid
					  |    inner join
					  |        tmpVisitorfact vf
					  |            on vf.dateid = cast(from_unixtime(unix_timestamp(i.inquirydate, 'yyyy-MM-dd'), 'yyyyMMdd') AS int)
					  |        and vf.visitid = i.visitid
					  |        and vf.fullvisitorid = i.fullvisitorid
					  |    where
					  |        bf.inquirysource <> vf.source
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal6 = updateDataFrame(spark, jc, dfFinal5, "dfFinal5", query, "Update #final InquirySource")
				// var dfFinal6 = updateDataFrame(spark, jc, dfFinal5, "dfFinal5", query, "Update #final InquirySource")
				// dfFinal6 = createCheckPoint(spark, jc, dfFinal6)
				dfFinal6.createOrReplaceTempView("dfFinal6")
				dfFinal5.unpersist

				query =
					"""
					  |select
					  |        fq.reservationid ,
					  |        fq.quoteid
					  |    from
					  |        ( select
					  |            qf.reservationid ,
					  |            qf.quoteid ,
					  |            row_number() over (partition by q.reservationid order by qf.quoteid) AS rownum
					  |        from
					  |            dfFinal6 bf
					  |        inner join
					  |            tmpQuotefact qf
					  |                on qf.reservationid = bf.reservationid
					  |        inner join
					  |            tmpQuote q
					  |                on qf.quoteid = q.quoteid
					  |        inner join
					  |            tmpReservation r
					  |                on qf.reservationid = r.reservationid
					  |        where
					  |            qf.reservationid > -1
					  |            and q.bookingtypeid in (2 , 3)
					  |            and r.activeflag = 1 ) fq
					  |    where
					  |        fq.rownum = 1
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfFirstQuote = sql(query)
				dfFirstQuote.persist
				dfFirstQuote = createCheckPoint(spark, jc, dfFirstQuote, "Create #FirstQuote")
				dfFirstQuote.createOrReplaceTempView("dfFirstQuote")

				query =
					"""
					  |select
					  |        ms.reservationid ,
					  |        ms.quoteid ,
					  |        ms.visitid ,
					  |        ms.fullvisitorid ,
					  |        ms.websitereferralmediumsessionid
					  |   from
					  |        ( select
					  |            mq.reservationid ,
					  |            qfa.quoteid ,
					  |            qfa.visitid ,
					  |            qfa.fullvisitorid ,
					  |            qfa.websitereferralmediumsessionid ,
					  |            row_number() over ( partition by mq.reservationid order by qfa.websitereferralmediumsessionid desc ) AS rownum
					  |        from
					  |            dfFirstquote mq
					  |        inner join
					  |            tmpQuotefact qfa
					  |                on mq.quoteid = qfa.quoteid ) ms
					  |    where
					  |        ms.rownum = 1
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfMaxSession = sql(query)
				dfMaxSession.persist
				dfMaxSession = createCheckPoint(spark, jc, dfMaxSession, "Create #MaxSession")
				dfMaxSession.createOrReplaceTempView("dfMaxSession")


				query =
					"""
					  |select
					  |        *
					  |    from
					  |        ( select
					  |            br.* ,
					  |            vf.source ,
					  |            vf.visitstarttime ,
					  |            row_number() over (partition by br.reservationid order by vf.visitstarttime desc) AS rownum
					  |        from
					  |            dfMaxSession br
					  |        inner join
					  |            tmpQuotefact qf
					  |                on qf.reservationid = br.reservationid
					  |                and qf.quoteid = br.quoteid
					  |                and qf.websitereferralmediumsessionid = br.websitereferralmediumsessionid
					  |        left join
					  |            tmpVisitorfact vf
					  |                on qf.visitid = vf.visitid
					  |                and qf.fullvisitorid = vf.fullvisitorid
					  |                and qf.quotecreateddate = vf.visitdate ) fs
					  |    where
					  |        fs.rownum = 1
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfSession = sql(query)
				dfSession.persist
				dfSession = createCheckPoint(spark, jc, dfSession, "Create #session")
				dfSession.createOrReplaceTempView("dfSession")


				query =
					"""
					  |select
					  |        bf.*,
					  |        coalesce ( s.visitid, 0 ) as bookingrequestvisitid_TMP,
					  |        coalesce ( s.fullvisitorid, '[Unknown]' ) as bookingrequestfullvisitorid_TMP,
					  |        wrms.websitereferralmedium  as bookingrequestmedium_TMP,
					  |        wrms.websitereferralmediumsessionid  as bookingrequestwebsitereferralmediumsessionid_TMP,
					  |        coalesce ( s.source, '[N/A]' ) as bookingrequestsource_TMP
					  |    from
					  |        {SOURCE_DF} bf
					  |    inner join
					  |        dfSession s
					  |            on s.reservationid = bf.reservationid
					  |    inner join
					  |        tmpWebsitereferralmediumsession wrms
					  |            on s.websitereferralmediumsessionid = wrms.websitereferralmediumsessionid
					  |    where
					  |        bf.bookingrequestwebsitereferralmediumsessionid = -1
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal7 = updateDataFrame(spark, jc, dfFinal6, "dfFinal6", query, "Update #final BookingRequest columns")
				// var dfFinal7 = updateDataFrame(spark, jc, dfFinal6, "dfFinal6", query, "Update #final BookingRequest columns")
				// dfFinal7 = createCheckPoint(spark, jc, dfFinal7)
				dfFinal7.createOrReplaceTempView("dfFinal7")
				dfFinal6.unpersist

				query =
					"""
					  |select
					  |        bf.reservationid ,
					  |        bf.websitereferralmediumsessionid AS bookingwebsitereferralmediumsessionid,
					  |        wrms.marketingmedium AS bookingmediumcase,
					  |        coalesce ( vf.source,'[N/A]' ) AS bookingmediumsource
					  |    from
					  |        dfFinal7 bf
					  |    inner join
					  |        tmpWebsitereferralmediumsession wrms
					  |            on wrms.websitereferralmediumsessionid = bf.websitereferralmediumsessionid
					  |    left join
					  |        tmpVisitorfact vf
					  |            on bf.visitid = vf.visitid
					  |            and bf.fullvisitorid = vf.fullvisitorid
					  |            and bf.bookingdateid = vf.visitdate
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfBookingMedium = sql(query)
				dfBookingMedium.persist
				dfBookingMedium = createCheckPoint(spark, jc, dfBookingMedium, "Create #BookingMedium")
				dfBookingMedium.createOrReplaceTempView("dfBookingMedium")

				query =
					"""
					  |select
					  |        bf.reservationid ,
					  |        wrmsbr.marketingmedium AS bookingrequestmediumcase,
					  |        wrmsi.marketingmedium AS inquirymediumcase
					  |    from
					  |        dfFinal7 bf
					  |    inner join
					  |        tmpWebsitereferralmediumsession wrmsbr
					  |            on wrmsbr.websitereferralmediumsessionid = bf.bookingrequestwebsitereferralmediumsessionid
					  |    inner join
					  |        tmpWebsitereferralmediumsession wrmsi
					  |            on wrmsi.websitereferralmediumsessionid = bf.inquirywebsitereferralmediumsessionid
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfOtherMediums = sql(query)
				dfOtherMediums.persist
				dfOtherMediums = createCheckPoint(spark, jc, dfOtherMediums, "Create #OtherMediums")
				dfOtherMediums.createOrReplaceTempView("dfOtherMediums")


				query =
					"""
					  |select
					  |        bf.*,
					  |        bf.visitid  as calculatedvisitid_TMP,
					  |        bf.fullvisitorid  as calculatedfullvisitorid_TMP,
					  |        bm.bookingmediumcase  as calculatedbookingmedium_TMP,
					  |        bm.bookingmediumsource  as calculatedbookingsource_TMP,
					  |        bm.bookingwebsitereferralmediumsessionid  as calculatedbookingwebsitereferralmediumsessionid_TMP
					  |    from
					  |        {SOURCE_DF} bf
					  |    inner join
					  |        dfBookingMedium bm
					  |            on bm.reservationid = bf.reservationid
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal8 = updateDataFrame(spark, jc, dfFinal7, "dfFinal7", query, "Update #final calculated fields when Booking is not undefined")
				// var dfFinal8 = updateDataFrame(spark, jc, dfFinal7, "dfFinal7", query, "Update #final calculated fields when Booking is not undefined")
				// dfFinal8 = createCheckPoint(spark, jc, dfFinal8)
				dfFinal8.createOrReplaceTempView("dfFinal8")
				dfFinal7.unpersist


				query =
					"""
					  |select
					  |        bf.*,
					  |        bf.bookingrequestvisitid  as calculatedvisitid_TMP,
					  |        bf.bookingrequestfullvisitorid  as calculatedfullvisitorid_TMP,
					  |        om.bookingrequestmediumcase  as calculatedbookingmedium_TMP,
					  |        bf.bookingrequestsource  as calculatedbookingsource_TMP,
					  |        bf.bookingrequestwebsitereferralmediumsessionid  as calculatedbookingwebsitereferralmediumsessionid_TMP
					  |    from
					  |        {SOURCE_DF} bf
					  |    inner join
					  |        dfOtherMediums om
					  |            on om.reservationid = bf.reservationid
					  |    where
					  |        bf.calculatedbookingmedium = 'Undefined'
					  |        and om.bookingrequestmediumcase <> 'Undefined'
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal9 = updateDataFrame(spark, jc, dfFinal8, "dfFinal8", query, "Update #final calculated fields when Booking medium is undefined")
				// var dfFinal9 = updateDataFrame(spark, jc, dfFinal8, "dfFinal8", query, "Update #final calculated fields when Booking medium is undefined")
				// dfFinal9 = createCheckPoint(spark, jc, dfFinal9)
				dfFinal9.createOrReplaceTempView("dfFinal9")
				dfFinal8.unpersist


				query =
					"""
					  |select
					  |        bf.*,
					  |        bf.inquiryvisitid  as calculatedvisitid_TMP,
					  |        bf.inquiryfullvisitorid  as calculatedfullvisitorid_TMP,
					  |        om.inquirymediumcase  as calculatedbookingmedium_TMP,
					  |        bf.inquirysource  as calculatedbookingsource_TMP,
					  |        bf.inquirywebsitereferralmediumsessionid  as calculatedbookingwebsitereferralmediumsessionid_TMP
					  |    from
					  |        {SOURCE_DF} bf
					  |    inner join
					  |        dfOtherMediums om
					  |            on om.reservationid = bf.reservationid
					  |    where
					  |        bf.calculatedbookingmedium = 'Undefined'
					  |        and om.inquirymediumcase <> 'Undefined'
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinal10 = updateDataFrame(spark, jc, dfFinal9, "dfFinal9", query, "Update #final calculated fields when Booking Request medium is undefined")
				dfFinal10.createOrReplaceTempView("dfFinal10")
				dfFinal9.unpersist

				query =
					"""
					  |SELECT
					  |      bf.*
					  |     ,appid
					  |     ,rentalid
					  |     ,rentalnumber
					  |     ,apiuuid as listinguuid
					  |     ,listingunituuid
					  |     ,rentalunitid
					  |     ,advertiseruuid
					  |     ,subscriptionkey
					  |FROM dfFinal10 bf
					  |LEFT JOIN tmpListing l ON bf.listingid = l.listingid
					  |LEFT JOIN tmpListingunit lu ON bf.listingunitid = lu.listingunitid
					  |LEFT JOIN tmpCustomer c ON bf.customerid = c.customerid
					  |LEFT JOIN tmpSubscription s ON bf.subscriptionid = s.subscriptionid
					""".stripMargin

				log.info(s"Running Query: $query")
				var dfFinal = sql(query)
				dfFinal = dataFrameOutputFiles(dfFinal, "subscriptionid", Array("subscriptionid"), Map("-1" -> 47), 1)
				dfFinal.persist
				dfFinal = createCheckPoint(spark, jc, dfFinal)
				dfFinal.createOrReplaceTempView("dfFinal")
				dfFinal10.unpersist

				query =
					"""
					  |SELECT
					  | COALESCE(fin.ReservationId, f.ReservationId) AS ReservationId
					  |,COALESCE(fin.BookingDateId, f.BookingDateId) AS BookingDateId
					  |,COALESCE(fin.BookingReportedDateId, f.BookingReportedDateId) AS BookingReportedDateId
					  |,COALESCE(fin.CancelledDateId, f.CancelledDateId) AS CancelledDateId
					  |,COALESCE(fin.CreateDate, f.ReservationCreateDateId) AS ReservationCreateDateId
					  |,COALESCE(fin.BookingCategoryId, f.BookingCategoryId) AS BookingCategoryId
					  |,COALESCE(fin.BookingValueSourceId, f.BookingValueSourceId) AS BookingValueSourceId
					  |,COALESCE(fin.BrandAttributesId, f.BrandAttributesId) AS BrandAttributesId
					  |,COALESCE(fin.BrandId, f.BrandId) AS BrandId
					  |,COALESCE(fin.CurrencyConversionIdBrandCurrency, f.CurrencyConversionIdBrandCurrency) AS CurrencyConversionIdBrandCurrency
					  |,COALESCE(fin.CurrencyConversionIdUSD, f.CurrencyConversionIdUSD) AS CurrencyConversionIdUSD
					  |,COALESCE(fin.CustomerAttributesId, f.CustomerAttributesId) AS CustomerAttributesId
					  |,COALESCE(fin.CustomerId, f.CustomerId) AS CustomerId
					  |,COALESCE(fin.DeviceCategorySessionId, f.DeviceCategorySessionId) AS DeviceCategorySessionId
					  |,COALESCE(fin.DisplayRegionId, f.DisplayRegionId) AS DisplayRegionId
					  |,COALESCE(fin.FullVisitorId, f.FullVisitorId) AS FullVisitorId
					  |,COALESCE(fin.InquiryId, f.InquiryId) AS InquiryId
					  |,COALESCE(fin.ListingAttributesId, f.ListingAttributesId) AS ListingAttributesId
					  |,COALESCE(fin.ListingChannelId, f.ListingChannelId) AS ListingChannelId
					  |,COALESCE(fin.ListingId, f.ListingId) AS ListingId
					  |,COALESCE(fin.ListingUnitAttributesId, f.ListingUnitAttributesId) AS ListingUnitAttributesId
					  |,COALESCE(fin.ListingUnitId, f.ListingUnitId) AS ListingUnitId
					  |,COALESCE(fin.LocalCurrencyId, f.LocalCurrencyId) AS LocalCurrencyId
					  |,COALESCE(fin.OnlineBookingProviderTypeId, f.OnlineBookingProviderTypeId) AS OnlineBookingProviderTypeId
					  |,COALESCE(fin.PaymentTypeId, f.PaymentTypeId) AS PaymentTypeId
					  |,COALESCE(fin.PersonTypeId, f.PersonTypeId) AS PersonTypeId
					  |,COALESCE(fin.RegionId, f.RegionId) AS RegionId
					  |,COALESCE(fin.ReservationAvailabilityStatusTypeId, f.ReservationAvailabilityStatusTypeId) AS ReservationAvailabilityStatusTypeId
					  |,COALESCE(fin.ReservationPaymentStatusTypeId, f.ReservationPaymentStatusTypeId) AS ReservationPaymentStatusTypeId
					  |,COALESCE(fin.SiteId, f.SiteId) AS SiteId
					  |,COALESCE(fin.SubscriptionId, f.SubscriptionId) AS SubscriptionId
					  |,COALESCE(fin.TravelerEmailId, f.TravelerEmailId) AS TravelerEmailId
					  |,COALESCE(fin.VisitId, f.VisitId) AS VisitId
					  |,COALESCE(fin.VisitorId, f.VisitorId) AS VisitorId
					  |,COALESCE(fin.WebsiteReferralMediumSessionId, f.WebsiteReferralMediumSessionId) AS WebsiteReferralMediumSessionId
					  |,COALESCE(fin.BookingCount, f.BookingCount) AS BookingCount
					  |,COALESCE(fin.BookingValue, f.BookingValue) AS BookingValue
					  |,COALESCE(fin.OrderAmount, f.OrderAmount) AS OrderAmount
					  |,COALESCE(fin.RefundableDamageDepositAmount, f.RefundableDamageDepositAmount) AS RefundableDamageDepositAmount
					  |,COALESCE(fin.RentalAmount, f.RentalAmount) AS RentalAmount
					  |,COALESCE(fin.ServiceFeeAmount, f.ServiceFeeAmount) AS ServiceFeeAmount
					  |,COALESCE(fin.TaxAmount, f.TaxAmount) AS TaxAmount
					  |,COALESCE(fin.CommissionAmount, f.CommissionAmount) AS CommissionAmount
					  |,COALESCE(fin.VasAmount, f.VasAmount) AS VasAmount
					  |,COALESCE(fin.PaidAmount, f.PaidAmount) AS PaidAmount
					  |,COALESCE(fin.RefundAmount, f.RefundAmount) AS RefundAmount
					  |,COALESCE(f.DWCreateDateTime, CURRENT_TIMESTAMP()) AS DWCreateDateTime
					  |,CURRENT_TIMESTAMP() AS DWUpdateDateTime
					  |,COALESCE(fin.BookingChannelId, f.BookingChannelId) AS BookingChannelId
					  |,COALESCE(fin.PaymentMethodTypeId, f.PaymentMethodTypeId) AS PaymentMethodTypeId
					  |,COALESCE(fin.PaidAmountOnline, f.PaidAmountOnline) AS PaidAmountOnline
					  |,COALESCE(fin.PaidAmountOffline, f.PaidAmountOffline) AS PaidAmountOffline
					  |,COALESCE(fin.StrategicDestinationId, f.StrategicDestinationId) AS StrategicDestinationId
					  |,COALESCE(fin.StrategicDestinationAttributesId, f.StrategicDestinationAttributesId) AS StrategicDestinationAttributesId
					  |,COALESCE(fin.BookedNightsCount, f.BookedNightsCount) AS BookedNightsCount
					  |,COALESCE(fin.InquiryWebsiteReferralMediumSessionID, f.InquiryWebsiteReferralMediumSessionID) AS InquiryWebsiteReferralMediumSessionID
					  |,COALESCE(fin.InquiryMedium, f.InquiryMedium) AS InquiryMedium
					  |,COALESCE(fin.InquirySource, f.InquirySource) AS InquirySource
					  |,COALESCE(fin.BookingRequestWebsiteReferralMediumSessionId, f.BookingRequestWebsiteReferralMediumSessionId) AS BookingRequestWebsiteReferralMediumSessionId
					  |,COALESCE(fin.BookingRequestMedium, f.BookingRequestMedium) AS BookingRequestMedium
					  |,COALESCE(fin.BookingRequestSource, f.BookingRequestSource) AS BookingRequestSource
					  |,COALESCE(fin.CalculatedBookingWebsiteReferralMediumSessionId, f.CalculatedBookingWebsiteReferralMediumSessionId) AS CalculatedBookingWebsiteReferralMediumSessionId
					  |,COALESCE(fin.CalculatedBookingMedium, f.CalculatedBookingMedium) AS CalculatedBookingMedium
					  |,COALESCE(fin.CalculatedBookingSource, f.CalculatedBookingSource) AS CalculatedBookingSource
					  |,COALESCE(fin.BookingRequestFullVisitorId, f.BookingRequestFullVisitorId) AS BookingRequestFullVisitorId
					  |,COALESCE(fin.BookingRequestVisitId, f.BookingRequestVisitId) AS BookingRequestVisitId
					  |,COALESCE(fin.InquiryFullVisitorId, f.InquiryFullVisitorId) AS InquiryFullVisitorId
					  |,COALESCE(fin.InquiryVisitId, f.InquiryVisitId) AS InquiryVisitId
					  |,COALESCE(fin.CalculatedFullVisitorId, f.CalculatedFullVisitorId) AS CalculatedFullVisitorId
					  |,COALESCE(fin.CalculatedVisitId, f.CalculatedVisitId) AS CalculatedVisitId
					  |,COALESCE(fin.appid, f.appid) AS appid
					  |,COALESCE(fin.rentalid, f.rentalid) AS rentalid
					  |,COALESCE(fin.rentalnumber, f.rentalnumber) AS rentalnumber
					  |,COALESCE(fin.listinguuid, f.listinguuid) AS listinguuid
					  |,COALESCE(fin.listingunituuid, f.listingunituuid) AS listingunituuid
					  |,COALESCE(fin.rentalunitid, f.rentalunitid) AS rentalunitid
					  |,COALESCE(fin.advertiseruuid, f.advertiseruuid) AS advertiseruuid
					  |,COALESCE(fin.subscriptionkey, f.subscriptionkey) AS subscriptionkey
					  |FROM tmpBookingfact f
					  |FULL OUTER JOIN dfFinal fin
					  |ON f.ReservationId = fin.ReservationId AND f.ReservationCreateDateId = fin.CreateDate
					""".stripMargin

				log.info(s"Running Query: $query")
				val dfFinalBookingFact = sql(query).repartition(50)

				dfFinalBookingFact.persist

				val fCount = dfFinalBookingFact.count

				val fmsg = "BookingFact Merge - Final Count:"
				log.info(s"$fmsg $fCount")
				jc.logInfo(jobId, instanceId, fmsg, fCount)


				log.info(s"Saving Data to Temp Location: $tmpLocation")
				saveDataFrameToHdfs(dfFinalBookingFact, SaveMode.Overwrite, finalFormat, tmpLocation)
				// dfFinalBookingFact.write.mode(SaveMode.Overwrite).format(finalFormat).save(tmpLocation)

				log.info("Moving Data from Temp to Final Location")
				hdfsRemoveAndMove(dfs, dfc, tmpLocation, finalLocation)

				log.info("Check and Create Hive DDL")
				checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, finalFormat, finalLocation, getColsFromDF(dfFinalBookingFact))

				log.info("Purge Old Snapshots of Booking Fact")
				purgeHDFSPath(dfs, dfc, finalBaseLoc, bookingFactRetentionDays)

				log.info("Altering Table's Location to new Location")
				alterTableLocation(hiveMetaStore, targetDB, targetTable, dfs.getUri.toString + finalLocation)

				jc.logInfo(jobId, instanceId, "END BookingFact Process", -1)

				jc.endJob(instanceId, 1, startDate, endDate)
			}
		}
		finally {
			edwLoader.cleanup()
			cleanup()
		}

		def debug(df: DataFrame, dfName: String): Unit = {

				log.info(s"Debug Mode Enabled, writing temp data $df to HDFS Location: ${debugLocation + "/" + dfName}")
				saveDataFrameToHdfs(df, SaveMode.Overwrite, finalFormat, debugLocation + "/" + dfName)

		}

		def cleanup(): Unit = {
			log.info("Cleaning up CheckPoint Directory")
			hdfsRemove(dfs, dfc, checkPointDir)
		}
	}


}