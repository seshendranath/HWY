/*****************************************************************************************************************************************
Description:	This stored procedure will incrementally load DW_Facts.BookingFact

Dependecies:	BookingFact , Reservation, ListingUnit, Brand, Calendar, Inquiry, OnlineBookingProviderType, Channel, ListingAttributes, Subscription,
			BrandAttributes, CustomerAttributes, PersonType, ListingUnitAttributes, Quote, QuoteItem, QuoteFact, RatePlan, ListingUnitDailyRate, 
			ListingUnitRate, BookingValueSource, Currency, CurrencyConversion

Audit Info:
Change By			Date		JIRA#		Description
Katherine Raney		11/23/2015	Create
Katherine Raney		12/11/2015	ISANA-5515	UniqueKey Violation in Bookingfact
Madhu Yengala		12/14/2015	ISANA-5481	Add 'Quote Unk'Row in BookingValue
Madhu Yengala		12/23/2015	ISANA-5588	Rename ColumnNames
Logan Boyd			12/15/2015	ISANA-4924	Add BookingChannelId
Katherine Raney		1/13/2016	ISANA-5736	Add PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline 
Madhu Yengala		01/18/2016	ISANA-5741	Add DWUpdateDateTime
Madhu Yengala		01/27/2015	ISANA-5941	Add ETL Controllog to select,Update and Insert
Madhu Yengala		02/01/2016	ISANA-5979	BookingFact_Merge not detecting change in Reservation.BookingCategoryId
Katherine Raney		02/03/2016	ISANA-6110	Refining the GBV Estimation to filter out more rates that are obviously fake
Katherine Raney		02/12/2016	ISANA-6265	Making CancelledDate be NULL if the BookingCount goes to 0 
Katherine Raney		02/16/2016	ISANA-6278	Improve performance in final not-a-booking step and restrict to Reservation.CreateDate>='2014-01-01'
Katherine Raney		02/19/2016	ISANA-6404	Pick up ServiceFee amount from TOPD for EXTERNAL_SOR bookings. Pick up ServiceFee VAT amount for all bookings. 
Jeff Baird			2016-03-04	ISANA-6611	Add StrategicDestinationId and StrategicDestinationAttributesId columns
Katherine Raney		03/17/2016	ISANA-6492	Pick up standalone VAS not in QuoteItem
Katherine Raney		03/22/2016	ISANA-6642	Add parameters to assist in targeted processing
Jeff Baird			2016-05-12	ISANA-7417	Change logic for assigning destiniation ID values
Katherine Raney		2016-06-22	ISANA-7928	Remove reference to TravelerOrder.CurrencyCode, replace with TravelerOrderItem.CurrencyCode
Katherine Raney		2016-08-05	ISANA-7997	Modify lookup for SubscriptionId 
Katherine Raney		2016-09-21	ISANA-6696	Modify estimated booking value to utilize AverageKnownNightlyBookingValue 
Katherine Raney		2016-10-06	ISANA-8936	Add BookedNightCount to merge
Logan Boyd			2016-10-17	ISANA-9007	Adding InquiryMedium, InquirySource and InquiryWebsiteReferralMediumSessionID to BookingFact
MVinciguerra		2017-01-09	ISANA-9424	Added BookingRequestWebsiteReferralMediumId, BookingRequestMedium, BookingRequestSource, CalculatedBookingWebsiteReferralMediumId, CalculatedBookingMedium, CalculatedBookingSource
MVinciguerra		2017-04-18	AE-1459		Added VisitId/FullVisitorId for BookingRequest, Inquiry, and Calculated medium.
Logan Boyd			2017-05-17	AE-1851		Add Inquiry and BookingRequest VisitId/FullVisitorId to MERGE statement. Set VisitId and FullVisitorId from Reservation in #Final
*******************************************************************************************************************************************/
CREATE PROCEDURE [dbo].[PR_BookingFact_Merge]
	@startDate DATE = NULL
	, @endDate DATE = '9999-12-31'
	, @debug TINYINT = 0
AS
BEGIN

BEGIN TRY
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET XACT_ABORT ON;

	DECLARE
		@msg VARCHAR(255)
		, @RollingTwoYears DATE = DATEFROMPARTS(YEAR(DATEADD(YEAR, -2, GETDATE())), 1, 1)
		, @RowStartDate DATE = dbo.constRowStartDate()
		, @ProcessName NVARCHAR(75) = OBJECT_NAME(@@PROCID)
		, @currentDateTime DATETIME = GETDATE()
		, @ETLControlId INT
		, @rowCount INT
		, @QuoteAndTravelerOrderBookingValueSourceId INT = dbo.constBookingValueSourceTravelerOrder()
		, @TravelerOrderBookingValueSourceId INT = dbo.constBookingValueSourceQuoteAndTravelerOrder() ;

	/* Use 2 Days Back if @startDate is not supplied */
	SET @startDate = ISNULL(@startDate, CONVERT(DATE, DATEADD(DAY, -2, GETDATE())));

	SELECT @ETLControlId = ETLControlID FROM DW_Config.dbo.ETLControl WHERE ProcessName = @ProcessName;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Begin', -1;

	SET @msg = '@startDate: ' + CAST(@startDate AS VARCHAR(25)) + ' ; @endDate: ' + CAST(@endDate AS VARCHAR(25));
	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, @msg, -1;

	IF OBJECT_ID('tempdb..#resList') IS NOT NULL DROP TABLE #resList;
		
	CREATE TABLE #resList ( ReservationId INT );

	INSERT INTO #resList
		SELECT DISTINCT
			qf.ReservationId
		FROM
			dbo.QuoteFact qf ( NOLOCK )
		WHERE
			1 = 1
			AND qf.QuoteCreatedDate >= @RollingTwoYears
			AND qf.QuoteItemActiveFlag = 1
			AND qf.QuoteActiveFlag = 1
			AND qf.QuoteStatusId >= -1
			AND (qf.DWCreateDate BETWEEN CAST(@startDate AS DATETIME) AND CAST(@endDate AS DATETIME)
				OR DWLastUpdateDate BETWEEN CAST(@startDate AS DATETIME) AND CAST(@endDate AS DATETIME))
			AND EXISTS ( SELECT 1 FROM dbo.Reservation r ( NOLOCK ) WHERE r.ActiveFlag = 1 AND r.CreateDate >= @RollingTwoYears AND r.BookingCategoryId != 1 AND qf.ReservationId = r.ReservationId );
		
	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #resList Quote', @@ROWCOUNT;
	
	INSERT INTO #resList
	SELECT
		r.ReservationId
	FROM
		dbo.Reservation r ( NOLOCK )
	WHERE
		r.CreateDate >= @RollingTwoYears
		AND r.ActiveFlag = 1
		AND r.BookingCategoryId != 1
		AND r.DWUpdateDateTime BETWEEN @startDate AND @endDate
		AND NOT EXISTS ( SELECT 1 FROM #resList r2 WHERE r2.ReservationId = r.ReservationId );

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #resList Reservation', @@ROWCOUNT;

	INSERT INTO #resList
	SELECT DISTINCT
		tto.ReservationId
	FROM
		DW_Traveler.dbo.TravelerOrder tto WITH ( NOLOCK )
	INNER JOIN DW_Traveler.dbo.TravelerOrderItem toi WITH ( NOLOCK ) ON tto.TravelerOrderId = toi.TravelerOrderId
	INNER JOIN DW_Traveler.dbo.TravelerOrderPayment topp WITH ( NOLOCK ) ON tto.TravelerOrderId = topp.TravelerOrderId
	INNER JOIN DW_Traveler.dbo.TravelerOrderPaymentDistribution topd WITH ( NOLOCK ) ON topp.TravelerOrderPaymentId = topd.TravelerOrderPaymentId AND topd.TravelerOrderItemId = topd.TravelerOrderItemId
	WHERE
		topp.TravelerOrderPaymentTypeName = 'PAYMENT'
		AND topp.TravelerOrderPaymentStatusName IN ( 'PAID', 'SETTLED', 'REMITTED' )
		AND topd.UpdateDate BETWEEN @startDate AND @endDate
		AND EXISTS ( SELECT 1 FROM dbo.Reservation r WITH(NOLOCK) WHERE r.ReservationId = tto.ReservationId AND r.ActiveFlag = 1 AND r.BookingCategoryId != 1 AND r.CreateDate >= @RollingTwoYears )
		AND NOT EXISTS ( SELECT 1 FROM #resList r2 WHERE r2.ReservationId = tto.ReservationId );
		
	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #resList TOPD', @@ROWCOUNT;

	SET @rowCount = @@ROWCOUNT;
	SET @msg = 'Insert #resList Reservation CreateDate btwn ' + CAST(@startDate AS VARCHAR(25)) + ' and ' + CAST(@endDate AS VARCHAR(25));
	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, @msg , @rowCount;

	IF OBJECT_ID('tempdb..#res') IS NOT NULL DROP TABLE #res;

	SELECT
		r.ArrivalDate
		,r.CreateDate
	/* from Reservation */
		,r.BookingCategoryId
		,BookingDateId = r.BookingDate	/* can this be NULL for bookings? why? */
		,BookingReportedDateId = r.BookingReportedDate	/* can this be NULL for bookings? why? */
		,CancelledDateId = r.CancelledDate
		,r.BrandId
		,r.ListingUnitId
		,r.ReservationId
		,r.SiteId
		,r.TravelerEmailId
		,VisitorId = ISNULL(r.VisitorId, 0)
		,r.OnlineBookingProviderTypeId
		,ReservationAvailabilityStatusTypeId = r.ReservationAvailabilityStatusTypeId
		,ReservationPaymentStatusTypeId = r.ReservationPaymentStatusTypeId
	/* derived from Reservation */
		,DeviceCategorySessionId = r.DeviceCategorySessionId
		,r.InquiryServiceEntryGUID
		,InquiryId = 0
	/* from ListingUnit */
		,lu.CustomerId
		,lu.ListingId
		,b.ReportingCurCode AS BrandCurrencyCode
		,lu.RegionId
		,BookedNightsCount = CASE WHEN r.ArrivalDate = r.DepartureDate THEN 1 ELSE DATEDIFF(D, r.ArrivalDate, r.DepartureDate) END
		,BrandAttributesId = 0
		,CustomerAttributesId = 0
		,ListingAttributesId = 0
		,ListingUnitAttributesId = -1
		,Subscriptionid = -1
		,PaymentTypeId = -1
		,ListingChannelId = -1
		,PersonTypeId = -1
		,BookingCount = 1
		,BookingChannelId = -1
		,StrategicDestinationId = 0
		,StrategicDestinationAttributesId = 0
		,r.VisitId
		,r.FullVisitorId
		,r.WebsiteReferralMediumSessionId
	INTO
		#res
	FROM
		dbo.Reservation r ( NOLOCK )
	INNER JOIN dbo.ListingUnit lu ( NOLOCK ) ON r.ListingUnitId = lu.ListingUnitId
	INNER JOIN dbo.Brand b ( NOLOCK ) ON r.BrandId = b.BrandId
	INNER JOIN #resList rl ON r.ReservationId = rl.ReservationId;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert BookingFact #res', @@ROWCOUNT;

	/* 
	pick up tertiary dims 
	note: tried to do this in a single select, but it resulted in clusteredindexscans on listingattributes and listingunitattributes.
	instead, will try to do in updates one at a time
	and try make the optimizer use the proper indexes 
	It doesn't, I don't think. But this does perform better than the multi-join select, mainly b/c of the inner joins instead of left joins 
	*/
	UPDATE
		r
	SET
		r.InquiryId = i.InquiryId
	FROM
		#res r
	INNER JOIN dbo.Inquiry i ( NOLOCK ) ON r.InquiryServiceEntryGUID = i.InquiryServiceEntryGUID AND r.ListingId = i.ListingId; /* for index help */

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.InquiryId', @@ROWCOUNT;

	UPDATE
		r
	SET
		r.ListingChannelId = ch.ChannelId
	FROM
		#res r
	INNER JOIN dbo.OnlineBookingProviderType obpt ( NOLOCK ) ON r.OnlineBookingProviderTypeId = obpt.OnlineBookingProviderTypeId
	LEFT JOIN dbo.Channel ch ( NOLOCK ) ON CASE WHEN obpt.OlbProviderGatewayTypeDescription LIKE 'Clear Stay%' THEN 'clearstay' ELSE obpt.ListingSourceName END = ch.ChannelName
		AND obpt.ListingSourceName = ch.ListingSource;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.ListingChannelId', @@ROWCOUNT;

	UPDATE
		r
	SET
		r.ListingAttributesId = la.ListingAttributesId
	FROM
		#res r
	INNER JOIN dbo.ListingAttributes la ( NOLOCK ) ON r.ListingId = la.ListingId AND r.CreateDate BETWEEN la.RowStartDate AND la.RowEndDate;
	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.ListingAttributesId', @@ROWCOUNT;

	UPDATE
		r
	SET
		r.Subscriptionid = s.SubscriptionId
		,r.PaymentTypeId = s.PaymentTypeId
	FROM
		#res r
	INNER JOIN dbo.Subscription s (NOLOCK) ON r.ListingId = s.ListingId 
	WHERE r.BookingDateId BETWEEN s.SubscriptionStartDate AND s.SubscriptionEndDate;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.Subscriptionid.PaymentTypeId', @@ROWCOUNT;

	UPDATE
		r
	SET
		r.StrategicDestinationId = l.StrategicDestinationId
	FROM #res r
	INNER JOIN dbo.Listing l ( NOLOCK ) ON r.ListingId = l.ListingId;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.StrategicDestinationId', @@ROWCOUNT;

	UPDATE
		r
	SET
		r.StrategicDestinationAttributesId = da.DestinationAttributesId
	FROM #res r
	INNER JOIN dbo.DestinationAttributes da (NOLOCK) ON r.StrategicDestinationId = da.DestinationId AND r.CreateDate BETWEEN da.RowStartDate AND da.RowEndDate;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.StrategicDestinationAttributesId', @@ROWCOUNT;

	UPDATE
		r
	SET
		r.BrandAttributesId = ba.BrandAttributesId
	FROM
		#res r
	INNER JOIN dbo.BrandAttributes ba ( NOLOCK ) ON r.BrandId = ba.BrandId AND r.CreateDate BETWEEN ba.RowStartDate AND ba.RowEndDate;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.BrandAttributesId', @@ROWCOUNT;

	UPDATE
		r
	SET
		r.CustomerAttributesId = ca.CustomerAttributesId
		,r.PersonTypeId = ISNULL(pt.PersonTypeId, -1)
	FROM
		#res r
	INNER JOIN dbo.CustomerAttributes ca ( NOLOCK ) ON r.CustomerId = ca.CustomerId AND r.CreateDate BETWEEN ca.RowStartDate AND ca.RowEndDate
	LEFT JOIN dbo.PersonType pt ( NOLOCK ) ON ISNULL(ca.PersonType, 'Unknown') = pt.PersonTypeName;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.CustomerAttributesId.PersonTypeId', @@ROWCOUNT;

	/* this update performs like crap b/c of the size of ListingUnitAttributes */
	UPDATE
		r
	SET
		r.ListingUnitAttributesId = lua.ListingUnitAttributesId
	FROM
		#res r
	INNER JOIN dbo.ListingUnitAttributes lua WITH ( NOLOCK ) ON r.ListingUnitId = lua.ListingUnitId AND r.CreateDate BETWEEN lua.RowStartDate AND lua.RowEndDate;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.ListingUnitAttributesId', @@ROWCOUNT;

	/* look up bookingchannelid */
	UPDATE
		r
	SET
		r.BookingChannelId = s.BookingChannelId
	FROM
		#res r
	INNER JOIN dbo.Site s WITH ( NOLOCK ) ON s.SiteId = r.SiteId
	WHERE
		s.BookingChannelId > -1;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #res.BookingChannelId', @@ROWCOUNT;

	/* gbv from Quote */
	IF OBJECT_ID('tempdb..#Qgbv') IS NOT NULL DROP TABLE #Qgbv;

	SELECT
		r.ReservationId
		,gbv.CurrencyCode
		,ValueSource = CAST(CASE WHEN gbv.QuoteStatusId = -1 THEN 'Quote Unk'
								ELSE 'Quote'
							END AS VARCHAR(25))
		,gbv.RentalAmount
		,gbv.RefundableDamageDepositAmount
		,gbv.ServiceFeeAmount
		,gbv.TaxAmount
		,gbv.VasAmount
		,gbv.OrderAmount
		,gbv.PaidAmount
		,gbv.RefundAmount
		,gbv.BookingValue
		,gbv.CommissionAmount
		,gbv.PaymentMethodTypeId
		,rowNum = ROW_NUMBER() OVER ( PARTITION BY gbv.ReservationId ORDER BY CAST(CASE WHEN gbv.QuoteStatusId = -1 THEN 'Quote Unk' ELSE 'Quote' END AS VARCHAR(25)), gbv.QuoteId )
	INTO
		#Qgbv
	FROM
		dbo.VW_QuoteGrossBookingValue gbv
	INNER JOIN #res r ON gbv.ReservationId = r.ReservationId;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #Qgbv from Quote', @@ROWCOUNT;

	IF OBJECT_ID('tempdb..#gbv') IS NOT NULL DROP TABLE #gbv;

	;WITH	GBV
	AS ( SELECT
		ReservationId
		,CurrencyCode
		,ValueSource
		,RentalAmount = SUM(RentalAmount)
		,RefundableDamageDepositAmount = SUM(RefundableDamageDepositAmount)
		,ServiceFeeAmount = SUM(ServiceFeeAmount)
		,TaxAmount = SUM(TaxAmount)
		,VasAmount = SUM(VasAmount)
		,OrderAmount = SUM(OrderAmount)
		,PaidAmount = SUM(PaidAmount)
		,RefundAmount = SUM(RefundAmount)
		,BookingValue = SUM(BookingValue)
		,CommissionAmount = SUM(CommissionAmount)
		FROM
		#Qgbv
		GROUP BY
		ReservationId
		,CurrencyCode
		,ValueSource
		)
	SELECT
		GBV.ReservationId
		,GBV.CurrencyCode
		,GBV.RentalAmount
		,GBV.RefundableDamageDepositAmount
		,GBV.ServiceFeeAmount
		,GBV.TaxAmount
		,GBV.VasAmount
		,GBV.OrderAmount
		,GBV.PaidAmount
		,GBV.RefundAmount
		,GBV.BookingValue
		,GBV.ValueSource
		,DisplayRegionId = -1
		,GBV.CommissionAmount
		,PaymentMethodTypeId = 0
		,PaidAmountOffline = 0
		,PaidAmountOnline = 0
	INTO
		#gbv
	FROM
		GBV;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #gbv', @@ROWCOUNT;

	/* there may be one quote with status of Unknown and one quote with good status in that case, we only want the good one */
	;WITH dup
	AS ( SELECT
		ReservationId
		,MIN(ValueSource) AS MinValueSource
		,MAX(ValueSource) AS MaxValueSource
		FROM
		#gbv
		GROUP BY
		ReservationId
		HAVING
		COUNT(1) > 1
		)
	DELETE FROM
		g
	FROM
		dup d
	INNER JOIN #gbv g ON d.ReservationId = g.ReservationId AND d.MaxValueSource = g.ValueSource AND d.MaxValueSource != d.MinValueSource;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#gbv dup', @@ROWCOUNT;

	;WITH dup AS (
		SELECT
			g.ReservationId
			,MIN(g.ValueSource) AS MinValueSource
			,MAX(g.ValueSource) AS MaxValueSource
		FROM
			#gbv g
		GROUP BY
			g.ReservationId
		HAVING
			COUNT(1) > 1 )
	DELETE FROM
		g
	FROM
		dup d
	INNER JOIN #gbv g ON d.ReservationId = g.ReservationId AND d.MaxValueSource = g.ValueSource AND d.MaxValueSource=d.MinValueSource
	INNER JOIN #Qgbv q ON g.ReservationId=q.ReservationId AND g.CurrencyCode=q.CurrencyCode AND q.rowNum = 1;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#gbv dup part2', @@ROWCOUNT;

	/* lookup the PaymentMethodTypeId */
	UPDATE
		gbv
	SET
		gbv.PaymentMethodTypeId = x.PaymentMethodTypeId
	FROM
		#Qgbv x
	INNER JOIN #gbv gbv ON x.ReservationId = gbv.ReservationId AND x.ValueSource = gbv.ValueSource
	WHERE
		x.rowNum = 1
		AND x.PaymentMethodTypeId > 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #gbv.PaymentMethodTypeId', @@ROWCOUNT;

	/* lookup the PaidAmountOffline and PaidAmountOnline */
	IF OBJECT_ID('tempdb..#gbv2') IS NOT NULL DROP TABLE #gbv2;

	WITH	X
	AS ( SELECT
		tot.ReservationId
		,g.CurrencyCode
		,Amount = CASE WHEN g.CurrencyCode != topp.CurrencyCode
							AND cc.ConversionRate IS NOT NULL THEN topp.Amount * cc.ConversionRate
						ELSE topp.Amount
					END
		,pmt.PaymentCollectionType
		FROM
		DW_Traveler..TravelerOrderPayment topp ( NOLOCK )
		INNER JOIN DW_Traveler..TravelerOrder tot ( NOLOCK ) ON topp.TravelerOrderId = tot.TravelerOrderId
		INNER JOIN DW_Traveler..PaymentMethodType pmt ( NOLOCK ) ON pmt.PaymentMethodTypeId = topp.PaymentMethodTypeId
		INNER JOIN #gbv g ( NOLOCK ) ON tot.ReservationId = g.ReservationId
		LEFT JOIN dbo.CurrencyConversion cc ( NOLOCK ) ON g.CurrencyCode = cc.ToCurrency /* payments can be made in multiple currencies */
															AND topp.CurrencyCode = cc.FromCurrency
															AND topp.PaymentDate BETWEEN cc.RowStartDate AND cc.RowEndDate
		WHERE
		topp.Amount <> 0
		AND tot.QuoteId > 0
		AND topp.TravelerOrderPaymentTypeName IN ( 'PAYMENT' )	/* THIS IS GROSS OF REFUNDS */
		AND topp.TravelerOrderPaymentStatusName IN ( 'PAID', 'SETTLED', 'REMITTED' )
		)
	SELECT
		p.ReservationId
		,p.CurrencyCode
		,PaidAmountOnline = COALESCE(p.ONLINE, 0)
		,PaidAmountOffline = COALESCE(p.OFFLINE, 0) + COALESCE(p.UNKNOWN, 0)
	INTO
		#GBV2
	FROM
		X PIVOT
		( SUM(Amount) FOR PaymentCollectionType IN ( [ONLINE], [OFFLINE], [UNKNOWN] ) )
	AS p;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #GBV2', @@ROWCOUNT;

	/* get the DisplayRegionId from Quote */
	;WITH r
	AS ( SELECT
		r.ReservationId
		,MAX(q.DisplayRegionId) AS DisplayRegionId
		FROM
		dbo.QuoteFact q ( NOLOCK )
		INNER JOIN #res r ON q.ReservationId = r.ReservationId
		GROUP BY
		r.ReservationId
		)
	UPDATE
		g
	SET
		g.DisplayRegionId = r.DisplayRegionId
	FROM
		#gbv g
	INNER JOIN r ON g.ReservationId = r.ReservationId;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #gbv.DisplayRegionId', @@ROWCOUNT;

	/* pick up TOPD service fees for EXTERNAL_SOR bookings and service fee VAT for all bookings */
	IF OBJECT_ID('tempdb..#sf') IS NOT NULL DROP TABLE #sf;

	SELECT
		tto.ReservationId
		,toi.CurrencyCode
		,ServiceFeeAmount = SUM(topd.DistributedAmount)
		,ServiceFeeVATAmount = SUM(CASE WHEN topd.TravelerOrderPaymentDistributionToAccountTypeId = 2 THEN topd.DistributedAmount
										ELSE 0
									END)
		,ServiceFeeProcessingFeeAmount = SUM(CASE WHEN topd.TravelerOrderPaymentDistributionTypeId IN ( 10, 5, 4, 2, 1 ) THEN topd.DistributedAmount
													ELSE 0
											END)
	INTO
		#sf
	FROM
		DW_Traveler.dbo.TravelerOrder tto WITH ( NOLOCK )
	INNER JOIN DW_Traveler.dbo.TravelerOrderItem toi WITH ( NOLOCK ) ON tto.TravelerOrderId = toi.TravelerOrderId
	INNER JOIN DW_Traveler.dbo.Product p WITH ( NOLOCK ) ON toi.ProductId = p.ProductId
	INNER JOIN DW_Traveler.dbo.TravelerOrderPayment topp WITH ( NOLOCK ) ON tto.TravelerOrderId = topp.TravelerOrderId
	/* not going to use TravelerCheckoutFact due to poor performance. Need TOPD and not TOI b/c the TOPD breaks out the VAT */
	INNER JOIN DW_Traveler.dbo.TravelerOrderPaymentDistribution topd WITH ( NOLOCK ) ON topp.TravelerOrderPaymentId = topd.TravelerOrderPaymentId AND toi.TravelerOrderItemId = topd.TravelerOrderItemId
	INNER JOIN #gbv gbv ON tto.ReservationId = gbv.ReservationId
	/* these will all have quotes. in the case of external_sor, the quote just won't have the service fee line */
	WHERE
		p.GrossBookingValueProductCategory = 'TRAVELER_FEE'
		AND topp.TravelerOrderPaymentTypeName = 'PAYMENT'
		AND topp.TravelerOrderPaymentStatusName IN ( 'PAID', 'SETTLED', 'REMITTED' )
	GROUP BY
		tto.ReservationId
		,toi.CurrencyCode;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#sf insert', @@ROWCOUNT;

	/* incorporate into #gbv */
	UPDATE
		gbv
	SET
		gbv.ValueSource = 'Quote and TravelerOrder'
		,gbv.ServiceFeeAmount = CASE WHEN gbv.ServiceFeeAmount = 0 THEN sf.ServiceFeeAmount - sf.ServiceFeeVATAmount - sf.ServiceFeeProcessingFeeAmount ELSE gbv.ServiceFeeAmount END
		,gbv.TaxAmount = gbv.TaxAmount + sf.ServiceFeeVATAmount
		,gbv.OrderAmount = gbv.OrderAmount
			- gbv.ServiceFeeAmount
			- gbv.TaxAmount
			+ CASE WHEN gbv.ServiceFeeAmount = 0 THEN sf.ServiceFeeAmount - sf.ServiceFeeVATAmount - sf.ServiceFeeProcessingFeeAmount ELSE gbv.ServiceFeeAmount END
			+ gbv.TaxAmount + sf.ServiceFeeVATAmount
		,gbv.BookingValue = gbv.BookingValue
			- gbv.ServiceFeeAmount
			- gbv.TaxAmount
			+ CASE WHEN gbv.ServiceFeeAmount = 0 THEN sf.ServiceFeeAmount - sf.ServiceFeeVATAmount - sf.ServiceFeeProcessingFeeAmount ELSE gbv.ServiceFeeAmount END
			+ gbv.TaxAmount
			+ sf.ServiceFeeVATAmount
	FROM
		#sf sf
	INNER JOIN #gbv gbv ON sf.ReservationId = gbv.ReservationId AND sf.CurrencyCode = gbv.CurrencyCode	/* should all be in the same currency */
	WHERE
		gbv.ServiceFeeAmount != CASE WHEN gbv.ServiceFeeAmount = 0 THEN sf.ServiceFeeAmount - sf.ServiceFeeVATAmount - sf.ServiceFeeProcessingFeeAmount ELSE gbv.ServiceFeeAmount END
		OR sf.ServiceFeeVATAmount != 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#gbv SF update', @@ROWCOUNT;

	/*****************************************************************************************************/
	/* now Estimated GrossBookingValue */
	IF OBJECT_ID('tempdb..#est') IS NOT NULL DROP TABLE #est;

	/* 1. Determine the Reservations that need to be estimated */
	SELECT
		r.ReservationId
		,r.ListingUnitId
		,r.RegionId
		,r.ArrivalDate
		,cal.QuarterEndDate AS ArrivalQuarterEndDate
		,cal.YearNumber AS ArrivalYearNumber
		,r.BrandCurrencyCode
		,r.BrandId
		,r.BookedNightsCount
		,l.BedroomNum
		,l.PostalCode
		,l.City
		,l.Country
		,BookingQuarterEndDate = calB.QuarterEndDate
		,BookingYearNumber = calB.YearNumber
		,CurrencyCode = 'USD'
		,BookingValue = CAST(0 AS DECIMAL(18,6))
		,Step = 0
	INTO
		#est
	FROM
		#res r
	INNER JOIN dbo.BookingCategory bc ( NOLOCK ) ON r.BookingCategoryId = bc.BookingCategoryId
	INNER JOIN dbo.Listing l ( NOLOCK ) ON r.ListingId = l.ListingId
	INNER JOIN dbo.Calendar cal ( NOLOCK ) ON r.ArrivalDate = cal.DateId
	INNER JOIN dbo.Calendar calB ( NOLOCK ) ON r.BookingDateId = calB.DateId
	WHERE
		bc.BookingIndicator = 1
		AND bc.KnownBookingIndicator = 0
		AND NOT EXISTS ( SELECT g.ReservationId FROM #gbv g WHERE r.ReservationId = g.ReservationId );

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #est', @@ROWCOUNT;
	
	/* 1. same listingunitid with arrival date in the same quarter */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 1
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 1 AND f.ListingUnitId = x.ListingUnitId AND f.ArrivalQuarterEndDate = x.ArrivalQuarterEndDate;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 1', @@ROWCOUNT;

	/* 2. same listingunitid w arrival date in same year */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 2
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 2 AND f.ListingUnitId = x.ListingUnitId AND f.ArrivalYearNumber = x.ArrivalYearNumber
	WHERE
		f.Step=0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 2', @@ROWCOUNT;

	/* 3. same listingunitid */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 3
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 3 AND f.ListingUnitId = x.ListingUnitId
	WHERE
		f.Step=0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 3', @@ROWCOUNT;

	/* 4. BrandID, BedroomNum, postalcode,BookingDateQuarter */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 4
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 4 AND f.BrandId = x.BrandId AND f.BedroomNum = x.BedroomNum
		AND f.PostalCode = x.PostalCode AND f.BookingQuarterEndDate = x.BookingQuarterEndDate
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 4', @@ROWCOUNT;

	/* 5. BrandID, BedroomNum, postalcode, ArrivalDateYear*/
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 5
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 5 AND f.BrandId = x.BrandId AND f.BedroomNum = x.BedroomNum
		AND f.PostalCode = x.PostalCode AND f.ArrivalYearNumber = x.ArrivalYearNumber
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 5', @@ROWCOUNT;

	/* 6. BrandID, BedroomNum, postalcode */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 6
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 6 AND f.BrandId = x.BrandId AND f.BedroomNum = x.BedroomNum AND f.PostalCode = x.PostalCode
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 6', @@ROWCOUNT;

	/* 7. BrandID, BedroomNum, City, Country,BookingDateQuarter */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 7
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 7 AND f.BrandId = x.BrandId AND f.BedroomNum = x.BedroomNum
		AND f.City = x.City AND f.Country = x.Country AND f.BookingQuarterEndDate = x.BookingQuarterEndDate
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 7', @@ROWCOUNT;

	/* 8. BrandID, BedroomNum, City, Country, ArrivalDateYear	*/
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 8
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 8 AND f.BrandId = x.BrandId AND f.BedroomNum = x.BedroomNum
		AND f.City = x.City AND f.Country = x.Country AND f.ArrivalYearNumber = x.ArrivalYearNumber
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 8', @@ROWCOUNT;

	/* 9. BrandID, BedroomNum, City, Country	 */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = 9
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 9 AND f.BrandId = x.BrandId AND f.BedroomNum = x.BedroomNum AND f.City = x.City AND f.Country = x.Country
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 9', @@ROWCOUNT;

	/* 10. BrandID, BedroomNum */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = EstimationStepNum
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 10 AND f.BrandId = x.BrandId AND f.BedroomNum = x.BedroomNum
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 10', @@ROWCOUNT;

	/* 11. BedroomNum	 */
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = EstimationStepNum
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 11 AND f.BedroomNum = x.BedroomNum
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 11', @@ROWCOUNT;

	/* 12. BrandId	*/
	UPDATE
		f
	SET
		f.BookingValue = ROUND(x.AverageBookingValuePerNightUSD * f.BookedNightsCount,6)
		,f.Step = EstimationStepNum
	FROM
		#est f
	INNER JOIN dbo.AverageKnownNightlyBookingValue x ON x.EstimationStepNum = 12 AND f.BrandId = x.BrandId
	WHERE
		f.Step = 0;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#est 12', @@ROWCOUNT;

	INSERT INTO #gbv
		( ReservationId
		,CurrencyCode
		,RentalAmount
		,RefundableDamageDepositAmount
		,ServiceFeeAmount
		,TaxAmount
		,VasAmount
		,OrderAmount
		,PaidAmount
		,RefundAmount
		,BookingValue
		,ValueSource
		,DisplayRegionId
		,CommissionAmount
		,PaymentMethodTypeId
		,PaidAmountOffline
		,PaidAmountOnline 
		)
	SELECT
		ReservationId
		,CurrencyCode
		,RentalAmount=BookingValue
		,RefundableDamageDepositAmount = 0
		,ServiceFeeAmount = 0
		,TaxAmount = 0
		,VasAmount = 0
		,OrderAmount = BookingValue
		,PaidAmount = 0
		,RefundAmount = 0
		,BookingValue 
		,ValueSource = 'Estimate Step ' + CAST(Step AS VARCHAR(5))
		,-1
		,CommissionAmount = 0
		,PaymentMethodTypeId = 0
		,PaidAmountOffline = 0
		,PaidAmountOnline = 0
	FROM
		#est;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #gbv From #est', @@ROWCOUNT;

	/*****************************************************************************************************/
	IF OBJECT_ID('tempdb..#final') IS NOT NULL DROP TABLE #final;

	/* now left join and get the currency conversions */
	SELECT
		r.*
		,BookingValueSourceId = ISNULL(s.BookingValueSourceId, 1)
		,DisplayRegionId = ISNULL(g.DisplayRegionId, -1)
		,RentalAmount = ISNULL(g.RentalAmount, 0)
		,RefundableDamageDepositAmount = ISNULL(g.RefundableDamageDepositAmount, 0)
		,ServiceFeeAmount = ISNULL(g.ServiceFeeAmount, 0)
		,TaxAmount = ISNULL(g.TaxAmount, 0)
		,VasAmount = ISNULL(g.VasAmount, 0)
		,OrderAmount = ISNULL(g.OrderAmount, 0)
		,PaidAmount = ISNULL(g.PaidAmount, 0)
		,RefundAmount = ISNULL(g.RefundAmount, 0)
		,BookingValue = ISNULL(g.BookingValue, 0)
		,LocalCurrencyId = ISNULL(c.CurrencyId, 154)
		,CurrencyConversionIdUSD = ISNULL(ccUSD.CurrencyConversionId, -1)
		,CurrencyConversionIdBrandCurrency = ISNULL(ccBC.CurrencyConversionId, -1)
		,CommissionAmount = ISNULL(g.CommissionAmount, 0)
		,PaymentMethodTypeId = ISNULL(g.PaymentMethodTypeId, 0)
		/* 
		setting up PaidOnlineAmount and PaidOfflineAmount to come ONLY from TravelerOrderPayment at this time
		and not to get mixed up with the QuoteFact.PaidAmount 
		BECAUSE 
		- there are known issues with QuoteFact.PaidAmount
		- there is a known disconnect between TravelerOrderPayment and QuotePaymentSchedule/QuotePayment that is on purpose
			but leaves out some online payments

		THIS MEANS -- PaidOnlineAmount + PaidOfflineAmount != PaidAmount necessarily. And PaidOfflineAmount 
		will be 0 even if PaidAmount !=0 if Traveler Checkout was not utilized at all
		*/
		,PaidAmountOnline = ISNULL(g2.PaidAmountOnline, 0)
		,PaidAmountOffline = ISNULL(g2.PaidAmountOffline, 0)
		,InquiryVisitId = 0
		,InquiryFullVisitorId = CONVERT(VARCHAR(50), '[Unknown]')
		,InquiryMedium = CONVERT(NVARCHAR(255), 'undefined')
		,InquiryWebsiteReferralMediumSessionId = CONVERT(INT, -1)
		,InquirySource = CONVERT(NVARCHAR(255), '[N/A]')
		,BookingRequestVisitId = 0
		,BookingRequestFullVisitorId = CONVERT(VARCHAR(50), '[Unknown]')
		,BookingRequestMedium = CONVERT(NVARCHAR(255), 'Undefined')
		,BookingRequestWebsiteReferralMediumSessionId = CONVERT(INT, -1)
		,BookingRequestSource = CONVERT(NVARCHAR(255), '[N/A]')
		,CalculatedVisitId = 0
		,CalculatedFullVisitorId = CONVERT(VARCHAR(50), '[Unknown]')
		,CalculatedBookingMedium = CONVERT(NVARCHAR(255), 'Undefined')
		,CalculatedBookingWebsiteReferralMediumSessionId = CONVERT(INT, -1)
		,CalculatedBookingSource = CONVERT(NVARCHAR(255), '[N/A]')
	INTO
		#final
	FROM
		#res r
		LEFT JOIN #gbv g ON r.ReservationId = g.ReservationId
		LEFT JOIN #GBV2 g2 ON r.ReservationId = g2.ReservationId
		LEFT JOIN dbo.BookingValueSource s ( NOLOCK ) ON g.ValueSource = s.BookingValueSourceName
		LEFT JOIN dbo.Currency c ( NOLOCK ) ON g.CurrencyCode = c.CurrencyCode
		LEFT JOIN dbo.CurrencyConversion ccUSD ON g.CurrencyCode = ccUSD.FromCurrency
													AND ccUSD.ToCurrency = 'USD'
													AND r.CreateDate BETWEEN ccUSD.RowStartDate AND ccUSD.RowEndDate
		LEFT JOIN dbo.CurrencyConversion ccBC ON r.BrandCurrencyCode = ccBC.ToCurrency
													AND g.CurrencyCode = ccBC.FromCurrency
													AND r.CreateDate BETWEEN ccBC.RowStartDate AND ccBC.RowEndDate;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #final', @@ROWCOUNT;

	IF OBJECT_ID('tempdb..#vas') IS NOT NULL DROP TABLE #vas;

	;WITH QF AS (
		SELECT
			q.QuoteId
			,q.QuoteItemId
			,q.QuoteItemCreatedDate
			,tp.QuoteItemType
			,q.AmountLocalCurrency
			,q.ReservationId
		FROM
			dbo.QuoteFact q ( NOLOCK )
		INNER JOIN dbo.TravelerProduct tp ( NOLOCK ) ON q.TravelerProductId = tp.TravelerProductId
		INNER JOIN #res r ON q.ReservationId = r.ReservationId
		WHERE
			q.QuoteCreatedDate >= '2013-01-01'
			AND q.QuoteActiveFlag = 1
			AND q.QuoteItemActiveFlag = 1 )
	SELECT
		tto.ReservationId
		,toi.TravelerOrderId
		,r.BrandCurrencyCode
		,ISNULL(cc.ConversionRate, 1) AS ConversionRate
		,ISNULL(q.CurrencyCode, toi.CurrencyCode) AS LocalCurrencyCode 
		,SUM(toi.Amount * ISNULL(cc.ConversionRate, 1)) AS StandaloneVASAmt	/* standalone VAS may be in a different currency; */
	INTO
		#vas
	FROM
		DW_Traveler.dbo.TravelerOrder tto ( NOLOCK )
	INNER JOIN DW_Traveler.dbo.TravelerOrderItem toi ( NOLOCK ) ON toi.TravelerOrderId = tto.TravelerOrderId
	INNER JOIN DW_Traveler.dbo.Product p ( NOLOCK ) ON toi.ProductId = p.ProductId
	INNER JOIN dbo.Quote q ( NOLOCK ) ON tto.QuoteId = q.QuoteId 
	/* note: there is a ProductFulfillmentId on TravelerOrderItem 
		BUT -- it's not populated yet. meanwhile, have to join the old fashioned way.
		Adding an index to support this join. */
	INNER JOIN DW_Traveler.dbo.ProductFulfillment pf ( NOLOCK ) ON pf.ExternalRefId = q.QuoteGuid AND pf.ExternalRefType = 'quote' AND pf.ProductTypeGuid = p.ProductGuid
	INNER JOIN #res r ( NOLOCK ) ON q.ReservationId = r.ReservationId
	LEFT JOIN dbo.CurrencyConversion cc ( NOLOCK ) ON toi.CurrencyCode = cc.FromCurrency AND q.CurrencyCode = cc.ToCurrency AND r.CreateDate BETWEEN cc.RowStartDate AND cc.RowEndDate
	WHERE
		1 = 1
		AND p.ProductGuid NOT IN ( 'TRAVELER_PROTECTION', 'COMMISSION' )
		AND p.GrossBookingValueProductCategory = 'VAS'
	/* cd also be NEW or CANCELLED or REFUNDED. we only want PAID */
		AND toi.TravelerOrderItemStatus = 'PAID'
		AND NOT EXISTS ( SELECT 1 FROM QF WHERE tto.ReservationId = QF.ReservationId AND QF.QuoteItemType = p.ProductGuid )
	GROUP BY
		tto.ReservationId
		,toi.TravelerOrderId
		,ISNULL(cc.ConversionRate, 1)
		,ISNULL(q.CurrencyCode, toi.CurrencyCode)
		,r.BrandCurrencyCode;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#vas', @@ROWCOUNT;

	IF OBJECT_ID('tempdb..#vas2') IS NOT NULL DROP TABLE #vas2;

	/* now get the payment info on those orders 
	and from here you can aggregate on Reservation 
	*/ 
	SELECT
		v.ReservationId
		,v.LocalCurrencyCode
		,v.BrandCurrencyCode
		,ROUND(SUM(v.StandaloneVASAmt), 6) AS StandaloneVASAmt /* could be multiple orders */
		,PaidAmount = ISNULL(ROUND(SUM(topp.PaidAmount * v.ConversionRate), 6), 0)
		,PaymentMethodTypeId = ISNULL(MIN(topp.PaymentMethodTypeId), 0)
		,PaidAmountOnline = ISNULL(ROUND(SUM(topp.PaidAmountOnline * v.ConversionRate), 6), 0)
		,PaidAmountOffline = ISNULL(ROUND(SUM(topp.PaidAmountOffline * v.ConversionRate), 6), 0)
	INTO
		#vas2
	FROM
		#vas v
	LEFT JOIN ( SELECT
					tp.TravelerOrderId
					,MIN(tp.PaymentMethodTypeId) AS PaymentMethodTypeId
					,PaidAmount = SUM(topd.DistributedAmount)	/* this may be net of processing fees */
					,PaidAmountOnline = SUM(CASE WHEN pmt.PaymentCollectionType='ONLINE' THEN topd.DistributedAmount ELSE 0 END)
					,PaidAmountOffline = SUM(CASE WHEN pmt.PaymentCollectionType='OFFLINE' THEN topd.DistributedAmount ELSE 0 END)
				FROM
					DW_Traveler..TravelerOrderPayment tp ( NOLOCK )
				JOIN DW_Traveler..TravelerOrderPaymentDistribution topd ( NOLOCK ) ON topd.TravelerOrderPaymentId = tp.TravelerOrderPaymentId
				JOIN DW_Traveler..TravelerOrderPaymentDistributionAmountType at ( NOLOCK ) ON topd.TravelerOrderPaymentDistributionAmountTypeId = at.TravelerOrderPaymentDistributionAmountTypeId
				JOIN DW_Traveler..TravelerOrderPaymentDistributionType topdt ON topd.TravelerOrderPaymentDistributionTypeId = topdt.TravelerOrderPaymentDistributionTypeId
				JOIN DW_Traveler..TravelerOrderPaymentDistributionToAccountType act ( NOLOCK ) ON topd.TravelerOrderPaymentDistributionToAccountTypeId = act.TravelerOrderPaymentDistributionToAccountTypeId
				JOIN DW_Traveler..PaymentMethodType pmt ( NOLOCK ) ON pmt.PaymentMethodTypeId = tp.PaymentMethodTypeId
				WHERE
					at.TravelerOrderPaymentDistributionAmountTypeName = 'credit'
					AND act.TravelerOrderPaymentDistributionToAccountTypeName = 'Vendor'
					AND tp.TravelerOrderPaymentTypeName = 'payment'
					AND tp.TravelerOrderPaymentStatusName IN ( 'SETTLED', 'REMITTED', 'PAID' )
					AND topdt.TravelerOrderPaymentDistributionTypeName = 'SERVICE'
				GROUP BY
					tp.TravelerOrderId
					) topp ON v.TravelerOrderId = topp.TravelerOrderId
	GROUP BY
		v.ReservationId
		,v.LocalCurrencyCode
		,v.BrandCurrencyCode;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#vas2' , @@ROWCOUNT;

	/* 
	either vas is going to be in the same currency as the booking by this point
	or the booking has UNK currency code -- that is, have a None BookingValueSource b/c the VAS is all there is.
	And in that case, gotta overwrite the #final currency 
	and use the TravelerOrder bvs Id
	and also update currencyconversionids and paymentmethodtypeid
	*/
	UPDATE
		bf
	SET
		bf.VasAmount = bf.VasAmount + StandaloneVASAmt
		,bf.OrderAmount = bf.OrderAmount + StandaloneVASAmt
		,bf.BookingValue = bf.BookingValue + StandaloneVASAmt
		,bf.BookingValueSourceId = @QuoteAndTravelerOrderBookingValueSourceId
		,bf.PaidAmount = bf.PaidAmount + v.PaidAmount
	FROM
		#final bf ( NOLOCK )
		JOIN dbo.Currency c ( NOLOCK ) ON bf.LocalCurrencyId = c.CurrencyId
		JOIN #vas2 v ON bf.ReservationId = v.ReservationId
						AND c.CurrencyCode = v.LocalCurrencyCode
		JOIN dbo.BookingValueSource bvs ( NOLOCK ) ON bf.BookingValueSourceId = bvs.BookingValueSourceId
	WHERE bvs.BookingValueSourceType = 'Order';

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#final vas update same currency' , @@ROWCOUNT;

	UPDATE
		bf
	SET
		bf.VasAmount = bf.VasAmount + StandaloneVASAmt
		,bf.OrderAmount = bf.OrderAmount + StandaloneVASAmt
		,bf.BookingValue = bf.BookingValue + StandaloneVASAmt
		,bf.BookingValueSourceId = @TravelerOrderBookingValueSourceId
		,bf.LocalCurrencyId = vc.CurrencyId
		,bf.CurrencyConversionIdUSD = cc.CurrencyConversionId
		,bf.PaymentMethodTypeId = v.PaymentMethodTypeId
		,bf.PaidAmount = bf.PaidAmount + v.PaidAmount
		,bf.CurrencyConversionIdBrandCurrency = bc.CurrencyConversionId
		,bf.PaidAmountOnline = bf.PaidAmountOnline + v.PaidAmountOnline
		,bf.PaidAmountOffline = bf.PaidAmountOffline + v.PaidAmountOffline 
	FROM
		#final bf ( NOLOCK )
	INNER JOIN dbo.Currency c ( NOLOCK ) ON bf.LocalCurrencyId = c.CurrencyId
	INNER JOIN #vas2 v ON bf.ReservationId = v.ReservationId AND c.CurrencyCode != v.LocalCurrencyCode
	INNER JOIN dbo.Currency vc ( NOLOCK ) ON v.LocalCurrencyCode = vc.CurrencyCode
	INNER JOIN dbo.CurrencyConversion cc ( NOLOCK ) ON vc.CurrencyCode = cc.FromCurrency AND cc.ToCurrency = 'USD' AND bf.BookingDateId BETWEEN cc.RowStartDate AND cc.RowEndDate
	INNER JOIN dbo.CurrencyConversion bc ( NOLOCK ) ON v.BrandCurrencyCode = bc.ToCurrency AND v.LocalCurrencyCode = bc.FromCurrency AND bf.BookingDateId BETWEEN bc.RowStartDate AND bc.RowEndDate
	WHERE
		bf.BookingValueSourceId = 1;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, '#final vas update diff currency', @@ROWCOUNT;

	/* 
	pick up ones that aren't bookings any more but ARE in BookingFact and INSERT 0s FOR ALL MEASURES in #final 
	just use existing BookingFact dim keys except BookingCategoryId AND dates; don't rebuild all that for no good reason 
	*/ 
	INSERT INTO #final
		(ArrivalDate
		,CreateDate
		,BookingCategoryId
		,BookingDateId
		,BookingReportedDateId
		,CancelledDateId
		,BrandId
		,ListingUnitId
		,ReservationId
		,SiteId
		,TravelerEmailId
		,VisitorId
		,OnlineBookingProviderTypeId
		,ReservationAvailabilityStatusTypeId
		,ReservationPaymentStatusTypeId
		,DeviceCategorySessionId
		,InquiryServiceEntryGUID
		,InquiryId
		,CustomerId
		,ListingId
		,BrandCurrencyCode
		,RegionId
		,BookedNightsCount
		,BrandAttributesId
		,CustomerAttributesId
		,ListingAttributesId
		,ListingUnitAttributesId
		,Subscriptionid
		,PaymentTypeId
		,ListingChannelId
		,PersonTypeId
		,BookingCount
		,BookingValueSourceId
		,DisplayRegionId
		,RentalAmount
		,RefundableDamageDepositAmount
		,ServiceFeeAmount
		,TaxAmount
		,VasAmount
		,OrderAmount
		,PaidAmount
		,RefundAmount
		,BookingValue
		,LocalCurrencyId
		,CurrencyConversionIdUSD
		,CurrencyConversionIdBrandCurrency
		,CommissionAmount
		,BookingChannelId
		,PaymentMethodTypeId
		,PaidAmountOffline
		,PaidAmountOnline 
		,StrategicDestinationId
		,StrategicDestinationAttributesId
		,VisitId
		,FullVisitorId
		,WebsiteReferralMediumSessionId
		,InquiryVisitId
		,InquiryFullVisitorId
		,InquiryMedium
		,InquiryWebsiteReferralMediumSessionId
		,InquirySource
		,BookingRequestVisitId
		,BookingRequestFullVisitorId
		,BookingRequestMedium
		,BookingRequestWebsiteReferralMediumSessionID
		,BookingRequestSource
		,CalculatedVisitId
		,CalculatedFullVisitorId
		,CalculatedBookingMedium
		,CalculatedBookingWebsiteReferralMediumSessionID
		,CalculatedBookingSource )
	SELECT
		r.ArrivalDate
		,r.CreateDate
		,r.BookingCategoryId
		,BookingDateId = NULL
		,BookingReportedDateId = r.BookingReportedDate
		,CancelledDateId = NULL
		,f.BrandId
		,f.ListingUnitId
		,r.ReservationId
		,f.SiteId
		,f.TravelerEmailId
		,f.VisitorId
		,f.OnlineBookingProviderTypeId
		,f.ReservationAvailabilityStatusTypeId
		,f.ReservationPaymentStatusTypeId
		,f.DeviceCategorySessionId
		,r.InquiryServiceEntryGUID
		,f.InquiryId
		,f.CustomerId
		,f.ListingId
		,BrandCurrencyCode = 'UNK'
		,f.RegionId
		,BookedNightsCount = 0
		,f.BrandAttributesId
		,f.CustomerAttributesId
		,f.ListingAttributesId
		,f.ListingUnitAttributesId
		,f.SubscriptionId
		,f.PaymentTypeId
		,f.ListingChannelId
		,f.PersonTypeId
		,BookingCount = 0
		,f.BookingValueSourceId
		,f.DisplayRegionId
		,RentalAmount = 0
		,RefundableDamageDepositAmount = 0
		,ServiceFeeAmount = 0
		,TaxAmount = 0
		,VasAmount = 0
		,OrderAmount = 0
		,PaidAmount = 0
		,RefundAmount = 0
		,BookingValue = 0
		,f.LocalCurrencyId
		,f.CurrencyConversionIdUSD
		,f.CurrencyConversionIdBrandCurrency
		,CommissionAmount = 0
		,BookingChannelId = f.BookingChannelId
		,PaymentMethodTypeId = f.PaymentMethodTypeId
		,PaidAmountOffline = 0
		,PaidAmountOnline = 0
		,f.StrategicDestinationId
		,f.StrategicDestinationAttributesId
		,f.VisitId
		,f.FullVisitorId
		,f.WebsiteReferralMediumSessionId
		,f.InquiryVisitId
		,f.InquiryFullVisitorId
		,f.InquiryMedium
		,f.InquiryWebsiteReferralMediumSessionId
		,f.InquirySource
		,f.BookingRequestVisitId
		,f.BookingRequestFullVisitorId
		,f.BookingRequestMedium
		,f.BookingRequestWebsiteReferralMediumSessionId
		,f.BookingRequestSource
		,f.CalculatedVisitId
		,f.CalculatedFullVisitorId
		,f.CalculatedBookingMedium
		,f.CalculatedBookingWebsiteReferralMediumSessionId
		,f.CalculatedBookingSource
	FROM
		dbo.Reservation r ( NOLOCK )
		INNER JOIN DW_Facts.dbo.BookingFact f ( NOLOCK ) ON r.ReservationId = f.ReservationId AND r.CreateDate = f.ReservationCreateDateId
	WHERE
		r.CreateDate >= @RollingTwoYears
		AND (r.ActiveFlag = 0 OR (r.ActiveFlag = 1 AND r.BookingCategoryId = 1))
		AND r.DWUpdateDateTime BETWEEN @startDate AND @endDate
		AND f.BookingCount = 1;	/* won't worry about quote changes here - the reservation would have to be changed for it to no longer be a booking */ 

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Insert #final BookingFact', @@ROWCOUNT;

	/* Set the InquiryMedium and InquiryWebsiteReferralMediumSessionId values on #final before the merge */
	UPDATE bf
	SET bf.InquiryVisitId = ISNULL(i.VisitId,0),
		bf.InquiryFullVisitorId = ISNULL(i.FullVisitorId,'[Unknown]'),
		bf.InquiryMedium = wrms.WebsiteReferralMedium,
		bf.InquiryWebsiteReferralMediumSessionId = wrms.WebsiteReferralMediumSessionId
	FROM #final bf
	INNER JOIN dbo.InquirySLAFact i WITH(NOLOCK) ON i.InquiryId = bf.InquiryId
	INNER JOIN dbo.WebsiteReferralMediumSession wrms WITH(NOLOCK) ON wrms.WebsiteReferralMediumSessionId = i.WebsiteReferralMediumSessionId
	WHERE bf.InquiryMedium <> wrms.WebsiteReferralMedium
		OR bf.InquiryWebsiteReferralMediumSessionId <> wrms.WebsiteReferralMediumSessionId;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #final InquiryMedium, InquiryWebsiteReferralMediumSessionID', @@ROWCOUNT;

	/* Set the InquirySource values on #final before the merge */
	UPDATE bf
	SET bf.InquirySource = vf.source
	FROM #final bf
	INNER JOIN dbo.InquirySLAFact i WITH(NOLOCK) ON i.InquiryId = bf.InquiryId
	INNER JOIN DW_Traveler.dbo.VisitorFact vf WITH(NOLOCK) ON vf.dateId = CAST(FORMAT(i.InquiryDate,'yyyyMMdd') AS INT) AND vf.visitId = i.VisitId AND vf.fullVisitorId = i.FullVisitorId
	WHERE bf.InquirySource <> vf.source;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #final InquirySource', @@ROWCOUNT;
	
	/* Get the first Quote on the reservation */
	IF OBJECT_ID('tempdb..#FirstQuote') IS NOT NULL DROP TABLE #FirstQuote;

	SELECT 
		fq.ReservationId,
		fq.QuoteId
	INTO #FirstQuote
	FROM (
		SELECT 
			qf.ReservationId,
			qf.QuoteId,
			RowNum = ROW_NUMBER() OVER (PARTITION BY q.ReservationId ORDER BY qf.QuoteId)
		FROM #final bf
		INNER JOIN dbo.QuoteFact (NOLOCK) qf ON qf.ReservationId = bf.ReservationId
		INNER JOIN dbo.Quote (NOLOCK) q ON qf.QuoteId = q.QuoteId
		INNER JOIN dbo.Reservation (NOLOCK) r ON qf.ReservationId = r.ReservationId
		WHERE qf.ReservationId > -1
			AND q.BookingTypeId IN (2, 3) -- "Quote & Hold" and "Online Booking" 
			AND r.ActiveFlag = 1 ) fq
	WHERE fq.RowNum = 1

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Create #FirstQuote', @@ROWCOUNT;

	/* Get the max medium */
	IF OBJECT_ID('tempdb..#MaxSession') IS NOT NULL DROP TABLE #MaxSession;

	SELECT 
		ms.ReservationId,
		ms.QuoteId,
		ms.VisitId,
		ms.FullVisitorId,
		ms.WebsiteReferralMediumSessionId
	INTO #MaxSession
	FROM (
		SELECT 
			mq.ReservationId,
			qfa.QuoteId,
			qfa.VisitId,
			qfa.FullVisitorId,
			qfa.WebsiteReferralMediumSessionId,
			RowNum = ROW_NUMBER() OVER (PARTITION BY mq.ReservationId ORDER BY qfa.WebsiteReferralMediumSessionId DESC)
		FROM #FirstQuote mq
		INNER JOIN dbo.QuoteFact qfa WITH (NOLOCK) ON mq.QuoteId = qfa.QuoteId ) ms
	WHERE ms.RowNum = 1

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Create #MaxSession', @@ROWCOUNT;
	
	/* Get the BR source */
	IF OBJECT_ID('tempdb..#session') IS NOT NULL DROP TABLE #session;

	SELECT *
	INTO #session
	FROM (
		SELECT 
			br.*,
			vf.source,
			vf.visitStartTime,
			RowNum = ROW_NUMBER() OVER (PARTITION BY br.ReservationId ORDER BY vf.visitStartTime DESC)
		FROM #MaxSession br
		INNER JOIN dbo.QuoteFact qf WITH (NOLOCK) ON qf.ReservationId = br.ReservationId AND qf.QuoteId = br.QuoteId AND qf.WebsiteReferralMediumSessionId = br.WebsiteReferralMediumSessionId
		LEFT JOIN DW_Traveler.dbo.VisitorFact vf WITH(NOLOCK) ON qf.VisitId = vf.visitId AND qf.FullVisitorId = vf.fullVisitorId AND qf.QuoteCreatedDate = vf.visitdate
		) fs
	WHERE fs.RowNum = 1;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Create #session', @@ROWCOUNT;
	
	/* Update the BR fields */
	UPDATE bf
	SET bf.BookingRequestVisitId = ISNULL(s.VisitId,0),
		bf.BookingRequestFullVisitorId = ISNULL(s.FullVisitorId,'[Unknown]'),
		bf.BookingRequestMedium = wrms.WebsiteReferralMedium,
		bf.BookingRequestWebsiteReferralMediumSessionID = wrms.WebsiteReferralMediumSessionId,
		bf.BookingRequestSource = ISNULL(s.source,'[N/A]')
	FROM #final bf
	INNER JOIN #session s WITH(NOLOCK) ON s.ReservationId = bf.ReservationId
	INNER JOIN dbo.WebsiteReferralMediumSession wrms ON s.WebsiteReferralMediumSessionId = wrms.WebsiteReferralMediumSessionId
	WHERE bf.BookingRequestWebsiteReferralMediumSessionID = -1;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #final BookingRequest columns', @@ROWCOUNT;

	/* Classify mediums into subgroups */
	IF OBJECT_ID('tempdb..#BookingMedium') IS NOT NULL DROP TABLE #BookingMedium;
	
	SELECT 
		bf.ReservationId,
		BookingWebsiteReferralMediumSessionId = bf.WebsiteReferralMediumSessionId,
		BookingMediumCase = wrms.MarketingMedium,
		BookingMediumSource = ISNULL(vf.source,'[N/A]')
	INTO #BookingMedium
	FROM #final bf
	INNER JOIN dbo.WebsiteReferralMediumSession wrms WITH(NOLOCK) ON wrms.WebsiteReferralMediumSessionId = bf.WebsiteReferralMediumSessionId
	LEFT JOIN DW_Traveler.dbo.VisitorFact vf WITH(NOLOCK) ON bf.VisitId = vf.visitId AND bf.FullVisitorId = vf.fullVisitorId AND bf.BookingDateId = vf.visitdate;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Create #BookingMedium', @@ROWCOUNT;

	IF OBJECT_ID('tempdb..#OtherMediums') IS NOT NULL DROP TABLE #OtherMediums;
	
	SELECT 
		bf.ReservationId,
		BookingRequestMediumCase = wrmsBR.MarketingMedium,
		InquiryMediumCase = wrmsI.MarketingMedium
	INTO #OtherMediums
	FROM #final bf
	INNER JOIN dbo.WebsiteReferralMediumSession wrmsBR WITH(NOLOCK) ON wrmsBR.WebsiteReferralMediumSessionId = bf.BookingRequestWebsiteReferralMediumSessionID
	INNER JOIN dbo.WebsiteReferralMediumSession wrmsI WITH(NOLOCK) ON wrmsI.WebsiteReferralMediumSessionId = bf.InquiryWebsiteReferralMediumSessionID;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Create #OtherMediums', @@ROWCOUNT;

	/* Set the CalculatedBookingMedium, CalculatedBookingWebsiteReferralMediumSessionID, CalculatedBookingSource values on #final before the merge */
	UPDATE bf
	SET bf.CalculatedVisitId = bf.VisitId,
		bf.CalculatedFullVisitorId = bf.FullVisitorId,
		bf.CalculatedBookingMedium = bm.BookingMediumCase,
		bf.CalculatedBookingSource = bm.BookingMediumSource,
		bf.CalculatedBookingWebsiteReferralMediumSessionID = bm.BookingWebsiteReferralMediumSessionId
	FROM #final bf
	INNER JOIN #BookingMedium bm ON bm.ReservationId = bf.ReservationId;

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #final calculated fields when Booking is not undefined', @@ROWCOUNT;

	/* Set the CalculatedBookingMedium, CalculatedBookingWebsiteReferralMediumSessionID, CalculatedBookingSource values on #final before the merge when the calculated field is still undefined */
	UPDATE bf
	SET bf.CalculatedVisitId = bf.BookingRequestVisitId,
		bf.CalculatedFullVisitorId = bf.BookingRequestFullVisitorId,
		bf.CalculatedBookingMedium = om.BookingRequestMediumCase,
		bf.CalculatedBookingSource = bf.BookingRequestSource,
		bf.CalculatedBookingWebsiteReferralMediumSessionID = bf.BookingRequestWebsiteReferralMediumSessionID
	FROM #final bf
	INNER JOIN #OtherMediums om ON om.ReservationId = bf.ReservationId
	WHERE bf.CalculatedBookingMedium = 'Undefined'
		AND om.BookingRequestMediumCase <> 'Undefined';

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #final calculated fields when Booking medium is undefined', @@ROWCOUNT;

	UPDATE bf
	SET bf.CalculatedVisitId = bf.InquiryVisitId,
		bf.CalculatedFullVisitorId = bf.InquiryFullVisitorId,
		bf.CalculatedBookingMedium = om.InquiryMediumCase,
		bf.CalculatedBookingSource = bf.InquirySource,
		bf.CalculatedBookingWebsiteReferralMediumSessionID = bf.InquiryWebsiteReferralMediumSessionID
	FROM #final bf
	INNER JOIN #OtherMediums om ON om.ReservationId = bf.ReservationId
	WHERE bf.CalculatedBookingMedium = 'Undefined'
		AND om.InquiryMediumCase <> 'Undefined';

	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'Update #final calculated fields when Booking Request medium is undefined', @@ROWCOUNT;
	
	IF @debug = 0
	BEGIN
	/* merge final step */
		MERGE INTO DW_Facts.dbo.BookingFact f
		USING #final fin
		ON f.ReservationId = fin.ReservationId AND f.ReservationCreateDateId = fin.CreateDate
		WHEN MATCHED AND EXISTS (
			SELECT
				fin.BookingDateId
				,fin.BookingReportedDateId
				,fin.CancelledDateId
				,fin.BookingCategoryId
				,fin.BookingValueSourceId
				,fin.BrandAttributesId
				,fin.BrandId
				,fin.ListingChannelId
				,fin.CurrencyConversionIdBrandCurrency
				,fin.CurrencyConversionIdUSD
				,fin.CustomerAttributesId
				,fin.CustomerId
				,fin.DeviceCategorySessionId
				,fin.DisplayRegionId
				,fin.InquiryId
				,fin.ListingAttributesId
				,fin.ListingId
				,fin.ListingUnitAttributesId
				,fin.ListingUnitId
				,fin.LocalCurrencyId
				,fin.OnlineBookingProviderTypeId
				,fin.PaymentTypeId
				,fin.PersonTypeId
				,fin.RegionId
				,fin.ReservationAvailabilityStatusTypeId
				,fin.ReservationPaymentStatusTypeId
				,fin.SiteId
				,fin.SubscriptionId
				,fin.TravelerEmailId
				,fin.VisitorId
				,fin.BookingCount
				,fin.BookingValue
				,fin.OrderAmount
				,fin.RefundableDamageDepositAmount
				,fin.RentalAmount
				,fin.ServiceFeeAmount
				,fin.TaxAmount
				,fin.VasAmount
				,fin.RefundAmount
				,fin.PaidAmount
				,fin.CommissionAmount
				,fin.BookingChannelId
				,fin.PaymentMethodTypeId
				,fin.PaidAmountOffline
				,fin.PaidAmountOnline
				,fin.StrategicDestinationId
				,fin.StrategicDestinationAttributesId
				,fin.BookedNightsCount
				,fin.VisitId
				,fin.FullVisitorId
				,fin.WebsiteReferralMediumSessionId
				,fin.InquiryVisitId
				,fin.InquiryFullVisitorId
				,fin.InquiryMedium
				,fin.InquiryWebsiteReferralMediumSessionID
				,fin.InquirySource
				,fin.BookingRequestVisitId
				,fin.BookingRequestFullVisitorId
				,fin.BookingRequestMedium
				,fin.BookingRequestWebsiteReferralMediumSessionID
				,fin.BookingRequestSource
				,fin.CalculatedVisitId
				,fin.CalculatedFullVisitorId
				,fin.CalculatedBookingMedium
				,fin.CalculatedBookingWebsiteReferralMediumSessionID
				,fin.CalculatedBookingSource
			EXCEPT
			SELECT
				f.BookingDateId
				,f.BookingReportedDateId
				,f.CancelledDateId
				,f.BookingCategoryId
				,f.BookingValueSourceId
				,f.BrandAttributesId
				,f.BrandId
				,f.ListingChannelId
				,f.CurrencyConversionIdBrandCurrency
				,f.CurrencyConversionIdUSD
				,f.CustomerAttributesId
				,f.CustomerId
				,f.DeviceCategorySessionId
				,f.DisplayRegionId
				,f.InquiryId
				,f.ListingAttributesId
				,f.ListingId
				,f.ListingUnitAttributesId
				,f.ListingUnitId
				,f.LocalCurrencyId
				,f.OnlineBookingProviderTypeId
				,f.PaymentTypeId
				,f.PersonTypeId
				,f.RegionId
				,f.ReservationAvailabilityStatusTypeId
				,f.ReservationPaymentStatusTypeId
				,f.SiteId
				,f.SubscriptionId
				,f.TravelerEmailId
				,f.VisitorId
				,f.BookingCount
				,f.BookingValue
				,f.OrderAmount
				,f.RefundableDamageDepositAmount
				,f.RentalAmount
				,f.ServiceFeeAmount
				,f.TaxAmount
				,f.VasAmount
				,f.RefundAmount
				,f.PaidAmount
				,f.CommissionAmount
				,f.BookingChannelId
				,f.PaymentMethodTypeId
				,f.PaidAmountOffline
				,f.PaidAmountOnline
				,f.StrategicDestinationId
				,f.StrategicDestinationAttributesId
				,f.BookedNightsCount
				,f.VisitId
				,f.FullVisitorId
				,f.WebsiteReferralMediumSessionId
				,f.InquiryVisitId
				,f.InquiryFullVisitorId
				,f.InquiryMedium
				,f.InquiryWebsiteReferralMediumSessionID
				,f.InquirySource
				,f.BookingRequestVisitId
				,f.BookingRequestFullVisitorId
				,f.BookingRequestMedium
				,f.BookingRequestWebsiteReferralMediumSessionID
				,f.BookingRequestSource
				,f.CalculatedVisitId
				,f.CalculatedFullVisitorId
				,f.CalculatedBookingMedium
				,f.CalculatedBookingWebsiteReferralMediumSessionID
				,f.CalculatedBookingSource
				) THEN
			UPDATE SET
				f.BookingDateId = fin.BookingDateId
				,f.BookingReportedDateId = fin.BookingReportedDateId
				,f.CancelledDateId = fin.CancelledDateId
				,f.BookingCategoryId = fin.BookingCategoryId
				,f.BookingValueSourceId = fin.BookingValueSourceId
				,f.BrandAttributesId = fin.BrandAttributesId
				,f.BrandId = fin.BrandId
				,f.ListingChannelId = fin.ListingChannelId
				,f.CurrencyConversionIdBrandCurrency = fin.CurrencyConversionIdBrandCurrency
				,f.CurrencyConversionIdUSD = fin.CurrencyConversionIdUSD
				,f.CustomerAttributesId = fin.CustomerAttributesId
				,f.CustomerId = fin.CustomerId
				,f.DeviceCategorySessionId = fin.DeviceCategorySessionId
				,f.DisplayRegionId = fin.DisplayRegionId
				,f.InquiryId = fin.InquiryId
				,f.ListingAttributesId = fin.ListingAttributesId
				,f.ListingId = fin.ListingId
				,f.ListingUnitAttributesId = fin.ListingUnitAttributesId
				,f.ListingUnitId = fin.ListingUnitId
				,f.LocalCurrencyId = fin.LocalCurrencyId
				,f.OnlineBookingProviderTypeId = fin.OnlineBookingProviderTypeId
				,f.PaymentTypeId = fin.PaymentTypeId
				,f.PersonTypeId = fin.PersonTypeId
				,f.RegionId = fin.RegionId
				,f.ReservationAvailabilityStatusTypeId = fin.ReservationAvailabilityStatusTypeId
				,f.ReservationPaymentStatusTypeId = fin.ReservationPaymentStatusTypeId
				,f.SiteId = fin.SiteId
				,f.SubscriptionId = fin.Subscriptionid
				,f.TravelerEmailId = fin.TravelerEmailId
				,f.VisitorId = fin.VisitorId
				,f.BookingCount = fin.BookingCount
				,f.BookingValue = fin.BookingValue
				,f.OrderAmount = fin.OrderAmount
				,f.RefundableDamageDepositAmount = fin.RefundableDamageDepositAmount
				,f.RentalAmount = fin.RentalAmount
				,f.ServiceFeeAmount = fin.ServiceFeeAmount
				,f.TaxAmount = fin.TaxAmount
				,f.VasAmount = fin.VasAmount
				,f.RefundAmount = fin.RefundAmount
				,f.PaidAmount = fin.PaidAmount
				,f.CommissionAmount = fin.CommissionAmount
				,f.BookingChannelId = fin.BookingChannelId
				,f.PaymentMethodTypeId = fin.PaymentMethodTypeId
				,f.PaidAmountOffline = fin.PaidAmountOffline
				,f.PaidAmountOnline = fin.PaidAmountOnline
				,f.StrategicDestinationId=fin.StrategicDestinationId
				,f.StrategicDestinationAttributesId=fin.StrategicDestinationAttributesId
				,f.BookedNightsCount = fin.BookedNightsCount
				,f.VisitId = fin.VisitId
				,f.FullVisitorId = fin.FullVisitorId
				,f.WebsiteReferralMediumSessionId = fin.WebsiteReferralMediumSessionId
				,f.InquiryVisitId = fin.InquiryVisitId
				,f.InquiryFullVisitorId = fin.InquiryFullVisitorId
				,f.InquiryMedium = fin.InquiryMedium
				,f.InquiryWebsiteReferralMediumSessionID = fin.InquiryWebsiteReferralMediumSessionID
				,f.InquirySource = fin.InquirySource
				,f.BookingRequestVisitId = fin.BookingRequestVisitId
				,f.BookingRequestFullVisitorId = fin.BookingRequestFullVisitorId
				,f.BookingRequestMedium = fin.BookingRequestMedium
				,f.BookingRequestWebsiteReferralMediumSessionId = fin.BookingRequestWebsiteReferralMediumSessionID
				,f.BookingRequestSource = fin.BookingRequestSource
				,f.CalculatedVisitId = fin.CalculatedVisitId
				,f.CalculatedFullVisitorId = fin.CalculatedFullVisitorId
				,f.CalculatedBookingMedium = fin.CalculatedBookingMedium
				,f.CalculatedBookingWebsiteReferralMediumSessionId = fin.CalculatedBookingWebsiteReferralMediumSessionID
				,f.CalculatedBookingSource = fin.CalculatedBookingSource
				,f.DWUpdateDateTime = GETDATE()
		WHEN NOT MATCHED THEN
			INSERT (
				ReservationId
				,BookingDateId
				,BookingReportedDateId
				,CancelledDateId
				,ReservationCreateDateId
				,BookingCategoryId
				,BookingValueSourceId
				,BrandAttributesId
				,BrandId
				,ListingChannelId
				,CurrencyConversionIdBrandCurrency
				,CurrencyConversionIdUSD
				,CustomerAttributesId
				,CustomerId
				,DeviceCategorySessionId
				,DisplayRegionId
				,InquiryId
				,ListingAttributesId
				,ListingId
				,ListingUnitAttributesId
				,ListingUnitId
				,LocalCurrencyId
				,OnlineBookingProviderTypeId
				,PaymentTypeId
				,PersonTypeId
				,RegionId
				,ReservationAvailabilityStatusTypeId
				,ReservationPaymentStatusTypeId
				,SiteId
				,SubscriptionId
				,TravelerEmailId
				,VisitorId
				,BookingCount
				,BookingValue
				,OrderAmount
				,RefundableDamageDepositAmount
				,RentalAmount
				,ServiceFeeAmount
				,TaxAmount
				,VasAmount
				,RefundAmount
				,PaidAmount
				,CommissionAmount
				,BookingChannelId
				,PaymentMethodTypeId
				,PaidAmountOffline
				,PaidAmountOnline
				,StrategicDestinationId
				,StrategicDestinationAttributesId
				,BookedNightsCount
				,VisitId
				,FullVisitorId
				,WebsiteReferralMediumSessionId
				,InquiryVisitId
				,InquiryFullVisitorId
				,InquiryMedium
				,InquiryWebsiteReferralMediumSessionId
				,InquirySource
				,BookingRequestVisitId
				,BookingRequestFullVisitorId
				,BookingRequestMedium
				,BookingRequestWebsiteReferralMediumSessionId
				,BookingRequestSource
				,CalculatedVisitId
				,CalculatedFullVisitorId
				,CalculatedBookingMedium
				,CalculatedBookingWebsiteReferralMediumSessionId
				,CalculatedBookingSource
				,DWCreateDateTime
				,DWUpdateDateTime )
			VALUES (
				fin.ReservationId
				,fin.BookingDateId
				,fin.BookingReportedDateId
				,fin.CancelledDateId
				,fin.CreateDate
				,fin.BookingCategoryId
				,fin.BookingValueSourceId
				,fin.BrandAttributesId
				,fin.BrandId
				,fin.ListingChannelId
				,fin.CurrencyConversionIdBrandCurrency
				,fin.CurrencyConversionIdUSD
				,fin.CustomerAttributesId
				,fin.CustomerId
				,fin.DeviceCategorySessionId
				,fin.DisplayRegionId
				,fin.InquiryId
				,fin.ListingAttributesId
				,fin.ListingId
				,fin.ListingUnitAttributesId
				,fin.ListingUnitId
				,fin.LocalCurrencyId
				,fin.OnlineBookingProviderTypeId
				,fin.PaymentTypeId
				,fin.PersonTypeId
				,fin.RegionId
				,fin.ReservationAvailabilityStatusTypeId
				,fin.ReservationPaymentStatusTypeId
				,fin.SiteId
				,fin.SubscriptionId
				,fin.TravelerEmailId
				,fin.VisitorId
				,fin.BookingCount
				,fin.BookingValue
				,fin.OrderAmount
				,fin.RefundableDamageDepositAmount
				,fin.RentalAmount
				,fin.ServiceFeeAmount
				,fin.TaxAmount
				,fin.VasAmount
				,fin.RefundAmount
				,fin.PaidAmount
				,fin.CommissionAmount
				,fin.BookingChannelId
				,fin.PaymentMethodTypeId
				,fin.PaidAmountOffline
				,fin.PaidAmountOnline
				,fin.StrategicDestinationId
				,fin.StrategicDestinationAttributesId
				,fin.BookedNightsCount
				,fin.VisitId
				,fin.FullVisitorId
				,fin.WebsiteReferralMediumSessionId
				,fin.InquiryVisitId
				,fin.InquiryFullVisitorId
				,fin.InquiryMedium
				,fin.InquiryWebsiteReferralMediumSessionId
				,fin.InquirySource
				,fin.BookingRequestVisitId
				,fin.BookingRequestFullVisitorId
				,fin.BookingRequestMedium
				,fin.BookingRequestWebsiteReferralMediumSessionId
				,fin.BookingRequestSource
				,fin.CalculatedVisitId
				,fin.CalculatedFullVisitorId
				,fin.CalculatedBookingMedium
				,fin.CalculatedBookingWebsiteReferralMediumSessionId
				,fin.CalculatedBookingSource
				,GETDATE()
				,GETDATE() );

		EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'BookingFact Merge', @@ROWCOUNT;
	END 
	ELSE /* debug only */
	BEGIN
		SELECT * FROM #final;
	END

	EXEC DW_Config.dbo.PR_ETLControl_Update 'PR_BookingFact_Merge', @currentDateTime, @RowStartDate, NULL, NULL;
	EXEC DW_Config.dbo.PR_ETLControlLog_Insert @ETLControlId, 'END', -1;

END TRY
BEGIN CATCH

	IF ( XACT_STATE() ) != 0
		ROLLBACK TRANSACTION;

	EXECUTE DW_Config.dbo.PR_ErrorPrint;		/*	Call procedure to print error information.	*/
	DECLARE @ETLErrorLogId INT;
	EXECUTE DW_Config.dbo.PR_ETLErrorLog_Insert @ETLErrorLogId = @ETLErrorLogId OUTPUT;	/*	Insert the detailed error information into dbo.ETLErrorLog	*/
	RETURN @ETLErrorLogId;

END CATCH;
END;

GO

EXECUTE sys.sp_addextendedproperty @name = N'MS_Description',
@value = N'
 Description:	This stored procedure will incrementally load DW_Facts.BookingFact

Dependecies:	BookingFact , Reservation, ListingUnit, Brand, Calendar, Inquiry, OnlineBookingProviderType, Channel, ListingAttributes, Subscription,
			BrandAttributes, CustomerAttributes, PersonType, ListingUnitAttributes, Quote, QuoteItem, QuoteFact, RatePlan, ListingUnitDailyRate, 
			ListingUnitRate, BookingValueSource, Currency, CurrencyConversion

Audit Info:
Change By			Date			Description
Katherine Raney		11/23/2015	Create
Katherine Raney		12/11/2015	UniqueKey Violation in Bookingfact ISANA-5515
Madhu Yengala		12/14/2015	Add Quote Unk Row in BookingValue -ISANA-5481
Madhu Yengala		12/23/2015	Rename ColumnNames https://jira.homeawaycorp.com/browse/ISANA-5588
Logan Boyd			12/15/2015	ISANA-4924	Add BookingChannelId
Katherine Raney		1/13/2016		ISANA-5736 Add PaymentMethodTypeId, PaidAmountOffline, PaidAmountOnline 
Madhu Yengala		01/18/2016	ISANA-5741 Add DWUpdateDateTime
Madhu Yengala		01/27/2015	ISANA-5941	Add ETL Controllog to select,Update and Insert
Madhu Yengala		02/01/2016	ISANA-5979	BookingFact_Merge not detecting change in Reservation.BookingCategoryId
Katherine Raney		02/03/2016	ISANA-6110 Refining the GBV Estimation to filter out more rates that are obviously fake
Katherine Raney		02/12/2016	ISANA-6265 Making CancelledDate be NULL if the BookingCount goes to 0 
Katherine Raney		02/16/2016	ISANA-6278 Improve performance in final not-a-booking step and restrict to Reservation.CreateDate>=2014-01-01
Katherine Raney		02/19/2016	ISANA-6404 Pick up ServiceFee amount from TOPD for EXTERNAL_SOR bookings. Pick up ServiceFee VAT amount for all bookings.
Jeff Baird			2016-03-04	ISANA-6611	Add StrategicDestinationId and StrategicDestinationAttributesId columns
Katherine Raney		03/07/2016	ISANA-6492 Pick up standalone VAS not in QuoteItem
Katherine Raney		03/22/2016	ISANA-6642 Add parameters to assist in targeted processing
Jeff Baird			2016-05-12	ISANA-7417	Change logic for assigning destiniation ID values
Katherine Raney		2016-06-22	ISANA-7928 Remove reference to TravelerOrder.CurrencyCode, replace with TravelerOrderItem.CurrencyCode
Katherine Raney		2016-08-05	ISANA-7997 Modify lookup for SubscriptionId 
Katherine Raney		2016-09-21	ISANA-6696 Modify estimated booking value to utilize AverageKnownNightlyBookingValue 
Katherine Raney		2016-10-06	ISANA-8936 Add BookedNightCount to merge
Logan Boyd			2016-10-17	ISANA-9007	Adding InquiryMedium, InquirySource and InquiryWebsiteReferralMediumSessionID to BookingFact
MVinciguerra		2017-01-09	ISANA-9424 Added BookingRequestWebsiteReferralMediumId, BookingRequestMedium, BookingRequestSource, CalculatedBookingWebsiteReferralMediumId, CalculatedBookingMedium, CalculatedBookingSource
MVinciguerra		2017-04-18	AE-1459 Added VisitId/FullVisitorId for BookingRequest, Inquiry, and Calculated medium.
Logan Boyd			2017-05-17	AE-1851		Add Inquiry and BookingRequest VisitId/FullVisitorId to MERGE statement. Set VisitId and FullVisitorId from Reservation in #Final',
@level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'PROCEDURE', @level1name = N'PR_BookingFact_Merge'

GO
