val sqlTravelerorderpayment = s"SELECT travelerorderpaymenttypename, travelerorderpaymentid, travelerorderpaymentstatusname, paymentdate, currencycode, amount, paymentmethodtypeid, travelerorderid FROM dw_traveler.dbo.travelerorderpayment with (NOLOCK)"

val sqlTravelerorderpaymentdistributiontype = s"SELECT travelerorderpaymentdistributiontypeid, travelerorderpaymentdistributiontypename FROM dw_traveler.dbo.travelerorderpaymentdistributiontype with (NOLOCK)"

val sqlPaymentmethodtype = s"SELECT paymentcollectiontype, paymentmethodtypeid FROM dw_traveler.dbo.paymentmethodtype with (NOLOCK)"

val sqlInquiryslafact = s"SELECT inquiryid, listingid, fullvisitorid, websitereferralmediumsessionid, visitid, inquirydate FROM dw.dbo.inquiryslafact with (NOLOCK)"

val sqlCurrency = s"SELECT currencyid, currencycode FROM dw.dbo.currency with (NOLOCK)"

val sqlQuote = s"SELECT reservationid, quoteguid, currencycode, quoteid, bookingtypeid, dwlastupdatedate FROM dw.dbo.quote with (NOLOCK)"

val sqlWebsitereferralmediumsession = s"SELECT marketingmedium, websitereferralmediumsessionid, websitereferralmedium FROM dw.dbo.websitereferralmediumsession with (NOLOCK)"

val sqlCustomerattributes = s"SELECT customerattributesid, rowstartdate, customerid, persontype, rowenddate FROM dw.dbo.customerattributes with (NOLOCK)"

val sqlListingunitattributes = s"SELECT listingunitattributesid, listingunitid, rowstartdate, rowenddate FROM dw.dbo.listingunitattributes with (NOLOCK)"

val sqlProduct = s"SELECT grossbookingvalueproductcategory, productid, productguid FROM dw_traveler.dbo.product with (NOLOCK)"

val sqlOnlinebookingprovidertype = s"SELECT onlinebookingprovidertypeid, olbprovidergatewaytypedescription, listingsourcename FROM dw.dbo.onlinebookingprovidertype with (NOLOCK)"

val sqlPersontype = s"SELECT persontypeid, persontypename FROM dw.dbo.persontype with (NOLOCK)"

val sqlListingunit = s"SELECT customerid, listingid, regionid, listingunitid, paymenttypeid FROM dw.dbo.listingunit with (NOLOCK)"

val sqlTravelerorderpaymentdistributionamounttype = s"SELECT travelerorderpaymentdistributionamounttypeid, travelerorderpaymentdistributionamounttypename FROM dw_traveler.dbo.travelerorderpaymentdistributionamounttype with (NOLOCK)"

val sqlChannel = s"SELECT channelid, channelname, listingsource FROM dw.dbo.channel with (NOLOCK)"

val sqlTravelerorderitem = s"SELECT travelerorderitemstatus, travelerorderitemid, currencycode, productid, amount, travelerorderid FROM dw_traveler.dbo.travelerorderitem with (NOLOCK)"

val sqlVw_quotegrossbookingvalue = s"SELECT bookingvalue, rentalamount, reservationid, orderamount, currencycode, quoteid, quotestatusid, refundabledamagedepositamount, vasamount, paymentmethodtypeid, taxamount, paidamount, servicefeeamount, refundamount, commissionamount FROM dw.dbo.vw_quotegrossbookingvalue with (NOLOCK)"

val sqlTravelerorderpaymentdistribution = s"SELECT travelerorderpaymentid, travelerorderitemid, updatedate, distributedamount, travelerorderpaymentdistributiontypeid, travelerorderpaymentdistributiontoaccounttypeid, travelerorderpaymentdistributionamounttypeid FROM dw_traveler.dbo.travelerorderpaymentdistribution with (NOLOCK)"

val sqlQuotefact = s"SELECT dwcreatedate, reservationid, displayregionid, quoteid, quotestatusid, quoteitemactiveflag, quotecreateddate, fullvisitorid, websitereferralmediumsessionid, visitid, dwlastupdatedate, quoteactiveflag FROM dw.dbo.quotefact with (NOLOCK)"

val sqlReservation = s"SELECT bookingcategoryid, departuredate, reservationpaymentstatustypeid, traveleremailid, brandid, reservationid, createdate, inquiryserviceentryguid, arrivaldate, cancelleddate, activeflag, dwupdatedatetime, devicecategorysessionid, siteid, fullvisitorid, bookingdate, visitorid, websitereferralmediumsessionid, reservationavailabilitystatustypeid, regionid, listingunitid, visitid, bookingreporteddate, onlinebookingprovidertypeid FROM dw.dbo.reservation with (NOLOCK)"

val sqlProductfulfillment = s"SELECT externalrefid, externalreftype, producttypeguid FROM dw_traveler.dbo.productfulfillment with (NOLOCK)"

val sqlBookingvaluesource = s"SELECT bookingvaluesourceid, bookingvaluesourcetype, bookingvaluesourcename FROM dw.dbo.bookingvaluesource with (NOLOCK)"

/*
The below table dw_traveler.dbo.visitorfact is Partitioned on dateid column(s) in SqlServer.
Hence declaring WHERE clause to reduce load.
PLEASE ADJUST THE WHERE CLAUSE ACCORDING TO YOUR NEEDS.
*/
val sqlVisitorfact = s"SELECT dateid, source, visitdate, fullvisitorid, visitstarttime, visitid FROM dw_traveler.dbo.visitorfact with (NOLOCK) WHERE dateid IN ('$startDate', '$prevDate')"

val sqlBrand = s"SELECT reportingcurcode, brandid FROM dw.dbo.brand with (NOLOCK)"

val sqlTravelerorder = s"SELECT reservationid, travelerorderid, quoteid FROM dw_traveler.dbo.travelerorder with (NOLOCK)"

val sqlAverageknownnightlybookingvalue = s"SELECT city, brandid, arrivalyearnumber, averagebookingvaluepernightusd, bookingquarterenddate, country, estimationstepnum, postalcode, listingunitid, bedroomnum, arrivalquarterenddate FROM dw.dbo.averageknownnightlybookingvalue with (NOLOCK)"

val sqlTravelerorderpaymentdistributiontoaccounttype = s"SELECT travelerorderpaymentdistributiontoaccounttypeid, travelerorderpaymentdistributiontoaccounttypename FROM dw_traveler.dbo.travelerorderpaymentdistributiontoaccounttype with (NOLOCK)"

val sqlBrandattributes = s"SELECT brandattributesid, brandid, rowstartdate, rowenddate FROM dw.dbo.brandattributes with (NOLOCK)"

val sqlCurrencyconversion = s"SELECT tocurrency, rowstartdate, conversionrate, fromcurrency, rowenddate, currencyconversionid FROM dw.dbo.currencyconversion with (NOLOCK)"

val sqlListingattributes = s"SELECT listingattributesid, listingid, rowstartdate, rowenddate FROM dw.dbo.listingattributes with (NOLOCK)"

/*
The below table dw_facts.dbo.bookingfact is Partitioned on reservationcreatedateid column(s) in SqlServer.
Hence declaring WHERE clause to reduce load.
PLEASE ADJUST THE WHERE CLAUSE ACCORDING TO YOUR NEEDS.
*/
val sqlBookingfact = s"SELECT customerattributesid, inquirymedium, brandattributesid, bookingvaluesourceid, bookingcategoryid, paidamountoffline, bookingvalue, rentalamount, reservationpaymentstatustypeid, traveleremailid, bookingrequestwebsitereferralmediumsessionid, brandid, reservationid, cancelleddateid, listingchannelid, orderamount, bookingrequestfullvisitorid, displayregionid, bookingdateid, strategicdestinationid, inquirysource, strategicdestinationattributesid, paidamountonline, refundabledamagedepositamount, calculatedvisitid, vasamount, currencyconversionidbrandcurrency, inquiryid, listingattributesid, customerid, bookingrequestsource, devicecategorysessionid, bookednightscount, bookingcount, paymentmethodtypeid, siteid, localcurrencyid, calculatedbookingwebsitereferralmediumsessionid, listingid, fullvisitorid, reservationcreatedateid, listingunitattributesid, taxamount, bookingrequestvisitid, visitorid, websitereferralmediumsessionid, reservationavailabilitystatustypeid, calculatedfullvisitorid, regionid, subscriptionid, listingunitid, visitid, paidamount, bookingchannelid, currencyconversionidusd, servicefeeamount, inquiryvisitid, paymenttypeid, calculatedbookingsource, inquirywebsitereferralmediumsessionid, calculatedbookingmedium, bookingrequestmedium, refundamount, onlinebookingprovidertypeid, persontypeid, bookingreporteddateid, commissionamount, inquiryfullvisitorid FROM dw_facts.dbo.bookingfact with (NOLOCK) WHERE reservationcreatedateid IN ('$startDate', '$prevDate')"

val sqlCalendar = s"SELECT quarterenddate, yearnumber, dateid FROM dw.dbo.calendar with (NOLOCK)"

val sqlListing = s"SELECT city, strategicdestinationid, country, listingid, postalcode, subscriptionid, paymenttypeid, bedroomnum FROM dw.dbo.listing with (NOLOCK)"

val sqlDestinationattributes = s"SELECT destinationattributesid, destinationid, rowstartdate, rowenddate FROM dw.dbo.destinationattributes with (NOLOCK)"




val dfTravelerorderpayment = edwLoader.getData(sqlTravelerorderpayment)

dfTravelerorderpayment.persist

dfTravelerorderpayment.createOrReplaceTempView("tmpTravelerorderpayment")

val dfTravelerorderpaymentdistributiontype = edwLoader.getData(sqlTravelerorderpaymentdistributiontype)

dfTravelerorderpaymentdistributiontype.persist

dfTravelerorderpaymentdistributiontype.createOrReplaceTempView("tmpTravelerorderpaymentdistributiontype")

val dfPaymentmethodtype = edwLoader.getData(sqlPaymentmethodtype)

dfPaymentmethodtype.persist

dfPaymentmethodtype.createOrReplaceTempView("tmpPaymentmethodtype")

val dfInquiryslafact = edwLoader.getData(sqlInquiryslafact)

dfInquiryslafact.persist

dfInquiryslafact.createOrReplaceTempView("tmpInquiryslafact")

val dfCurrency = edwLoader.getData(sqlCurrency)

dfCurrency.persist

dfCurrency.createOrReplaceTempView("tmpCurrency")

val dfQuote = edwLoader.getData(sqlQuote)

dfQuote.persist

dfQuote.createOrReplaceTempView("tmpQuote")

val dfWebsitereferralmediumsession = edwLoader.getData(sqlWebsitereferralmediumsession)

dfWebsitereferralmediumsession.persist

dfWebsitereferralmediumsession.createOrReplaceTempView("tmpWebsitereferralmediumsession")

val dfCustomerattributes = edwLoader.getData(sqlCustomerattributes)

dfCustomerattributes.persist

dfCustomerattributes.createOrReplaceTempView("tmpCustomerattributes")

val dfListingunitattributes = edwLoader.getData(sqlListingunitattributes)

dfListingunitattributes.persist

dfListingunitattributes.createOrReplaceTempView("tmpListingunitattributes")

val dfProduct = edwLoader.getData(sqlProduct)

dfProduct.persist

dfProduct.createOrReplaceTempView("tmpProduct")

val dfOnlinebookingprovidertype = edwLoader.getData(sqlOnlinebookingprovidertype)

dfOnlinebookingprovidertype.persist

dfOnlinebookingprovidertype.createOrReplaceTempView("tmpOnlinebookingprovidertype")

val dfPersontype = edwLoader.getData(sqlPersontype)

dfPersontype.persist

dfPersontype.createOrReplaceTempView("tmpPersontype")

val dfListingunit = edwLoader.getData(sqlListingunit)

dfListingunit.persist

dfListingunit.createOrReplaceTempView("tmpListingunit")

val dfTravelerorderpaymentdistributionamounttype = edwLoader.getData(sqlTravelerorderpaymentdistributionamounttype)

dfTravelerorderpaymentdistributionamounttype.persist

dfTravelerorderpaymentdistributionamounttype.createOrReplaceTempView("tmpTravelerorderpaymentdistributionamounttype")

val dfChannel = edwLoader.getData(sqlChannel)

dfChannel.persist

dfChannel.createOrReplaceTempView("tmpChannel")

val dfTravelerorderitem = edwLoader.getData(sqlTravelerorderitem)

dfTravelerorderitem.persist

dfTravelerorderitem.createOrReplaceTempView("tmpTravelerorderitem")

val dfVw_quotegrossbookingvalue = edwLoader.getData(sqlVw_quotegrossbookingvalue)

dfVw_quotegrossbookingvalue.persist

dfVw_quotegrossbookingvalue.createOrReplaceTempView("tmpVw_quotegrossbookingvalue")

val dfTravelerorderpaymentdistribution = edwLoader.getData(sqlTravelerorderpaymentdistribution)

dfTravelerorderpaymentdistribution.persist

dfTravelerorderpaymentdistribution.createOrReplaceTempView("tmpTravelerorderpaymentdistribution")

val dfQuotefact = edwLoader.getData(sqlQuotefact)

dfQuotefact.persist

dfQuotefact.createOrReplaceTempView("tmpQuotefact")

val dfReservation = edwLoader.getData(sqlReservation)

dfReservation.persist

dfReservation.createOrReplaceTempView("tmpReservation")

val dfProductfulfillment = edwLoader.getData(sqlProductfulfillment)

dfProductfulfillment.persist

dfProductfulfillment.createOrReplaceTempView("tmpProductfulfillment")

val dfBookingvaluesource = edwLoader.getData(sqlBookingvaluesource)

dfBookingvaluesource.persist

dfBookingvaluesource.createOrReplaceTempView("tmpBookingvaluesource")

val dfVisitorfact = edwLoader.getData(sqlVisitorfact)

dfVisitorfact.persist

dfVisitorfact.createOrReplaceTempView("tmpVisitorfact")

val dfBrand = edwLoader.getData(sqlBrand)

dfBrand.persist

dfBrand.createOrReplaceTempView("tmpBrand")

val dfTravelerorder = edwLoader.getData(sqlTravelerorder)

dfTravelerorder.persist

dfTravelerorder.createOrReplaceTempView("tmpTravelerorder")

val dfAverageknownnightlybookingvalue = edwLoader.getData(sqlAverageknownnightlybookingvalue)

dfAverageknownnightlybookingvalue.persist

dfAverageknownnightlybookingvalue.createOrReplaceTempView("tmpAverageknownnightlybookingvalue")

val dfTravelerorderpaymentdistributiontoaccounttype = edwLoader.getData(sqlTravelerorderpaymentdistributiontoaccounttype)

dfTravelerorderpaymentdistributiontoaccounttype.persist

dfTravelerorderpaymentdistributiontoaccounttype.createOrReplaceTempView("tmpTravelerorderpaymentdistributiontoaccounttype")

val dfBrandattributes = edwLoader.getData(sqlBrandattributes)

dfBrandattributes.persist

dfBrandattributes.createOrReplaceTempView("tmpBrandattributes")

val dfCurrencyconversion = edwLoader.getData(sqlCurrencyconversion)

dfCurrencyconversion.persist

dfCurrencyconversion.createOrReplaceTempView("tmpCurrencyconversion")

val dfListingattributes = edwLoader.getData(sqlListingattributes)

dfListingattributes.persist

dfListingattributes.createOrReplaceTempView("tmpListingattributes")

val dfBookingfact = edwLoader.getData(sqlBookingfact)

dfBookingfact.persist

dfBookingfact.createOrReplaceTempView("tmpBookingfact")

val dfCalendar = edwLoader.getData(sqlCalendar)

dfCalendar.persist

dfCalendar.createOrReplaceTempView("tmpCalendar")

val dfListing = edwLoader.getData(sqlListing)

dfListing.persist

dfListing.createOrReplaceTempView("tmpListing")

val dfDestinationattributes = edwLoader.getData(sqlDestinationattributes)

dfDestinationattributes.persist

dfDestinationattributes.createOrReplaceTempView("tmpDestinationattributes")





    select
        @etlcontrolid = etlcontrolid 
    from
        dw_config.dbo.etlcontrol 
    where
        processname = @processname



    create table #reslist (
        reservationid int 
    )



    insert 
    into
        #reslist
        select
            distinct qf.reservationid 
        from
            tmpQuotefact qf 
        where
            1 = 1 
            and qf.quotecreateddate >= @rollingtwoyears 
            and qf.quoteitemactiveflag = 1 
            and qf.quoteactiveflag = 1 
            and qf.quotestatusid >= -1 
            and (
                qf.dwcreatedate between cast ( @startdate as datetime ) and cast ( @enddate as datetime ) 
                or dwlastupdatedate between cast ( @startdate as datetime ) and cast ( @enddate as datetime ) 
            ) 
            and exists (
                select
                    1 
                from
                    tmpReservation r 
                where
                    r.activeflag = 1 
                    and r.createdate >= @rollingtwoyears 
                    and r.bookingcategoryid != 1 
                    and qf.reservationid = r.reservationid 
            )



    insert 
    into
        #reslist
        select
            r.reservationid 
        from
            tmpReservation r 
        where
            r.createdate >= @rollingtwoyears 
            and r.activeflag = 1 
            and r.bookingcategoryid != 1 
            and r.dwupdatedatetime between @startdate and @enddate 
            and not exists (
                select
                    1 
                from
                    #reslist r2 
                where
                    r2.reservationid = r.reservationid 
            )



    insert 
    into
        #reslist
        select
            distinct tto.reservationid 
        from
            tmpTravelerorder tto 
        inner join
            tmpTravelerorderitem toi 
                on tto.travelerorderid = toi.travelerorderid 
        inner join
            tmpTravelerorderpayment topp 
                on tto.travelerorderid = topp.travelerorderid 
        inner join
            tmpTravelerorderpaymentdistribution topd 
                on topp.travelerorderpaymentid = topd.travelerorderpaymentid 
                and topd.travelerorderitemid = topd.travelerorderitemid 
        where
            topp.travelerorderpaymenttypename = 'payment' 
            and topp.travelerorderpaymentstatusname in (
                'paid' , 'settled' , 'remitted' 
            ) 
            and topd.updatedate between @startdate and @enddate 
            and exists (
                select
                    1 
                from
                    tmpReservation r 
                where
                    r.reservationid = tto.reservationid 
                    and r.activeflag = 1 
                    and r.bookingcategoryid != 1 
                    and r.createdate >= @rollingtwoyears 
            ) 
            and not exists (
                select
                    1 
                from
                    #reslist r2 
                where
                    r2.reservationid = tto.reservationid 
            )



    select
        r.arrivaldate ,
        r.createdate  ,
        r.bookingcategoryid ,
        bookingdateid = r.bookingdate  ,
        bookingreporteddateid = r.bookingreporteddate  ,
        cancelleddateid = r.cancelleddate ,
        r.brandid ,
        r.listingunitid ,
        r.reservationid ,
        r.siteid ,
        r.traveleremailid ,
        visitorid = coalesce ( r.visitorid ,
        0 ) ,
        r.onlinebookingprovidertypeid ,
        reservationavailabilitystatustypeid = r.reservationavailabilitystatustypeid ,
        reservationpaymentstatustypeid = r.reservationpaymentstatustypeid  ,
        devicecategorysessionid = r.devicecategorysessionid ,
        r.inquiryserviceentryguid ,
        inquiryid = 0  ,
        lu.customerid ,
        lu.listingid ,
        b.reportingcurcode as brandcurrencycode ,
        lu.regionid ,
        bookednightscount = case 
            when r.arrivaldate = r.departuredate then 1 
            else datediff ( d ,
            r.arrivaldate ,
            r.departuredate ) 
        end ,
        brandattributesid = 0 ,
        customerattributesid = 0 ,
        listingattributesid = 0 ,
        listingunitattributesid = -1 ,
        subscriptionid = -1 ,
        paymenttypeid = -1 ,
        listingchannelid = -1 ,
        persontypeid = -1 ,
        bookingcount = 1 ,
        bookingchannelid = -1 ,
        strategicdestinationid = 0 ,
        strategicdestinationattributesid = 0 ,
        r.visitid ,
        r.fullvisitorid ,
        r.websitereferralmediumsessionid 
    into
        #res 
    from
        tmpReservation r 
    inner join
        tmpListingunit lu 
            on r.listingunitid = lu.listingunitid 
    inner join
        tmpBrand b 
            on r.brandid = b.brandid 
    inner join
        #reslist rl 
            on r.reservationid = rl.reservationid



    select
        i.inquiryid  as r.inquiryid 
    from
        #res r 
    inner join
        dbo.inquiry i 
            on r.inquiryserviceentryguid = i.inquiryserviceentryguid 
            and r.listingid = i.listingid



    select
        ch.channelid  as r.listingchannelid 
    from
        #res r 
    inner join
        tmpOnlinebookingprovidertype obpt 
            on r.onlinebookingprovidertypeid = obpt.onlinebookingprovidertypeid 
    left join
        tmpChannel ch 
            on case 
                when obpt.olbprovidergatewaytypedescription like 'clear stay%' then 'clearstay' 
                else obpt.listingsourcename 
            end = ch.channelname 
            and obpt.listingsourcename = ch.listingsource



    select
        la.listingattributesid  as r.listingattributesid 
    from
        #res r 
    inner join
        tmpListingattributes la 
            on r.listingid = la.listingid 
            and r.createdate between la.rowstartdate and la.rowenddate



    select
        s.subscriptionid  as r.subscriptionid,
        s.paymenttypeid  as r.paymenttypeid 
    from
        #res r 
    inner join
        dbo.subscription s 
            on r.listingid = s.listingid 
    where
        r.bookingdateid between s.subscriptionstartdate and s.subscriptionenddate



    select
        l.strategicdestinationid  as r.strategicdestinationid 
    from
        #res r 
    inner join
        tmpListing l 
            on r.listingid = l.listingid



    select
        da.destinationattributesid  as r.strategicdestinationattributesid 
    from
        #res r 
    inner join
        tmpDestinationattributes da 
            on r.strategicdestinationid = da.destinationid 
            and r.createdate between da.rowstartdate and da.rowenddate



    select
        ba.brandattributesid  as r.brandattributesid 
    from
        #res r 
    inner join
        tmpBrandattributes ba 
            on r.brandid = ba.brandid 
            and r.createdate between ba.rowstartdate and ba.rowenddate



    select
        ca.customerattributesid  as r.customerattributesid,
        coalesce ( pt.persontypeid,
        -1 ) as r.persontypeid 
    from
        #res r 
    inner join
        tmpCustomerattributes ca 
            on r.customerid = ca.customerid 
            and r.createdate between ca.rowstartdate and ca.rowenddate 
    left join
        tmpPersontype pt 
            on coalesce ( ca.persontype ,
        'unknown' ) = pt.persontypename



    select
        lua.listingunitattributesid  as r.listingunitattributesid 
    from
        #res r 
    inner join
        tmpListingunitattributes lua 
            on r.listingunitid = lua.listingunitid 
            and r.createdate between lua.rowstartdate and lua.rowenddate



    select
        s.bookingchannelid  as r.bookingchannelid 
    from
        #res r 
    inner join
        dbo.site s 
            on s.siteid = r.siteid 
    where
        s.bookingchannelid > -1



    select
        r.reservationid ,
        gbv.currencycode ,
        valuesource = cast ( case 
            when gbv.quotestatusid = -1 then 'quote unk' 
            else 'quote' 
        end as varchar ( 25 ) ) ,
        gbv.rentalamount ,
        gbv.refundabledamagedepositamount ,
        gbv.servicefeeamount ,
        gbv.taxamount ,
        gbv.vasamount ,
        gbv.orderamount ,
        gbv.paidamount ,
        gbv.refundamount ,
        gbv.bookingvalue ,
        gbv.commissionamount ,
        gbv.paymentmethodtypeid ,
        rownum = row_number ( ) over ( partition 
    by
        gbv.reservationid 
    order by
        cast ( case 
            when gbv.quotestatusid = -1 then 'quote unk' 
            else 'quote' 
        end as varchar ( 25 ) ) ,
        gbv.quoteid ) 
    into
        #qgbv 
    from
        tmpVw_quotegrossbookingvalue gbv 
    inner join
        #res r 
            on gbv.reservationid = r.reservationid



    with gbv as ( select
        reservationid ,
        currencycode ,
        valuesource ,
        rentalamount = sum ( rentalamount ) ,
        refundabledamagedepositamount = sum ( refundabledamagedepositamount ) ,
        servicefeeamount = sum ( servicefeeamount ) ,
        taxamount = sum ( taxamount ) ,
        vasamount = sum ( vasamount ) ,
        orderamount = sum ( orderamount ) ,
        paidamount = sum ( paidamount ) ,
        refundamount = sum ( refundamount ) ,
        bookingvalue = sum ( bookingvalue ) ,
        commissionamount = sum ( commissionamount ) 
    from
        #qgbv 
    group by
        reservationid ,
        currencycode ,
        valuesource ) select
        gbv.reservationid ,
        gbv.currencycode ,
        gbv.rentalamount ,
        gbv.refundabledamagedepositamount ,
        gbv.servicefeeamount ,
        gbv.taxamount ,
        gbv.vasamount ,
        gbv.orderamount ,
        gbv.paidamount ,
        gbv.refundamount ,
        gbv.bookingvalue ,
        gbv.valuesource ,
        displayregionid = -1 ,
        gbv.commissionamount ,
        paymentmethodtypeid = 0 ,
        paidamountoffline = 0 ,
        paidamountonline = 0 
    into
        #gbv 
    from
        gbv



    with dup as ( select
        reservationid ,
        min ( valuesource ) as minvaluesource ,
        max ( valuesource ) as maxvaluesource 
    from
        #gbv 
    group by
        reservationid 
    having
        count ( 1 ) > 1 ) delete 
    from
        g 
    from
        dup d 
    inner join
        #gbv g 
            on d.reservationid = g.reservationid 
            and d.maxvaluesource = g.valuesource 
            and d.maxvaluesource != d.minvaluesource



    with dup as ( select
        g.reservationid ,
        min ( g.valuesource ) as minvaluesource ,
        max ( g.valuesource ) as maxvaluesource 
    from
        #gbv g 
    group by
        g.reservationid 
    having
        count ( 1 ) > 1 ) delete 
    from
        g 
    from
        dup d 
    inner join
        #gbv g 
            on d.reservationid = g.reservationid 
            and d.maxvaluesource = g.valuesource 
            and d.maxvaluesource=d.minvaluesource 
    inner join
        #qgbv q 
            on g.reservationid=q.reservationid 
            and g.currencycode=q.currencycode 
            and q.rownum = 1



    select
        x.paymentmethodtypeid  as gbv.paymentmethodtypeid 
    from
        #qgbv x 
    inner join
        #gbv gbv 
            on x.reservationid = gbv.reservationid 
            and x.valuesource = gbv.valuesource 
    where
        x.rownum = 1 
        and x.paymentmethodtypeid > 0



    with x as ( select
        tot.reservationid ,
        g.currencycode ,
        amount = case 
            when g.currencycode != topp.currencycode 
            and cc.conversionrate is not null then topp.amount * cc.conversionrate 
            else topp.amount 
        end ,
        pmt.paymentcollectiontype 
    from
        dw_traveler..travelerorderpayment topp 
    inner join
        dw_traveler..travelerorder tot 
            on topp.travelerorderid = tot.travelerorderid 
    inner join
        dw_traveler..paymentmethodtype pmt 
            on pmt.paymentmethodtypeid = topp.paymentmethodtypeid 
    inner join
        #gbv g 
            on tot.reservationid = g.reservationid 
    left join
        tmpCurrencyconversion cc 
            on g.currencycode = cc.tocurrency  
            and topp.currencycode = cc.fromcurrency 
            and topp.paymentdate between cc.rowstartdate and cc.rowenddate 
    where
        topp.amount <> 0 
        and tot.quoteid > 0 
        and topp.travelerorderpaymenttypename in ( 'payment' )  
        and topp.travelerorderpaymentstatusname in ( 'paid' , 'settled' , 'remitted' ) ) select
        p.reservationid ,
        p.currencycode ,
        paidamountonline = coalesce ( p.online ,
        0 ) ,
        paidamountoffline = coalesce ( p.offline ,
        0 ) + coalesce ( p.unknown ,
        0 ) 
    into
        #gbv2 
    from
        x pivot ( sum ( amount ) for paymentcollectiontype in ( [online] ,
        [offline] ,
        [unknown] ) ) as p



    with r as ( select
        r.reservationid ,
        max ( q.displayregionid ) as displayregionid 
    from
        tmpQuotefact q 
    inner join
        #res r 
            on q.reservationid = r.reservationid 
    group by
        r.reservationid ) update
        g 
    set
        g.displayregionid = r.displayregionid 
    from
        #gbv g 
    inner join
        r 
            on g.reservationid = r.reservationid



    select
        tto.reservationid ,
        toi.currencycode ,
        servicefeeamount = sum ( topd.distributedamount ) ,
        servicefeevatamount = sum ( case 
            when topd.travelerorderpaymentdistributiontoaccounttypeid = 2 then topd.distributedamount 
            else 0 
        end ) ,
        servicefeeprocessingfeeamount = sum ( case 
            when topd.travelerorderpaymentdistributiontypeid in ( 10 ,
            5 ,
            4 ,
            2 ,
            1 ) then topd.distributedamount 
            else 0 
        end ) 
    into
        #sf 
    from
        tmpTravelerorder tto 
    inner join
        tmpTravelerorderitem toi 
            on tto.travelerorderid = toi.travelerorderid 
    inner join
        tmpProduct p 
            on toi.productid = p.productid 
    inner join
        tmpTravelerorderpayment topp 
            on tto.travelerorderid = topp.travelerorderid  
    inner join
        tmpTravelerorderpaymentdistribution topd 
            on topp.travelerorderpaymentid = topd.travelerorderpaymentid 
            and toi.travelerorderitemid = topd.travelerorderitemid 
    inner join
        #gbv gbv 
            on tto.reservationid = gbv.reservationid  
    where
        p.grossbookingvalueproductcategory = 'traveler_fee' 
        and topp.travelerorderpaymenttypename = 'payment' 
        and topp.travelerorderpaymentstatusname in (
            'paid' , 'settled' , 'remitted' 
        ) 
    group by
        tto.reservationid ,
        toi.currencycode



    select
        'quote and travelerorder'  as gbv.valuesource,
        case 
            when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount 
            else gbv.servicefeeamount  
        end as gbv.servicefeeamount,
        gbv.taxamount + sf.servicefeevatamount as gbv.taxamount,
        gbv.orderamount - gbv.servicefeeamount - gbv.taxamount + case 
            when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount 
            else gbv.servicefeeamount  
        end + gbv.taxamount + sf.servicefeevatamount as gbv.orderamount,
        gbv.bookingvalue - gbv.servicefeeamount - gbv.taxamount + case 
            when gbv.servicefeeamount = 0  then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount 
            else gbv.servicefeeamount  
        end + gbv.taxamount + sf.servicefeevatamount as gbv.bookingvalue 
    from
        #sf sf 
    inner join
        #gbv gbv 
            on sf.reservationid = gbv.reservationid 
            and sf.currencycode = gbv.currencycode  
    where
        gbv.servicefeeamount != case 
            when gbv.servicefeeamount = 0 then sf.servicefeeamount - sf.servicefeevatamount - sf.servicefeeprocessingfeeamount 
            else gbv.servicefeeamount 
        end 
        or sf.servicefeevatamount != 0



    select
        r.reservationid ,
        r.listingunitid ,
        r.regionid ,
        r.arrivaldate ,
        cal.quarterenddate as arrivalquarterenddate ,
        cal.yearnumber as arrivalyearnumber ,
        r.brandcurrencycode ,
        r.brandid ,
        r.bookednightscount ,
        l.bedroomnum ,
        l.postalcode ,
        l.city ,
        l.country ,
        bookingquarterenddate = calb.quarterenddate ,
        bookingyearnumber = calb.yearnumber ,
        currencycode = 'usd' ,
        bookingvalue = cast ( 0 as decimal ( 18 ,
        6 ) ) ,
        step = 0 
    into
        #est 
    from
        #res r 
    inner join
        dbo.bookingcategory bc 
            on r.bookingcategoryid = bc.bookingcategoryid 
    inner join
        tmpListing l 
            on r.listingid = l.listingid 
    inner join
        tmpCalendar cal 
            on r.arrivaldate = cal.dateid 
    inner join
        tmpCalendar calb 
            on r.bookingdateid = calb.dateid 
    where
        bc.bookingindicator = 1 
        and bc.knownbookingindicator = 0 
        and not exists (
            select
                g.reservationid 
            from
                #gbv g 
            where
                r.reservationid = g.reservationid 
        )



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        1  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 1 
            and f.listingunitid = x.listingunitid 
            and f.arrivalquarterenddate = x.arrivalquarterenddate



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        2  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 2 
            and f.listingunitid = x.listingunitid 
            and f.arrivalyearnumber = x.arrivalyearnumber 
    where
        f.step=0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        3  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 3 
            and f.listingunitid = x.listingunitid 
    where
        f.step=0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        4  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 4 
            and f.brandid = x.brandid 
            and f.bedroomnum = x.bedroomnum 
            and f.postalcode = x.postalcode 
            and f.bookingquarterenddate = x.bookingquarterenddate 
    where
        f.step = 0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        5  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 5 
            and f.brandid = x.brandid 
            and f.bedroomnum = x.bedroomnum 
            and f.postalcode = x.postalcode 
            and f.arrivalyearnumber = x.arrivalyearnumber 
    where
        f.step = 0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        6  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 6 
            and f.brandid = x.brandid 
            and f.bedroomnum = x.bedroomnum 
            and f.postalcode = x.postalcode 
    where
        f.step = 0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        7  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 7 
            and f.brandid = x.brandid 
            and f.bedroomnum = x.bedroomnum 
            and f.city = x.city 
            and f.country = x.country 
            and f.bookingquarterenddate = x.bookingquarterenddate 
    where
        f.step = 0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        8  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 8 
            and f.brandid = x.brandid 
            and f.bedroomnum = x.bedroomnum 
            and f.city = x.city 
            and f.country = x.country 
            and f.arrivalyearnumber = x.arrivalyearnumber 
    where
        f.step = 0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        9  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 9 
            and f.brandid = x.brandid 
            and f.bedroomnum = x.bedroomnum 
            and f.city = x.city 
            and f.country = x.country 
    where
        f.step = 0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        estimationstepnum  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 10 
            and f.brandid = x.brandid 
            and f.bedroomnum = x.bedroomnum 
    where
        f.step = 0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        estimationstepnum  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 11 
            and f.bedroomnum = x.bedroomnum 
    where
        f.step = 0



    select
        round ( x.averagebookingvaluepernightusd * f.bookednightscount ,
        6 ) as f.bookingvalue,
        estimationstepnum  as f.step 
    from
        #est f 
    inner join
        tmpAverageknownnightlybookingvalue x 
            on x.estimationstepnum = 12 
            and f.brandid = x.brandid 
    where
        f.step = 0



    insert 
    into
        #gbv
        (
            reservationid , currencycode , rentalamount , refundabledamagedepositamount , servicefeeamount , taxamount , vasamount , orderamount , paidamount , refundamount , bookingvalue , valuesource , displayregionid , commissionamount , paymentmethodtypeid , paidamountoffline , paidamountonline 
        ) select
            reservationid ,
            currencycode ,
            rentalamount=bookingvalue ,
            refundabledamagedepositamount = 0 ,
            servicefeeamount = 0 ,
            taxamount = 0 ,
            vasamount = 0 ,
            orderamount = bookingvalue ,
            paidamount = 0 ,
            refundamount = 0 ,
            bookingvalue ,
            valuesource = 'estimate step ' + cast ( step as varchar ( 5 ) ) ,
            -1 ,
            commissionamount = 0 ,
            paymentmethodtypeid = 0 ,
            paidamountoffline = 0 ,
            paidamountonline = 0 
        from
            #est



    select
        r.* ,
        bookingvaluesourceid = coalesce ( s.bookingvaluesourceid ,
        1 ) ,
        displayregionid = coalesce ( g.displayregionid ,
        -1 ) ,
        rentalamount = coalesce ( g.rentalamount ,
        0 ) ,
        refundabledamagedepositamount = coalesce ( g.refundabledamagedepositamount ,
        0 ) ,
        servicefeeamount = coalesce ( g.servicefeeamount ,
        0 ) ,
        taxamount = coalesce ( g.taxamount ,
        0 ) ,
        vasamount = coalesce ( g.vasamount ,
        0 ) ,
        orderamount = coalesce ( g.orderamount ,
        0 ) ,
        paidamount = coalesce ( g.paidamount ,
        0 ) ,
        refundamount = coalesce ( g.refundamount ,
        0 ) ,
        bookingvalue = coalesce ( g.bookingvalue ,
        0 ) ,
        localcurrencyid = coalesce ( c.currencyid ,
        154 ) ,
        currencyconversionidusd = coalesce ( ccusd.currencyconversionid ,
        -1 ) ,
        currencyconversionidbrandcurrency = coalesce ( ccbc.currencyconversionid ,
        -1 ) ,
        commissionamount = coalesce ( g.commissionamount ,
        0 ) ,
        paymentmethodtypeid = coalesce ( g.paymentmethodtypeid ,
        0 )  ,
        paidamountonline = coalesce ( g2.paidamountonline ,
        0 ) ,
        paidamountoffline = coalesce ( g2.paidamountoffline ,
        0 ) ,
        inquiryvisitid = 0 ,
        inquiryfullvisitorid = convert ( varchar ( 50 ) ,
        '[unknown]' ) ,
        inquirymedium = convert ( nvarchar ( 255 ) ,
        'undefined' ) ,
        inquirywebsitereferralmediumsessionid = convert ( int ,
        -1 ) ,
        inquirysource = convert ( nvarchar ( 255 ) ,
        '[n/a]' ) ,
        bookingrequestvisitid = 0 ,
        bookingrequestfullvisitorid = convert ( varchar ( 50 ) ,
        '[unknown]' ) ,
        bookingrequestmedium = convert ( nvarchar ( 255 ) ,
        'undefined' ) ,
        bookingrequestwebsitereferralmediumsessionid = convert ( int ,
        -1 ) ,
        bookingrequestsource = convert ( nvarchar ( 255 ) ,
        '[n/a]' ) ,
        calculatedvisitid = 0 ,
        calculatedfullvisitorid = convert ( varchar ( 50 ) ,
        '[unknown]' ) ,
        calculatedbookingmedium = convert ( nvarchar ( 255 ) ,
        'undefined' ) ,
        calculatedbookingwebsitereferralmediumsessionid = convert ( int ,
        -1 ) ,
        calculatedbookingsource = convert ( nvarchar ( 255 ) ,
        '[n/a]' ) 
    into
        #final 
    from
        #res r 
    left join
        #gbv g 
            on r.reservationid = g.reservationid 
    left join
        #gbv2 g2 
            on r.reservationid = g2.reservationid 
    left join
        tmpBookingvaluesource s 
            on g.valuesource = s.bookingvaluesourcename 
    left join
        tmpCurrency c 
            on g.currencycode = c.currencycode 
    left join
        tmpCurrencyconversion ccusd 
            on g.currencycode = ccusd.fromcurrency 
            and ccusd.tocurrency = 'usd' 
            and r.createdate between ccusd.rowstartdate and ccusd.rowenddate 
    left join
        tmpCurrencyconversion ccbc 
            on r.brandcurrencycode = ccbc.tocurrency 
            and g.currencycode = ccbc.fromcurrency 
            and r.createdate between ccbc.rowstartdate and ccbc.rowenddate



    with qf as ( select
        q.quoteid ,
        q.quoteitemid ,
        q.quoteitemcreateddate ,
        tp.quoteitemtype ,
        q.amountlocalcurrency ,
        q.reservationid 
    from
        tmpQuotefact q 
    inner join
        dbo.travelerproduct tp 
            on q.travelerproductid = tp.travelerproductid 
    inner join
        #res r 
            on q.reservationid = r.reservationid 
    where
        q.quotecreateddate >= '2013-01-01' 
        and q.quoteactiveflag = 1 
        and q.quoteitemactiveflag = 1 ) select
        tto.reservationid ,
        toi.travelerorderid ,
        r.brandcurrencycode ,
        coalesce ( cc.conversionrate ,
        1 ) as conversionrate ,
        coalesce ( q.currencycode ,
        toi.currencycode ) as localcurrencycode ,
        sum ( toi.amount * coalesce ( cc.conversionrate ,
        1 ) ) as standalonevasamt  
    into
        #vas 
    from
        tmpTravelerorder tto 
    inner join
        tmpTravelerorderitem toi 
            on toi.travelerorderid = tto.travelerorderid 
    inner join
        tmpProduct p 
            on toi.productid = p.productid 
    inner join
        tmpQuote q 
            on tto.quoteid = q.quoteid  
    inner join
        tmpProductfulfillment pf 
            on pf.externalrefid = q.quoteguid 
            and pf.externalreftype = 'quote' 
            and pf.producttypeguid = p.productguid 
    inner join
        #res r 
            on q.reservationid = r.reservationid 
    left join
        tmpCurrencyconversion cc 
            on toi.currencycode = cc.fromcurrency 
            and q.currencycode = cc.tocurrency 
            and r.createdate between cc.rowstartdate and cc.rowenddate 
    where
        1 = 1 
        and p.productguid not in (
            'traveler_protection' , 'commission' 
        ) 
        and p.grossbookingvalueproductcategory = 'vas'  
        and toi.travelerorderitemstatus = 'paid' 
        and not exists (
            select
                1 
            from
                qf 
            where
                tto.reservationid = qf.reservationid 
                and qf.quoteitemtype = p.productguid 
        ) 
    group by
        tto.reservationid ,
        toi.travelerorderid ,
        coalesce ( cc.conversionrate ,
        1 ) ,
        coalesce ( q.currencycode ,
        toi.currencycode ) ,
        r.brandcurrencycode



    select
        v.reservationid ,
        v.localcurrencycode ,
        v.brandcurrencycode ,
        round ( sum ( v.standalonevasamt ) ,
        6 ) as standalonevasamt  ,
        paidamount = coalesce ( round ( sum ( topp.paidamount * v.conversionrate ) ,
        6 ) ,
        0 ) ,
        paymentmethodtypeid = coalesce ( min ( topp.paymentmethodtypeid ) ,
        0 ) ,
        paidamountonline = coalesce ( round ( sum ( topp.paidamountonline * v.conversionrate ) ,
        6 ) ,
        0 ) ,
        paidamountoffline = coalesce ( round ( sum ( topp.paidamountoffline * v.conversionrate ) ,
        6 ) ,
        0 ) 
    into
        #vas2 
    from
        #vas v 
    left join
        (
            select
                tp.travelerorderid ,
                min ( tp.paymentmethodtypeid ) as paymentmethodtypeid ,
                paidamount = sum ( topd.distributedamount )  ,
                paidamountonline = sum ( case 
                    when pmt.paymentcollectiontype='online' then topd.distributedamount 
                    else 0 
                end ) ,
                paidamountoffline = sum ( case 
                    when pmt.paymentcollectiontype='offline' then topd.distributedamount 
                    else 0 
                end ) 
            from
                dw_traveler..travelerorderpayment tp 
            join
                dw_traveler..travelerorderpaymentdistribution topd 
                    on topd.travelerorderpaymentid = tp.travelerorderpaymentid 
            join
                dw_traveler..travelerorderpaymentdistributionamounttype at 
                    on topd.travelerorderpaymentdistributionamounttypeid = at.travelerorderpaymentdistributionamounttypeid 
            join
                dw_traveler..travelerorderpaymentdistributiontype topdt 
                    on topd.travelerorderpaymentdistributiontypeid = topdt.travelerorderpaymentdistributiontypeid 
            join
                dw_traveler..travelerorderpaymentdistributiontoaccounttype act 
                    on topd.travelerorderpaymentdistributiontoaccounttypeid = act.travelerorderpaymentdistributiontoaccounttypeid 
            join
                dw_traveler..paymentmethodtype pmt 
                    on pmt.paymentmethodtypeid = tp.paymentmethodtypeid 
            where
                at.travelerorderpaymentdistributionamounttypename = 'credit' 
                and act.travelerorderpaymentdistributiontoaccounttypename = 'vendor' 
                and tp.travelerorderpaymenttypename = 'payment' 
                and tp.travelerorderpaymentstatusname in (
                    'settled' , 'remitted' , 'paid' 
                ) 
                and topdt.travelerorderpaymentdistributiontypename = 'service' 
            group by
                tp.travelerorderid 
        ) topp 
            on v.travelerorderid = topp.travelerorderid 
    group by
        v.reservationid ,
        v.localcurrencycode ,
        v.brandcurrencycode



    select
        bf.vasamount + standalonevasamt as bf.vasamount,
        bf.orderamount + standalonevasamt as bf.orderamount,
        bf.bookingvalue + standalonevasamt as bf.bookingvalue,
        @quoteandtravelerorderbookingvaluesourceid  as bf.bookingvaluesourceid,
        bf.paidamount + v.paidamount as bf.paidamount 
    from
        #final bf 
    join
        tmpCurrency c 
            on bf.localcurrencyid = c.currencyid 
    join
        #vas2 v 
            on bf.reservationid = v.reservationid 
            and c.currencycode = v.localcurrencycode 
    join
        tmpBookingvaluesource bvs 
            on bf.bookingvaluesourceid = bvs.bookingvaluesourceid 
    where
        bvs.bookingvaluesourcetype = 'order'



    select
        bf.vasamount + standalonevasamt as bf.vasamount,
        bf.orderamount + standalonevasamt as bf.orderamount,
        bf.bookingvalue + standalonevasamt as bf.bookingvalue,
        @travelerorderbookingvaluesourceid  as bf.bookingvaluesourceid,
        vc.currencyid  as bf.localcurrencyid,
        cc.currencyconversionid  as bf.currencyconversionidusd,
        v.paymentmethodtypeid  as bf.paymentmethodtypeid,
        bf.paidamount + v.paidamount as bf.paidamount,
        bc.currencyconversionid  as bf.currencyconversionidbrandcurrency,
        bf.paidamountonline + v.paidamountonline as bf.paidamountonline,
        bf.paidamountoffline + v.paidamountoffline as bf.paidamountoffline 
    from
        #final bf 
    inner join
        tmpCurrency c 
            on bf.localcurrencyid = c.currencyid 
    inner join
        #vas2 v 
            on bf.reservationid = v.reservationid 
            and c.currencycode != v.localcurrencycode 
    inner join
        tmpCurrency vc 
            on v.localcurrencycode = vc.currencycode 
    inner join
        tmpCurrencyconversion cc 
            on vc.currencycode = cc.fromcurrency 
            and cc.tocurrency = 'usd' 
            and bf.bookingdateid between cc.rowstartdate and cc.rowenddate 
    inner join
        tmpCurrencyconversion bc 
            on v.brandcurrencycode = bc.tocurrency 
            and v.localcurrencycode = bc.fromcurrency 
            and bf.bookingdateid between bc.rowstartdate and bc.rowenddate 
    where
        bf.bookingvaluesourceid = 1



    insert 
    into
        #final
        (
            arrivaldate , createdate , bookingcategoryid , bookingdateid , bookingreporteddateid , cancelleddateid , brandid , listingunitid , reservationid , siteid , traveleremailid , visitorid , onlinebookingprovidertypeid , reservationavailabilitystatustypeid , reservationpaymentstatustypeid , devicecategorysessionid , inquiryserviceentryguid , inquiryid , customerid , listingid , brandcurrencycode , regionid , bookednightscount , brandattributesid , customerattributesid , listingattributesid , listingunitattributesid , subscriptionid , paymenttypeid , listingchannelid , persontypeid , bookingcount , bookingvaluesourceid , displayregionid , rentalamount , refundabledamagedepositamount , servicefeeamount , taxamount , vasamount , orderamount , paidamount , refundamount , bookingvalue , localcurrencyid , currencyconversionidusd , currencyconversionidbrandcurrency , commissionamount , bookingchannelid , paymentmethodtypeid , paidamountoffline , paidamountonline , strategicdestinationid , strategicdestinationattributesid , visitid , fullvisitorid , websitereferralmediumsessionid , inquiryvisitid , inquiryfullvisitorid , inquirymedium , inquirywebsitereferralmediumsessionid , inquirysource , bookingrequestvisitid , bookingrequestfullvisitorid , bookingrequestmedium , bookingrequestwebsitereferralmediumsessionid , bookingrequestsource , calculatedvisitid , calculatedfullvisitorid , calculatedbookingmedium , calculatedbookingwebsitereferralmediumsessionid , calculatedbookingsource 
        ) select
            r.arrivaldate ,
            r.createdate ,
            r.bookingcategoryid ,
            bookingdateid = null ,
            bookingreporteddateid = r.bookingreporteddate ,
            cancelleddateid = null ,
            f.brandid ,
            f.listingunitid ,
            r.reservationid ,
            f.siteid ,
            f.traveleremailid ,
            f.visitorid ,
            f.onlinebookingprovidertypeid ,
            f.reservationavailabilitystatustypeid ,
            f.reservationpaymentstatustypeid ,
            f.devicecategorysessionid ,
            r.inquiryserviceentryguid ,
            f.inquiryid ,
            f.customerid ,
            f.listingid ,
            brandcurrencycode = 'unk' ,
            f.regionid ,
            bookednightscount = 0 ,
            f.brandattributesid ,
            f.customerattributesid ,
            f.listingattributesid ,
            f.listingunitattributesid ,
            f.subscriptionid ,
            f.paymenttypeid ,
            f.listingchannelid ,
            f.persontypeid ,
            bookingcount = 0 ,
            f.bookingvaluesourceid ,
            f.displayregionid ,
            rentalamount = 0 ,
            refundabledamagedepositamount = 0 ,
            servicefeeamount = 0 ,
            taxamount = 0 ,
            vasamount = 0 ,
            orderamount = 0 ,
            paidamount = 0 ,
            refundamount = 0 ,
            bookingvalue = 0 ,
            f.localcurrencyid ,
            f.currencyconversionidusd ,
            f.currencyconversionidbrandcurrency ,
            commissionamount = 0 ,
            bookingchannelid = f.bookingchannelid ,
            paymentmethodtypeid = f.paymentmethodtypeid ,
            paidamountoffline = 0 ,
            paidamountonline = 0 ,
            f.strategicdestinationid ,
            f.strategicdestinationattributesid ,
            f.visitid ,
            f.fullvisitorid ,
            f.websitereferralmediumsessionid ,
            f.inquiryvisitid ,
            f.inquiryfullvisitorid ,
            f.inquirymedium ,
            f.inquirywebsitereferralmediumsessionid ,
            f.inquirysource ,
            f.bookingrequestvisitid ,
            f.bookingrequestfullvisitorid ,
            f.bookingrequestmedium ,
            f.bookingrequestwebsitereferralmediumsessionid ,
            f.bookingrequestsource ,
            f.calculatedvisitid ,
            f.calculatedfullvisitorid ,
            f.calculatedbookingmedium ,
            f.calculatedbookingwebsitereferralmediumsessionid ,
            f.calculatedbookingsource 
        from
            tmpReservation r 
        inner join
            tmpBookingfact f 
                on r.reservationid = f.reservationid 
                and r.createdate = f.reservationcreatedateid 
        where
            r.createdate >= @rollingtwoyears 
            and (
                r.activeflag = 0 
                or (
                    r.activeflag = 1 
                    and r.bookingcategoryid = 1 
                ) 
            ) 
            and r.dwupdatedatetime between @startdate and @enddate 
            and f.bookingcount = 1



    select
        coalesce ( i.visitid,
        0 ) as bf.inquiryvisitid,
        coalesce ( i.fullvisitorid,
        '[unknown]' ) as bf.inquiryfullvisitorid,
        wrms.websitereferralmedium  as bf.inquirymedium,
        wrms.websitereferralmediumsessionid  as bf.inquirywebsitereferralmediumsessionid 
    from
        #final bf 
    inner join
        tmpInquiryslafact i 
            on i.inquiryid = bf.inquiryid 
    inner join
        tmpWebsitereferralmediumsession wrms 
            on wrms.websitereferralmediumsessionid = i.websitereferralmediumsessionid 
    where
        bf.inquirymedium <> wrms.websitereferralmedium 
        or bf.inquirywebsitereferralmediumsessionid <> wrms.websitereferralmediumsessionid



    select
        vf.source  as bf.inquirysource 
    from
        #final bf 
    inner join
        tmpInquiryslafact i 
            on i.inquiryid = bf.inquiryid 
    inner join
        tmpVisitorfact vf 
            on vf.dateid = cast ( format ( i.inquirydate ,
        'yyyymmdd' ) as int ) 
        and vf.visitid = i.visitid 
        and vf.fullvisitorid = i.fullvisitorid 
    where
        bf.inquirysource <> vf.source



    select
        fq.reservationid ,
        fq.quoteid 
    into
        #firstquote 
    from
        ( select
            qf.reservationid ,
            qf.quoteid ,
            rownum = row_number ( ) over ( partition 
        by
            q.reservationid 
        order by
            qf.quoteid ) 
        from
            #final bf 
        inner join
            tmpQuotefact qf 
                on qf.reservationid = bf.reservationid 
        inner join
            tmpQuote q 
                on qf.quoteid = q.quoteid 
        inner join
            tmpReservation r 
                on qf.reservationid = r.reservationid 
        where
            qf.reservationid > -1 
            and q.bookingtypeid in (
                2 , 3 
            ) -- "quote & hold" 
            and "online booking" 
            and r.activeflag = 1 ) fq 
    where
        fq.rownum = 1 exec dw_config.dbo.pr_etlcontrollog_insert @etlcontrolid , 'create #firstquote' , @@rowcount



    select
        ms.reservationid ,
        ms.quoteid ,
        ms.visitid ,
        ms.fullvisitorid ,
        ms.websitereferralmediumsessionid 
    into
        #maxsession 
    from
        ( select
            mq.reservationid ,
            qfa.quoteid ,
            qfa.visitid ,
            qfa.fullvisitorid ,
            qfa.websitereferralmediumsessionid ,
            rownum = row_number ( ) over ( partition 
        by
            mq.reservationid 
        order by
            qfa.websitereferralmediumsessionid desc ) 
        from
            #firstquote mq 
        inner join
            tmpQuotefact qfa 
                on mq.quoteid = qfa.quoteid ) ms 
    where
        ms.rownum = 1 exec dw_config.dbo.pr_etlcontrollog_insert @etlcontrolid , 'create #maxsession' , @@rowcount



    select
        * 
    into
        #session 
    from
        ( select
            br.* ,
            vf.source ,
            vf.visitstarttime ,
            rownum = row_number ( ) over ( partition 
        by
            br.reservationid 
        order by
            vf.visitstarttime desc ) 
        from
            #maxsession br 
        inner join
            tmpQuotefact qf 
                on qf.reservationid = br.reservationid 
                and qf.quoteid = br.quoteid 
                and qf.websitereferralmediumsessionid = br.websitereferralmediumsessionid 
        left join
            tmpVisitorfact vf 
                on qf.visitid = vf.visitid 
                and qf.fullvisitorid = vf.fullvisitorid 
                and qf.quotecreateddate = vf.visitdate ) fs 
    where
        fs.rownum = 1



    select
        coalesce ( s.visitid,
        0 ) as bf.bookingrequestvisitid,
        coalesce ( s.fullvisitorid,
        '[unknown]' ) as bf.bookingrequestfullvisitorid,
        wrms.websitereferralmedium  as bf.bookingrequestmedium,
        wrms.websitereferralmediumsessionid  as bf.bookingrequestwebsitereferralmediumsessionid,
        coalesce ( s.source,
        '[n/a]' ) as bf.bookingrequestsource 
    from
        #final bf 
    inner join
        #session s 
            on s.reservationid = bf.reservationid 
    inner join
        tmpWebsitereferralmediumsession wrms 
            on s.websitereferralmediumsessionid = wrms.websitereferralmediumsessionid 
    where
        bf.bookingrequestwebsitereferralmediumsessionid = -1



    select
        bf.reservationid ,
        bookingwebsitereferralmediumsessionid = bf.websitereferralmediumsessionid ,
        bookingmediumcase = wrms.marketingmedium ,
        bookingmediumsource = coalesce ( vf.source ,
        '[n/a]' ) 
    into
        #bookingmedium 
    from
        #final bf 
    inner join
        tmpWebsitereferralmediumsession wrms 
            on wrms.websitereferralmediumsessionid = bf.websitereferralmediumsessionid 
    left join
        tmpVisitorfact vf 
            on bf.visitid = vf.visitid 
            and bf.fullvisitorid = vf.fullvisitorid 
            and bf.bookingdateid = vf.visitdate



    select
        bf.reservationid ,
        bookingrequestmediumcase = wrmsbr.marketingmedium ,
        inquirymediumcase = wrmsi.marketingmedium 
    into
        #othermediums 
    from
        #final bf 
    inner join
        tmpWebsitereferralmediumsession wrmsbr 
            on wrmsbr.websitereferralmediumsessionid = bf.bookingrequestwebsitereferralmediumsessionid 
    inner join
        tmpWebsitereferralmediumsession wrmsi 
            on wrmsi.websitereferralmediumsessionid = bf.inquirywebsitereferralmediumsessionid



    select
        bf.visitid  as bf.calculatedvisitid,
        bf.fullvisitorid  as bf.calculatedfullvisitorid,
        bm.bookingmediumcase  as bf.calculatedbookingmedium,
        bm.bookingmediumsource  as bf.calculatedbookingsource,
        bm.bookingwebsitereferralmediumsessionid  as bf.calculatedbookingwebsitereferralmediumsessionid 
    from
        #final bf 
    inner join
        #bookingmedium bm 
            on bm.reservationid = bf.reservationid



    select
        bf.bookingrequestvisitid  as bf.calculatedvisitid,
        bf.bookingrequestfullvisitorid  as bf.calculatedfullvisitorid,
        om.bookingrequestmediumcase  as bf.calculatedbookingmedium,
        bf.bookingrequestsource  as bf.calculatedbookingsource,
        bf.bookingrequestwebsitereferralmediumsessionid  as bf.calculatedbookingwebsitereferralmediumsessionid 
    from
        #final bf 
    inner join
        #othermediums om 
            on om.reservationid = bf.reservationid 
    where
        bf.calculatedbookingmedium = 'undefined' 
        and om.bookingrequestmediumcase <> 'undefined'



    select
        bf.inquiryvisitid  as bf.calculatedvisitid,
        bf.inquiryfullvisitorid  as bf.calculatedfullvisitorid,
        om.inquirymediumcase  as bf.calculatedbookingmedium,
        bf.inquirysource  as bf.calculatedbookingsource,
        bf.inquirywebsitereferralmediumsessionid  as bf.calculatedbookingwebsitereferralmediumsessionid 
    from
        #final bf 
    inner join
        #othermediums om 
            on om.reservationid = bf.reservationid 
    where
        bf.calculatedbookingmedium = 'undefined' 
        and om.inquirymediumcase <> 'undefined'


