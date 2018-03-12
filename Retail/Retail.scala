import org.apache.spark.sql.SaveMode

spark.sparkContext.setLogLevel("warn")

// strategic_market_hierarchy
val smh = spark.read.parquet("s3a://.../geo/strategic_market_hierarchy")
smh.persist
smh.count
smh.createOrReplaceTempView("strategic_market_hierarchy")


// Stay Cube
var query =
     s"""
        |SELECT
        |     strategicDestinationUuid
        |    ,snapshotDate 
        |    ,arrivalDate
        |    ,departureDate
        |    ,stayDate
        |    ,MAX(date_format(arrivalDate, 'EEEE')) AS arrivalDay
        |    ,MAX(date_format(departureDate, 'EEEE')) AS departureDay
        |    ,MAX(date_format(stayDate, 'EEEE')) AS stayDay
        |    ,SUM(uniqueDatedSearch) AS uniqueDatedSearch
        |    ,SUM(totalDatedSearch) AS totalDatedSearch
        |    ,SUM(avgUniqueDatedSearch) AS avgUniqueDatedSearch
        |    ,SUM(avgTotalDatedSearch) AS avgTotalDatedSearch
        |FROM
        |(
        |SELECT
        |     COALESCE(smh.strategic_search_term_uuid, tmp.strategicDestinationUuid) AS strategicDestinationUuid
        |    ,snapshotDate 
        |    ,arrivalDate
        |    ,departureDate
        |    ,stayDate
        |    ,(COALESCE(smh.percentage_overlapped/ 100.0, 1.0) * uniqueDatedSearch ) AS uniqueDatedSearch
        |    ,(COALESCE(smh.percentage_overlapped/ 100.0, 1.0) * totalDatedSearch ) AS totalDatedSearch
        |    ,(COALESCE(smh.percentage_overlapped/ 100.0, 1.0) * avgUniqueDatedSearch ) AS avgUniqueDatedSearch
        |    ,(COALESCE(smh.percentage_overlapped/ 100.0, 1.0) * avgTotalDatedSearch ) AS avgTotalDatedSearch
        |FROM
        |(
        |SELECT
        |     strategicDestinationUuid
        |    ,snapshotDate
        |    ,arrivalDate
        |    ,departureDate
        |    ,c.dateid AS stayDate
        |    ,uniqueDatedSearch
        |    ,totalDatedSearch
        |    ,uniqueDatedSearch / (DATEDIFF(departureDate, arrivalDate) + 1) AS avgUniqueDatedSearch
        |    ,totalDatedSearch / (DATEDIFF(departureDate, arrivalDate) + 1) AS avgTotalDatedSearch
        |FROM
        |(
        |SELECT
        |     strategicDestinationUuid
        |    ,monthenddate AS snapshotDate
        |    ,CONCAT(substring(monthenddate, 0, 8), '01') AS monthStartDate
        |    ,CAST((monthenddate + INTERVAL 1 years) AS String) As nextYear
        |    ,arrivalDate
        |    ,departureDate
        |    ,COUNT(DISTINCT fullvisitorid + visitId) AS uniqueDatedSearch
        |    ,COUNT(*) AS totalDatedSearch
        |FROM
        |(SELECT dateid, monthenddate from tier1_edw_batch.calendar) c
        |JOIN
        |(SELECT
        |      dateid
        |     ,location_based_search_uuid AS strategicDestinationUuid
        |     ,TRIM(trip_start_date) AS arrivalDate
        |     ,TRIM(trip_end_date) AS departureDate
        |     ,fullvisitorid
        |     ,visitId
        |FROM tier1_edw_clickstream.enriched_events_ga
        |WHERE  ((dateid BETWEEN '20161201' AND '20161231') OR (dateid BETWEEN '20171201' AND '20171231'))
        |     AND hit_type = 'PAGE'
        |     AND page_type = 'search'
        |     AND location_based_search_uuid IS NOT NULL
        |     AND page_url_path RLIKE 'arrival|departure|from-date|to-date'
        |     AND TRIM(trip_start_date)  RLIKE '^\\\\d{4}-\\\\d{2}-\\\\d{2}$$'
        |     AND TRIM(trip_end_date) RLIKE '^\\\\d{4}-\\\\d{2}-\\\\d{2}$$'
        |) ga
        |ON TO_DATE(CAST(UNIX_TIMESTAMP(CAST(ga.dateid AS String), 'yyyyMMdd') AS TIMESTAMP)) = c.dateid
        |GROUP BY 1, 2, 3, 4, 5, 6
        |) cga
        |JOIN (SELECT dateid FROM tier1_edw_batch.calendar WHERE dateid >= '2016-12-01' AND dateid <= '2018-12-31') c
        |ON CAST(cga.arrivalDate AS DATE) <= c.dateid AND c.dateid <= CAST(cga.departureDate AS DATE)
        |AND monthStartDate <= dateid AND dateid <= nextYear
        |) tmp
        |LEFT OUTER JOIN strategic_market_hierarchy smh
        |ON tmp.strategicDestinationUuid = smh.search_term_uuid
        |) final
        |INNER JOIN tier1_edw_batch.strategicdestination sd
        |ON final.strategicDestinationUuid = sd.search_term_uuid
        |GROUP BY 1, 2, 3, 4, 5
    """.stripMargin

val sc = sql(query)
sc.unpersist
sc.persist
sc.count
sc.createOrReplaceTempView("sc")

sc.write.mode(SaveMode.Overwrite).saveAsTable("tier1_edw_olap.SearchMetricsSnapshotByStayDate")

sql("""SELECT snapshotdate, sum(uniquedatedSearch) uniquedatedsearch, sum(totaldatedSearch) totaldatedsearch, sum(avguniquedatedSearch) avguniquedatedSearch, sum(avgtotaldatedSearch) avgtotaldatedSearch FROM sc group by 1 order by 1 desc""").show(1000, false)


// Event Cube
query =
s"""
  |SELECT
  |     strategicDestinationUuid
  |    ,snapshotDate
  |    ,arrivalDate
  |    ,departureDate
  |    ,MAX(arrivalDay) AS arrivalDay
  |    ,MAX(departureDay) AS departureDay
  |    ,SUM(uniqueDatedSearch) AS uniqueDatedSearch
  |    ,SUM(totalDatedSearch) AS totalDatedSearch
  |    ,SUM(uniqueDatelessSearch) AS uniqueDatelessSearch
  |    ,SUM(totalDatelessSearch) AS totalDatelessSearch
  |FROM
  |(
  |SELECT
  |     COALESCE(smh.strategic_search_term_uuid, tmp.strategicDestinationUuid) AS strategicDestinationUuid
  |    ,snapshotDate
  |    ,arrivalDate
  |    ,departureDate
  |    ,arrivalDay
  |    ,departureDay
  |    ,(COALESCE(smh.percentage_overlapped/ 100.0, 1.0) * uniqueDatedSearch ) AS uniqueDatedSearch
  |    ,(COALESCE(smh.percentage_overlapped/ 100.0, 1.0) * totalDatedSearch ) AS totalDatedSearch
  |    ,(COALESCE(smh.percentage_overlapped/ 100.0, 1.0) * uniqueDatelessSearch ) AS uniqueDatelessSearch
  |    ,(COALESCE(smh.percentage_overlapped/ 100.0, 1.0) * totalDatelessSearch ) AS totalDatelessSearch
  |FROM
  |(
  |SELECT
  |     dated
  |    ,strategicDestinationUuid
  |    ,weekenddate AS snapshotDate
  |    ,CASE WHEN dated = 1 THEN arrivalDate ELSE '9999-12-31' END AS arrivalDate
  |    ,CASE WHEN dated = 1 THEN departureDate ELSE '9999-12-31' END AS departureDate
  |    ,CASE WHEN dated = 1 THEN date_format(arrivalDate, 'EEEE') ELSE 'Friday' END AS arrivalDay
  |    ,CASE WHEN dated = 1 THEN date_format(departureDate, 'EEEE') ELSE 'Friday' END AS departureDay
  |    ,CASE WHEN dated = 1 THEN COUNT(DISTINCT fullvisitorid + visitId) ELSE 0 END AS uniqueDatedSearch
  |    ,CASE WHEN dated = 1 THEN COUNT(*) ELSE 0 END AS totalDatedSearch
  |    ,CASE WHEN dated = 0 THEN COUNT(DISTINCT fullvisitorid + visitId) ELSE 0 END AS uniqueDatelessSearch
  |    ,CASE WHEN dated = 0 THEN COUNT(*) ELSE 0 END AS totalDatelessSearch
  |FROM
  |(SELECT dateid, weekenddate from tier1_edw_batch.calendar) c
  |JOIN
  |(SELECT
  |     location_based_search_uuid AS strategicDestinationUuid
  |    ,TRIM(trip_start_date) AS arrivalDate
  |    ,TRIM(trip_end_date) AS departureDate
  |    ,dateid
  |    ,fullvisitorid
  |    ,visitId
  |    ,page_url_path
  |    ,CASE
  |        WHEN page_url_path RLIKE 'arrival|departure|from-date|to-date'
  |            AND TRIM(trip_start_date)  RLIKE '^\\\\d{4}-\\\\d{2}-\\\\d{2}$$'
  |            AND TRIM(trip_end_date) RLIKE '^\\\\d{4}-\\\\d{2}-\\\\d{2}$$'
  |            AND TRIM(trip_start_date) <= TRIM(trip_end_date)
  |        THEN 1
  |        ELSE 0
  |     END AS dated
  |FROM tier1_edw_clickstream.enriched_events_ga
  |WHERE  ((dateid BETWEEN '20160529' AND '20161231') OR (dateid BETWEEN '20170528' AND '20171230'))
  |     AND hit_type = 'PAGE'
  |     AND page_type = 'search'
  |     AND location_based_search_uuid IS NOT NULL
  |) ga
  |ON TO_DATE(CAST(UNIX_TIMESTAMP(CAST(ga.dateid AS String), 'yyyyMMdd') AS TIMESTAMP)) = c.dateid
  |GROUP BY 1, 2, 3, 4, 5, 6, 7
  |) tmp
  |LEFT OUTER JOIN strategic_market_hierarchy smh
  |ON tmp.strategicDestinationUuid = smh.search_term_uuid
  |) final
  |INNER JOIN tier1_edw_batch.strategicdestination sd
  |ON final.strategicDestinationUuid = sd.search_term_uuid
  |GROUP BY 1, 2, 3, 4
 """.stripMargin

val ec = sql(query)
ec.unpersist
ec.persist
ec.count
ec.createOrReplaceTempView("ec")

ec.write.mode(SaveMode.Overwrite).saveAsTable("tier1_edw_olap.SearchMetricsSnapshotByEventDate")

sql("""SELECT snapshotdate, sum(uniquedatedSearch) uniquedatedsearch, sum(totaldatedSearch) totaldatedsearch, sum(avguniquedatedSearch) avguniquedatedSearch, sum(avgtotaldatedSearch) avgtotaldatedSearch FROM sc group by 1 order by 1 desc""").show(1000, false)
