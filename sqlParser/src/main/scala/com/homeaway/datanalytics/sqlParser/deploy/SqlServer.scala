package com.homeaway.datanalytics.sqlParser.deploy

/**
  * Created by aguyyala on 6/15/17.
  */

import java.sql.{DriverManager, ResultSet}
import javax.sql.rowset.CachedRowSet
import com.sun.rowset.CachedRowSetImpl
import com.homeaway.datanalytics.sqlParser.config.ParserConfig


class SqlServer(parserConfig: ParserConfig.Config) {

  import parserConfig._

  Class.forName(driver)


  /**
    * Retreive Columns of a table from SqlServer
    *
    * @param db      : Database Name
    * @param tblName : Table Name
    * @return
    */
  def getCols(db: String, tblName: String) = {

    var cols = Set[String]()

    val query =
      s"""
         |SELECT
         |DISTINCT LOWER(column_name) as columnName
         |FROM $db.INFORMATION_SCHEMA.COLUMNS
         |WHERE TABLE_NAME = N'$tblName';
       """.stripMargin

    val rs = executeQuery(query)

    while (rs.next()) {
      cols += rs.getString("columnName")
    }

    cols
  }


  /**
    * Get partition column info
    *
    * @param db      : Database Name
    * @param tblName : Table Name
    * @return
    */
  def getPartCols(db: String, tblName: String) = {

    var cols = Set[String]()

    val query =
      s"""
         |USE $db;
         |
         |SELECT
         |    DISTINCT LOWER(c.name) AS PartitioningColumnName
         |FROM sys.tables t
         |    INNER JOIN sys.indexes i
         |        ON i.object_id = t.object_id
         |    INNER JOIN sys.index_columns ic
         |        ON ic.index_id = i.index_id
         |            AND ic.object_id = t.object_id
         |    INNER JOIN sys.columns c
         |        ON c.object_id = ic.object_id
         |            AND c.column_id = ic.column_id
         |WHERE t.object_id  = object_id('$tblName')AND
         |    ic.partition_ordinal = 1
       """.stripMargin

    val rs = executeQuery(query)

    while (rs.next()) {
      cols += rs.getString("PartitioningColumnName")
    }

    cols
  }


  /**
    * Executes SqlServer Query
    *
    * @param query : Query to be executed
    * @return
    */
  def executeQuery(query: String) = {

    val conn = DriverManager.getConnection(s"$url;databaseName=$defaultDb;user=$user;password=$password;useUnicode=true;characterEncoding=UTF-8")
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    val rowset: CachedRowSet = new CachedRowSetImpl

    try {
      val rs = statement.executeQuery(query)
      rowset.populate(rs)

    } catch {

      case e: Exception => e.printStackTrace()

    } finally {
      conn.close()
    }

    rowset
  }


}



