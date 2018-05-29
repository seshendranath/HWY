package com.homeaway.analyticsengineering.utilities

/**
  * Created by aguyyala on 6/21/17.
  */


import java.sql.{DriverManager, ResultSet}
import javax.sql.rowset.CachedRowSet
import com.sun.rowset.CachedRowSetImpl
import org.apache.log4j.{Level, Logger}
import com.homeaway.analyticsengineering.AnalyticsTaskApp.allConf


class SqlServer {

	val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

	Class.forName(driver)

	private val log: Logger = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))

	val serverName: String = Utility.getConf(allConf("app"), "sqlserver", allConf("env"), "serverName")
	val url: String = s"jdbc:sqlserver://$serverName:1433"
	val user: String = Utility.getConf(allConf("app"), "sqlserver", allConf("env"), "user")
	val pwd: String = Utility.getConf(allConf("app"), "sqlserver", allConf("env"), "pwd")

	/**
	  * Executes SqlServer Query
	  *
	  * @param query : Query to be executed
	  * @return Result Set of the query
	  */
	def executeQuery(query: String): CachedRowSet = {

		val conn = DriverManager.getConnection(url, user, pwd)
		val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

		val rowset: CachedRowSet = new CachedRowSetImpl

		try {
			val rs = statement.executeQuery(query)
			rowset.populate(rs)

		} catch {
			case e@(_: Exception | _: Error) => errorHandler(e, query)

		} finally {
			conn.close()
		}

		rowset
	}


	/**
	  * Executes Update, Delete, Insert Queries of SQL Server
	  *
	  * @param query : Query to be executed
	  * @return No. of rows updated by that query
	  */
	def executeUpdate(query: String): Int = {

		val conn = DriverManager.getConnection(url, user, pwd)
		val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

		var rs: Int = -1

		try {
			rs = statement.executeUpdate(query)

		} catch {
			case e@(_: Exception | _: Error) => errorHandler(e, query)

		} finally {
			conn.close()
		}

		rs
	}

	def errorHandler(e: Throwable, query: String): Unit = {
		log.error(s"Something Went Wrong while executing the Query $query")
		log.error(e.printStackTrace())
		throw e
		//			System.exit(1)
	}

}
