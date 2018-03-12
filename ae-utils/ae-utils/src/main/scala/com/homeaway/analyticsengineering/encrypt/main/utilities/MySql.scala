package com.homeaway.analyticsengineering.encrypt.main.utilities

/**
  * Created by aguyyala on 11/09/17.
  */


import java.sql.ResultSet
import javax.sql.rowset.CachedRowSet
import com.homeaway.analyticsengineering.encrypt.secret.Decrypt
import com.sun.rowset.CachedRowSetImpl
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

final class MySql(spark: SparkSession) {

	val driver = "org.mariadb.jdbc.Driver"

	Class.forName(driver)

	private val log: Logger = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))

	private val rdb = "mysql"
	private val appName = "jobcontrol"


	/**
	  * Executes MySql Server Query
	  *
	  * @param query : Query to be executed
	  * @return Result Set of the query
	  */
	def executeQuery(query: String): CachedRowSet = {

		val conn = Decrypt.getConn(spark, appName, rdb)
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


	/**
	  * Executes Update, Delete, Insert Queries of MySQL Server
	  *
	  * @param query : Query to be executed
	  * @return No. of rows updated by that query
	  */
	def executeUpdate(query: String): Int = {

		val conn = Decrypt.getConn(spark, appName, rdb)
		val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

		var rs: Int = -1

		try {
			rs = statement.executeUpdate(query)

		} catch {
			case e: Exception => errorHandler(e)
			case e: Error => errorHandler(e)

		} finally {
			conn.close()
		}

		def errorHandler(e: Throwable): Unit = {
			log.error(s"Something Went Wrong while executing the Query $query")
			log.error(e.printStackTrace())
			System.exit(1)
		}

		rs
	}

}
