package com.homeaway.analyticsengineering.task


/**
  * Created by aguyyala on 04/01/18.
  */


import com.homeaway.analyticsengineering.model.TableProperties
import com.homeaway.analyticsengineering.utilities.{Logging, Utility}
import com.homeaway.analyticsengineering.enums.SearchKeys
import com.homeaway.analyticsengineering.AnalyticsTaskApp.conf
import com.microsoft.sqlserver.jdbc.SQLServerException

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex


class Validation extends Logging {


	case class FaultyTable(tbl: String, stmt: String, error: String)

	case class FaultyColumns(tbl: String, stmt: String, invalidColumns: Set[String])

	case class FaultySearchKey(tbl: String, stmt: String, invalidSearchKey: Array[String])


	val substitutePattern: Regex = conf.substitutePattern
	val updatePattern: Regex = conf.updatePattern

	val faultyTables: mutable.Set[FaultyTable] = mutable.Set[FaultyTable]()
	val faultyColumns: ListBuffer[FaultyColumns] = ListBuffer[FaultyColumns]()
	val faultySearchKeys: ListBuffer[FaultySearchKey] = ListBuffer[FaultySearchKey]()


	def run(): Unit = {

		def validateDML(): Unit = {
			log.info("Get All Active Tables to Scan for PII Data")
			val activeTables: mutable.LinkedHashMap[String, TableProperties] = ForgetMeSqlOperations.getActiveTables()

			log.debug(s"Active Tables Map: $activeTables")

			for ((tbl, props) <- activeTables) {

				checkFaultyTables(tbl, props.selectStmt, props.updateStmt)
				checkFaultyColumns(tbl, props.updateStmt)
				checkFaultySearchKey(tbl, props.selectStmt, props.updateStmt)

			}

			log.info("Faulty Tables")
			faultyTables.foreach(x => log.info(x.tbl + " " + x.error + " " + x.stmt))

			log.info("Faulty Columns")
			faultyColumns.foreach(x => log.info(x.tbl + " " + x.invalidColumns.mkString(",") + " " + x.stmt))

			log.info("Faulty Search Keys")
			faultySearchKeys.foreach(x => log.info(x.tbl + " " + x.invalidSearchKey.mkString(",") + " " + x.stmt))

		}

		def checkFaultyTables(tbl: String, selectStmt: String, updateStmt: String): Unit = {

			log.info(s"Checking whether table $tbl is Faulty")

			try {
				val s = System.nanoTime
				ForgetMeSqlOperations.executeUpdateStmt(updateStmt.replaceAll(substitutePattern.toString, "00000"))
				Utility.timeit(s, s"Run Time of UPDATE for $tbl")
			} catch {
				case e: SQLServerException => e.printStackTrace(); faultyTables += FaultyTable(tbl, updateStmt, e.getMessage)
			}

			try {
				val s = System.nanoTime
				ForgetMeSqlOperations.executeSelectStmt(selectStmt.replaceAll(substitutePattern.toString, "00000"))
				Utility.timeit(s, s"Run Time of SELECT for $tbl")
			} catch {
				case e: SQLServerException => e.printStackTrace(); faultyTables += FaultyTable(tbl, updateStmt, e.getMessage)
			}
		}


		def checkFaultyColumns(tbl: String, updateStmt: String): Unit = {

			log.info(s"Checking whether table $tbl has Faulty Columns")

			val invalidCols = mutable.Set[String]()

			val updatePattern(_, _, _, updatedCols, _) = updateStmt

			val allCols = updatedCols.split(",").map(_.trim).filterNot(_.matches("[\\w\\.]+\\s+=\\s+getdate\\(\\)")).flatMap(_.split("=").map(_.trim.split("\\.").last))

			val actualCols = allCols.filter(_ (0).isLetter).toSet
			val obfuscatedCols = allCols.filterNot(_ (0).isLetter).map(_.replaceAll(s"'|${conf.obfuscatedKeyword}", "")).map(_.replaceAll(substitutePattern.toString, "$1")).toSet

			actualCols.foreach { c =>
				val query =
					s"""
					   |SELECT COUNT(*) AS cnt
					   |FROM ${conf.PIIMetadataTbl}
					   |WHERE LTRIM(RTRIM(LTRIM(RTRIM(DatabaseSchema)) + '.' + LTRIM(RTRIM(TableName)))) = '$tbl'
					   |      AND ColumnName = '$c'
					   |      AND ActiveFlag = 1
					 """.stripMargin
				val cnt = ForgetMeSqlOperations.getCountFromSelect(query)
				if (cnt == 0) invalidCols += c
			}

			val query =
				s"""
				   |SELECT LOWER(ColumnName) AS ColumnName, COUNT(*) AS cnt
				   |FROM ${conf.PIIMetadataTbl}
				   |WHERE LTRIM(RTRIM(LTRIM(RTRIM(DatabaseSchema)) + '.' + LTRIM(RTRIM(TableName)))) = '$tbl'
				   |      AND ActiveFlag = 1
				   |GROUP BY ColumnName
				 """.stripMargin
			val metaCols = mutable.Set[String]()
			val rs = ForgetMeSqlOperations.executeSelectStmt(query)
			while (rs.next) {
				val col = rs.getString(1)
				metaCols += col
				if (rs.getInt(2) > 1) invalidCols += col
			}

			invalidCols ++= (actualCols &~ obfuscatedCols) // ++ (obfuscatedCols &~ actualCols) ++ (actualCols &~ metaCols) ++ (metaCols &~ actualCols)

			if (invalidCols.nonEmpty) faultyColumns += FaultyColumns(tbl, updateStmt, invalidCols.toSet)
		}


		def checkFaultySearchKey(tbl: String, selectStmt: String, updateStmt: String): Unit = {

			log.info(s"Checking whether table $tbl has Faulty Search Key")

			val cols = substitutePattern.findAllIn(selectStmt).matchData.map(_.group(1)).filterNot(_.endsWith(conf.obfuscatedKeyword)).toArray

			if (cols.isEmpty || cols.length > 1 || !(SearchKeys.values.map(_.toString) contains cols.head)) {
				faultySearchKeys += FaultySearchKey(tbl, updateStmt, cols)
			}
		}


		validateDML()
	}
}
