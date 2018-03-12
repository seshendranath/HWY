package com.homeaway.datanalytics.sqlParser.deploy

/**
  * Created by aguyyala on 6/6/17.
  */

import java.io._

import org.hibernate.engine.jdbc.internal.BasicFormatterImpl
import com.homeaway.datanalytics.sqlParser.deploy.ScalaParser._
import com.homeaway.datanalytics.sqlParser.config.ParserConfig
import com.homeaway.datanalytics.sqlParser.config.ParserConfig.{ETLConfig, ETLExpression, JsonClass}
import play.api.libs.json.Json
import scala.collection.mutable.ListBuffer

class SqlParserCore(parserConfig: ParserConfig.Config) {

  import parserConfig._

  var etlConfig = ListBuffer[ETLConfig]()
  var etlExpression = ListBuffer[ETLExpression]()
  val log = SqlParser.log

  def runner() = {
    parseSql(input, output)
  }

  /**
    * Parses out sql file and generates appropriate spark commands
    *
    * @param inputFile  : Input Sql File
    * @param outputFile : Output Sql File
    */
  def parseSql(inputFile: String, outputFile: String) = {

    val cleanText = generateCleanTextFromFile(inputFile)

    val tblMap = extractTableNames(cleanText)

    val tblMetadata = buildMetadata(tblMap.values.toList)

    val tblColMap = extractColumns(cleanText, tblMap, tblMetadata)

    val writer = new PrintWriter(new File(outputFile))

    generateSqlStmts(writer, tblColMap, tblMetadata)

    generateDfStmts(writer, tblColMap)

    val declareVariablesMap = extractDeclaredVariables(cleanText)

    val replacedText = replaceText(cleanText, tblColMap, declareVariablesMap)

    generateQueries(writer, replacedText)

    writer.close()

    if (buildJson) {
      val jsonWriter = new PrintWriter(new File(inputFile.split("/").last.replace(".sql", "") + ".json"))
      val jsonClass = JsonClass(etlConfig, etlExpression)
      val jsonString = Json.prettyPrint(Json.toJson(jsonClass)).toString
      jsonWriter.write(jsonString)
      jsonWriter.close()

    }

  }


  /**
    * Generates clean text from the input file
    *
    * @param inputFile : Input Sql File
    * @return
    */
  def generateCleanTextFromFile(inputFile: String) = {
    var text = scala.io.Source.fromFile(inputFile).mkString.toLowerCase
    text = cleanTextMap.foldLeft(text) { case (z, (s, r)) => z.replaceAll(s, r) }

    var cleanText = text.split(delim).map(_.trim).filter(x => filterOut(x)).mkString(delim)
    cleanText = replaceCast(cleanText)
    cleanText
  }


  /**
    * Filter out the input text bases on the startsWithKeywords
    *
    * @param text : Input text to filter
    */
  def filterOut(text: String) = {
    startsWithKeywords.exists(x => text.startsWith(x))
  }


  /**
    * Replace all cast patterns with actual columns used in the query
    *
    * @param text : Input text to parse
    * @return
    */
  def replaceCast(text: String) = {
    // patternsMap("castPattern").replaceAllIn(text, """$1""")
    castPatternsMap.foldLeft(text) { case (z, (s, r)) => s.replaceAllIn(z, r) }
  }


  /**
    * Extract Table Names from the Sql File
    *
    * @param text : Input text to parse
    * @return
    */
  def extractTableNames(text: String) = {

    val rep = (x: String) => x.replace("..", ".dbo.")
    patternsMap("tblPattern").findAllIn(text).matchData.map(m => (if (m.group(2) != null) m.group(2) else rep(m.group(1)), rep(m.group(1))))
      .filterNot { case (_, v) => ignoreTable.contains(extractTbl(v).toLowerCase) || v.length == 1 }.toMap
  }


  /**
    * Extract Columns names from Table Aliases and others
    *
    * @param text : Input text to parse
    * @return
    */
  def extractColumns(text: String, tblMap: Map[String, String], tblMetadata: Map[String, Map[String, Set[String]]]) = {

    var tblColMap = Map[String, Set[String]]()

    for (k <- tblMap.keys) yield {
      val colPattern = ("""\b""" + k + """\.(\w+)\b""").r
      val res = colPattern.findAllIn(text).matchData.map(m => m.group(1)).toSet.intersect(tblMetadata(tblMap(k))("cols"))
      tblColMap += tblMap(k) -> (tblColMap.getOrElse(tblMap(k), Set()) ++ res)
    }

    text.split(delim).map(_.trim)
      .filter(t => queryKeywords.exists(key => t.startsWith(key)))
      .foreach { q =>

        val query = patternsMap("insertPattern").replaceAllIn(q, "")

        // One more way of extracting pure Words
        // val unMatchedCols = query.split("\\s").filter(w => w.matches("\\w*[a-zA-Z]\\w*") && !sqlServerKeywords.contains(w))

        val unMatchedCols = patternsMap("pureWords").findAllIn(query).matchData.map(_.group(2)).filterNot(x => sqlServerKeywords contains x)

        unMatchedCols.foreach { col =>
          for ((k, v) <- tblMetadata) if ((v.getOrElse("cols", Set()) contains col) && (query contains k)) tblColMap += k -> (tblColMap.getOrElse(k, Set()) ++ Set(col))
        }
      }

    tblColMap
  }


  /**
    * Build Table's Metadata from SqlServer
    *
    * @param tbls : tbls for which metadata to be build
    */
  def buildMetadata(tbls: Seq[String]) = {
    val sqlServer = new SqlServer(parserConfig)

    var tblMetadata = Map[String, Map[String, Set[String]]]()
    tbls.foreach { x =>
      val tblArray = x.split("\\.")
      val db = if (tblArray.length < 3) defaultDb else tblArray.head
      val tblName = tblArray.last
      val cols = sqlServer.getCols(db, tblName)
      val partCols = sqlServer.getPartCols(db, tblName)
      tblMetadata += x -> Map("cols" -> cols, "partCols" -> partCols)
    }
    tblMetadata
  }


  /**
    * Generate Sql Statements from the Table Column Map
    *
    * @param tblColMap : Table to Column Map
    */
  def generateSqlStmts(writer: PrintWriter, tblColMap: Map[String, Set[String]], tblMetadata: Map[String, Map[String, Set[String]]]) = {

    tblColMap.foreach { case (tbl, colList) =>
      val cols = if (colList.mkString.isEmpty) "*" else colList.mkString(", ")
      val tblName = extractTbl(tbl)
      val sqlSTblName = if (tbl.split("\\.").length < 3) s"$defaultDb.$tbl" else tbl
      var sqlStmt = s"""val sql$tblName = s"SELECT $cols FROM $sqlSTblName with (NOLOCK)"""


      val partCols = tblMetadata(tbl).getOrElse("partCols", Set())
      if (partCols.nonEmpty) {
        val msg =
          s"""/*
             |The below table $tbl is Partitioned on ${partCols.mkString(",")} column(s) in SqlServer.
             |Hence declaring WHERE clause to reduce load.
             |PLEASE ADJUST THE WHERE CLAUSE ACCORDING TO YOUR NEEDS.
             |*/\n""".stripMargin
        log.info(msg)
        writer.write(msg)

        sqlStmt += " WHERE "
        partCols.foreach { col =>
          var cnt = 0
          if (col contains "date") sqlStmt += s"""$col IN ('$$startDate', '$$prevDate')"""
          else sqlStmt += s"""$col = "SOME_PARTITION""""
          cnt += 1
          if (cnt > 0) sqlStmt += " AND "
        }
        sqlStmt = sqlStmt.stripSuffix(" AND ")

      }

      sqlStmt += s""""\n\n"""
      log.info(sqlStmt)
      writer.write(sqlStmt)

      if (buildJson) {
        val tblArr = sqlSTblName.split("\\.")
        val key = s"${tblArr.head}_${tblArr.last}".toUpperCase
        var jSqlStmt = sqlStmt.replace(s"$sqlSTblName with (NOLOCK)", s"$$$key")
        jSqlStmt = jSqlStmt.stripPrefix(s"val sql$tblName = s")
        jSqlStmt = jSqlStmt.replaceAll("\"", "").replaceAll("\n", "")
        etlExpression += ETLExpression(0, s"tmp$tblName", s"tmp$tblName", jSqlStmt, s"tmp$tblName", "false", "mssql")

        etlConfig += ETLConfig(key, sqlSTblName, "variable")
      }

    }

    log.info("\n\n\n")
    writer.write("\n\n\n")

  }


  /**
    * Generate DF Statements from the Table Column Map
    *
    * @param tblColMap : Table to Column Map
    */
  def generateDfStmts(writer: PrintWriter, tblColMap: Map[String, Set[String]]) = {

    tblColMap.foreach { case (tbl, _) =>
      val tblName = extractTbl(tbl)
      val dfStmt = s"""val df$tblName = edwLoader.getData(sql$tblName)\n\n"""
      log.info(dfStmt)
      writer.write(dfStmt)

      val dfpersist = s"""df$tblName.persist\n\n"""
      log.info(dfpersist)
      writer.write(dfpersist)

      val tmpView = s"""df$tblName.createOrReplaceTempView("tmp$tblName")\n\n"""
      log.info(tmpView)
      writer.write(tmpView)
    }
    log.info("\n\n\n")
    writer.write("\n\n\n")

  }


  /**
    * Extract Variables from Declare statements defined in Sql
    *
    * @param text : Input text to parse
    */
  def extractDeclaredVariables(text: String) = {

    var declareVariablesMap = Map[String, String]()
    val declarePattern = patternsMap("declarePattern")

    /**
      * Extracting the variables from Declare Pattern
      */
    text.split(delim).map(_.trim).filter(_.startsWith(declareKeyword)).foreach { x =>

      x.stripPrefix(declareKeyword).split(",").map(_.trim).foreach {

        case declarePattern(key, value) =>
          val v = {
            if (value contains "getdate")
              """'\$startDate'""" // Assuming date of run is named as startDate across all standards
            else if (value contains "previousdate")
              """'\$prevDate'""" // Assuming date of run - 1.day is named as prevDate across all standards
            else if (key contains "datedefault")
              """'1980-01-01'""" // Default Date
            else if (value matches "\\w+") value
            else if (value matches "n'\\w+'") value.replaceAll("n'", "").replaceAll("'", "")
            else declareVariablesMap.getOrElse(value, key)
          }

          declareVariablesMap += (key -> v)

        case _ =>
      }
    }
    declareVariablesMap
  }


  /**
    * Replace sqlServer text with original spark text
    */
  def replaceText(text: String, tblColMap: Map[String, Set[String]], declareVariablesMap: Map[String, String]) = {

    val tblDfMap = for ((tbl, _) <- tblColMap) yield ("\\b" + tbl + "\\b") -> s"tmp${extractTbl(tbl)}"

    val replaceStr = defaultReplaceMap ++ tblDfMap ++ declareVariablesMap

    val replacedText = replaceStr.foldLeft(text) { case (z, (s, r)) => z.replaceAll(s, r) }

    replacedText
  }


  /**
    * Generate Sql Queries from the replaced final text
    *
    * @param text : Input text to parse
    */
  def generateQueries(writer: PrintWriter, text: String) = {

    val sqlFormatter = new BasicFormatterImpl()

    var i = 0
    text.split(delim).map(_.trim)
      .filter(t => queryKeywords.exists(key => t.startsWith(key)))
      .foreach { x =>
        i += 1
        var query = if (x startsWith "update") convertUpdate(x) else x
        query = sqlFormatter.format(query)
        query += "\n\n\n"
        log.info(query)
        writer.write(query)

        if (buildJson) {
          etlExpression += ETLExpression(0, s"step$i", s"step$i", query, s"df$i", "false", "spark")
        }
      }

  }

  def convertUpdate(text: String) = {
    val res = parseAll(updateClouse(), text) match {
      case Success(r, _) => r
      case _ => text
    }

    if (res != text) {
      val cols = "select " + res.replaceAllLiterally("$", "\\$")

      if (text contains "from") {
        text.replaceFirst("""update(.*?)(?= from)""", cols)
      }
      else {
        text.replaceFirst("""update ([\w.]+).*""", cols + """ from $1""")
      }
    }
    else {
      text
    }
  }

}
