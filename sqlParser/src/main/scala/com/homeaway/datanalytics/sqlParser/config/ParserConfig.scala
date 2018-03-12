package com.homeaway.datanalytics.sqlParser.config

/**
  * Created by aguyyala on 6/7/17.
  */


import com.typesafe.config.ConfigFactory

import scala.collection.immutable.ListMap
import scala.util.matching.Regex
import scopt.OptionParser
import scala.collection.JavaConverters._
import play.api.libs.json.{Json, Writes}


object ParserConfig {

  def parseCmdLineArguments(args: Array[String]) = {

    val parser = new OptionParser[Config]("SqlParser") {
      head("SqlParser")
      opt[String]('i', "input").required() action ((x, c) => c.copy(input = x)) text "Input File"
      opt[String]('o', "output").required() action ((x, c) => c.copy(output = x)) text "Output File"

      opt[String]('e', "env").action((x, c) => c.copy(env = x))
        .validate(x =>
          if (Seq("dev", "stage", "prod") contains x) success
          else failure("Invalid env: only takes dev, stage, or prod"))
        .text("Environment in which you want to run dev|stage|prod")

      opt[Unit]("buildJson").action { (_, c) =>
        c.copy(buildJson = true)
      }.text("Produce Json File Flag")

      opt[Unit]("debug").action { (_, c) =>
        c.copy(debug = true)
      }.text("Debug Flag")

      help("help").text("Prints Usage Text")

      override def showUsageOnError = true

      override def errorOnUnknownArgument = true
    }

    val pConf = parser.parse(args, Config()).getOrElse(Config())

    val properties = ConfigFactory.parseResourcesAnySyntax("mssql.conf").entrySet.asScala.map(e => (e.getKey, e.getValue.unwrapped.toString)).toMap
    pConf.url = properties.getOrElse(s"sqlServer.${pConf.env}.url", "None")
    pConf.user = properties.getOrElse(s"sqlServer.${pConf.env}.user", "None")
    pConf.password = properties.getOrElse(s"sqlServer.${pConf.env}.password", "None")

    if (pConf.debug) pConf.logLevel = "Debug"

    pConf

  }

  case class Config(input: String = "None",
                    output: String = "None",
                    env: String = "stage",
                    defaultDb: String = "dw",
                    debug: Boolean = false,
                    delim: String = delim,
                    cleanTextMap: ListMap[String, String] = cleanTextMap,
                    patternsMap: Map[String, Regex] = patternsMap,
                    castPatternsMap: Map[Regex, String] = castPatternsMap,
                    defaultReplaceMap: Map[String, String] = defaultReplaceMap,
                    extractTbl: (String) => String = extractTbl,
                    queryKeywords: Set[String] = queryKeywords,
                    startsWithKeywords: Set[String] = startsWithKeywords,
                    ignoreTable: Set[String] = ignoreTable,
                    declareKeyword: String = declareKeyword,
                    sqlServerKeywords: Set[String] = sqlServerKeywords,
                    var url: String = "None",
                    var driver: String = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    var user: String = "None",
                    var password: String = "None",
                    var logLevel: String = "Info",
                    buildJson: Boolean = false,
                    etlConfigWrites: Writes[ETLConfig] = etlConfigWrites,
                    etlExpressionWrites: Writes[ETLExpression] = etlExpressionWrites,
                    jsonClassWrites: Writes[JsonClass] = jsonClassWrites
                   )


  /* Default Delimiter in Sql file */
  val delim = ";"

  /* Perform all the below transformations to clean up the input text*/
  val cleanTextMap = ListMap("(with)?\\s?\\(?\\s?nolock\\s?\\)?" -> "", "\\(" -> " ( ", "\\)" -> " ) ", "," -> " , ", "\\s+" -> " ", "/\\*.*?\\*/" -> "")

  /**
    * All the Regex Patterns are stored here in the below Map
    */
  val patternsMap: Map[String, Regex] = Map(
    "tblPattern" -> """(?:from|join)\s+([\w.]+)\s+((?!where)\w+)?""".r
//    , "declarePattern" -> """declare\s(@\w+).*\s=\s(.*)""".r
    , "declarePattern" -> """(@\w+)(?:.*?)\s?=\s?(.*)""".r
    , "castPattern" -> """cast\s\(\s([\w.]+)\sas\s\w+\s\)""".r
    , "insertPattern" -> """insert into [\w.]+ \(.*?\) """.r
    , "pureWords" -> """(^| )(\w*[a-zA-Z]\w*)(?= |$)""".r // Make Sure to Extract 2nd Groupn for pure words
  )

  val castPatternsMap = Map("""cast\s\(\s([\w.]+)\sas\sbit\s\)""".r -> """cast ( $1 as tinyint )""", """cast\s\(\s([\w.]+)\sas\svarchar\s\)""".r -> """cast ( $1 as string )""")

  /**
    * All the default replaces are stored here in the below Map
    */
  val defaultReplaceMap = Map("isnull" -> "coalesce", "\\(nolock\\)" -> "", "/\\*.*\\*/" -> "")

  /* Extract original table name from the fully qualified name and capitalize it */
  val extractTbl = (tbl: String) => tbl.split('.').last.capitalize

  /* Filter out the input text that starts with the below query keywords */
  val queryKeywords = Set("create table", "insert", "select", "merge", "update", "with")

  val declareKeyword = "declare"

  /* Filter out the input text that starts with the below keywords */
  val startsWithKeywords = Set(declareKeyword) ++ queryKeywords

  /* Ignore tables list */
  val ignoreTable = Set("etlcontrol", "indexes")

  /* Sql Server Reserved Keywords*/
  val sqlServerKeywords = Set("absolute", "action", "ada", "add", "admin", "after", "aggregate", "alias", "all", "allocate",
    "alter", "and", "any", "are", "array", "as", "asc", "asensitive", "assertion", "asymmetric", "at", "atomic",
    "authorization", "avg", "backup", "before", "begin", "between", "binary", "bit", "bit_length", "blob", "boolean",
    "both", "breadth", "break", "browse", "bulk", "by", "call", "called", "cardinality", "cascade", "cascaded", "case",
    "cast", "catalog", "char", "char_length", "character", "character_length", "check", "checkpoint", "class", "clob",
    "close", "clustered", "coalesce", "collate", "collation", "collect", "column", "commit", "completion", "compute",
    "condition", "connect", "connection", "constraint", "constraints", "constructor", "contains", "containstable",
    "continue", "convert", "corr", "corresponding", "count", "covar_pop", "covar_samp", "create", "cross", "cube",
    "cume_dist", "current", "current_catalog", "current_date", "current_default_transform_group", "current_path",
    "current_role", "current_schema", "current_time", "current_timestamp", "current_transform_group_for_type",
    "current_user", "cursor", "cycle", "data", "database", "date", "day", "dbcc", "deallocate", "dec", "decimal",
    "declare", "default", "deferrable", "deferred", "delete", "deny", "depth", "deref", "desc", "describe", "descriptor",
    "destroy", "destructor", "deterministic", "diagnostics", "dictionary", "disconnect", "disk", "distinct",
    "distributed", "domain", "double", "drop", "dump", "dynamic", "each", "element", "else", "end", "end-exec", "equals",
    "errlvl", "escape", "every", "except", "exception", "exec", "execute", "exists", "exit", "external", "extract",
    "false", "fetch", "file", "fillfactor", "filter", "first", "float", "for", "foreign", "fortran", "found", "free",
    "freetext", "freetexttable", "from", "full", "fulltexttable", "function", "fusion", "general", "get", "global", "go",
    "goto", "grant", "group", "grouping", "having", "hold", "holdlock", "host", "hour", "identity", "identity_insert",
    "identitycol", "if", "ignore", "immediate", "in", "include", "index", "indicator", "initialize", "initially", "inner",
    "inout", "input", "insensitive", "insert", "int", "integer", "intersect", "intersection", "interval", "into", "is",
    "isolation", "iterate", "join", "key", "kill", "language", "large", "last", "lateral", "leading", "left", "less",
    "level", "like", "like_regex", "limit", "lineno", "ln", "load", "local", "localtime", "localtimestamp", "locator",
    "lower", "map", "match", "max", "member", "merge", "method", "min", "minute", "mod", "modifies", "modify", "module",
    "month", "multiset", "names", "national", "natural", "nchar", "nclob", "new", "next", "no", "nocheck", "nonclustered",
    "none", "normalize", "not", "null", "nullif", "numeric", "object", "occurrences_regex", "octet_length", "of", "off",
    "offsets", "old", "on", "only", "open", "opendatasource", "openquery", "openrowset", "openxml", "operation", "option",
    "or", "order", "ordinality", "out", "outer", "output", "over", "overlaps", "overlay", "pad", "parameter", "parameters",
    "partial", "partition", "pascal", "path", "percent", "percent_rank", "percentile_cont", "percentile_disc", "pivot",
    "plan", "position", "position_regex", "postfix", "precision", "prefix", "preorder", "prepare", "preserve", "primary",
    "print", "prior", "privileges", "proc", "procedure", "public", "raiserror", "range", "read", "reads", "readtext", "real",
    "reconfigure", "recursive", "ref", "references", "referencing", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept",
    "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy", "relative", "release", "replication", "restore", "restrict",
    "result", "return", "returns", "revert", "revoke", "right", "role", "rollback", "rollup", "routine", "row", "rowcount",
    "rowguidcol", "rows", "rule", "save", "savepoint", "schema", "scope", "scroll", "search", "second", "section", "securityaudit",
    "select", "semantickeyphrasetable", "semanticsimilaritydetailstable", "semanticsimilaritytable", "sensitive", "sequence",
    "session", "session_user", "set", "sets", "setuser", "shutdown", "similar", "size", "smallint", "some", "space", "specific",
    "specifictype", "sql", "sqlca", "sqlcode", "sqlerror", "sqlexception", "sqlstate", "sqlwarning", "start", "state", "statement",
    "static", "statistics", "stddev_pop", "stddev_samp", "structure", "submultiset", "substring", "substring_regex", "sum",
    "symmetric", "system", "system_user", "table", "tablesample", "temporary", "terminate", "textsize", "than", "then", "time",
    "timestamp", "timezone_hour", "timezone_minute", "to", "top", "trailing", "tran", "transaction", "translate", "translate_regex",
    "translation", "treat", "trigger", "trim", "true", "truncate", "try_convert", "tsequal", "uescape", "under", "union", "unique",
    "unknown", "unnest", "unpivot", "update", "updatetext", "upper", "usage", "use", "user", "using", "value", "values", "var_pop",
    "var_samp", "varchar", "variable", "varying", "view", "waitfor", "when", "whenever", "where", "while", "width_bucket",
    "window", "with", "within", "without", "work", "write", "writetext", "xmlagg", "xmlattributes", "xmlbinary", "xmlcast",
    "xmlcomment", "xmlconcat", "xmldocument", "xmlelement", "xmlexists", "xmlforest", "xmliterate", "xmlnamespaces", "xmlparse",
    "xmlpi", "xmlquery", "xmlserialize", "xmltable", "xmltext", "xmlvalidate", "year", "zone")

  case class ETLConfig(key: String, value: String, `type`: String)
  case class ETLExpression(id: Int, name: String, desc: String, expression: String, alias: String, cache: String, datasource: String)
  case class JsonClass(etlConfig: Seq[ETLConfig], etlExpression: Seq[ETLExpression])

  implicit val etlConfigWrites = new Writes[ETLConfig] {
    def writes(etlConfig: ETLConfig) = Json.obj(
      "key" -> etlConfig.key,
      "value" -> etlConfig.value,
      "type" -> etlConfig.`type`
    )
  }

  implicit val etlExpressionWrites = new Writes[ETLExpression] {
    def writes(etlExpression: ETLExpression) = Json.obj(
      "id" -> etlExpression.id,
      "name" -> etlExpression.name,
      "desc" -> etlExpression.desc,
      "expression" -> etlExpression.expression,
      "alias" -> etlExpression.alias,
      "cache" -> etlExpression.cache,
      "datasource" -> etlExpression.datasource
    )
  }

  implicit val jsonClassWrites = new Writes[JsonClass] {
    def writes(jsonClass: JsonClass) = Json.obj(
      "etl.config" -> jsonClass.etlConfig,
      "etl.expressions" -> jsonClass.etlExpression
    )
  }

}
