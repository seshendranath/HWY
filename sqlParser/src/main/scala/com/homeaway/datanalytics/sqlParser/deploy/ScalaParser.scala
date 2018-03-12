package com.homeaway.datanalytics.sqlParser.deploy

import scala.util.parsing.combinator._


/**
  * Created by aguyyala on 6/30/17.
  */
object ScalaParser extends JavaTokenParsers {

  def normalWord: Parser[String] = """[\w.@'\[\]/-]+""".r

  def wordWithSpaces: Parser[String] = """[ \$\w.@\[\]/-]+""".r

  def quotedWordWithSpaces: Parser[String] = "'" ~ wordWithSpaces ~ "'" ^^ {case a ~ b ~ c => a + b + c}

  def word: Parser[String] = quotedWordWithSpaces | normalWord

  def digit: Parser[String] = """\d*""".r

  def any: Parser[String] = """.*""".r

  def operator: Parser[String] = "=" | "==" | "!=" | "<>" | "+" | "-" | "*" | "/" | ">" | "<" | ">=" | "<="

  def tableName: Parser[Any] = (ident | "dual") ~ opt("as" ~ ident)

  def expr: Parser[String] = word ~ (rep(operator ~ (caseExpr | word) ^^ {case a~b => a + " " + b}) ^^ (_ mkString " ")) ^^ { case a ~ b => a + " " + b }

  def caseExpr: Parser[String] = "case" ~ "when" ~ word ~ operator ~ expr ~ "then" ~ expr ~ "else" ~ expr ~ "end" ^^ { case a ~ b ~ c ~ d ~ e ~ f ~ g ~ h ~ i ~ j => constructString(a, b, c, d, e, f, g, h, i, j) }

  def coalesce: Parser[String] = "coalesce" ~ "(" ~ (repsep(word, ",") ^^ (_ mkString ",")) ~ ")" ^^ { case a ~ b ~ c ~ d => constructString(a, b, c, d) }

  def round: Parser[String] = "round" ~ "(" ~ expr ~ "," ~ digit ~ ")" ^^ { case (a ~ b ~ c ~ d ~ e ~ f) => constructString(a, b, c, d, e, f) }

  def projections: Parser[Any] = caseExpr | coalesce | round | expr | word

  def updateClouse(): Parser[String] = ("update" ~ tableName ~ "set" ~> repsep(word ~ "=" ~ projections ^^ {case a ~ "=" ~ b => (a, b)}, ",") ^^ { x => x.asInstanceOf[List[Any]].map{case (a, b) => b + " as " + a}.mkString(",")}) <~ opt(any)

  def constructString[A](a: A*) = {
    a mkString " "
  }
}
