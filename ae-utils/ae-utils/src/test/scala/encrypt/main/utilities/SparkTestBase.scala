// Copyright (C) HomeAway, Inc.

package com.homeaway.analytics.clickstream.utils

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

import scala.sys.process._
import scala.util.Try


trait SparkTestBase extends BeforeAndAfterAll {
  this: Suite =>
  var sparkSession: SparkSession = null

  def builderFunc(builder: SparkSession.Builder): SparkSession ={
    builder.getOrCreate()
  }

  private def getDerbyHome(): String ={
    new File(getClass.getClassLoader.getResource("").getPath).getParent
  }

  private def getMetastoreDir(): String ={
    new File(this.getDerbyHome(), "metastore_db").getAbsolutePath
  }

  private def deleteMetastoreDir(failIfError: Boolean): Unit={
    val metastoreDir = this.getMetastoreDir()
    println(s"metastoreDir: ${metastoreDir}")

    Try(s"rm -r ${metastoreDir}".!!) match{
      case scala.util.Success(str) => println(s"Successful deletion: ${str}")
      case scala.util.Failure(ex) =>
        println(s"Error while deleting directory: ${metastoreDir}; Exception: ${ex}")
    }
  }

  override def beforeAll: Unit ={
    super.beforeAll()

    // By default the current working directory is used for the derby metastore directory
    // This causes conflicts if multiple projects each run spark tests at the same time (the default behavior of sbt)
    // Thus move the derby metastore directory to a unique path per project
    this.deleteMetastoreDir(failIfError = false)
    val derbyHome = this.getDerbyHome()
    System.setProperty("derby.system.home", derbyHome)

    val conf = new SparkConf()
      .set("spark.driver.host", "localhost")
      .set("hive.root.logger", "warn")

    val builder = SparkSession.builder()
      .appName("SparkTestBase")
      .master("local[1]")
      .config(conf)
      .enableHiveSupport()

    sparkSession = this.builderFunc(builder)
  }

  override def afterAll(): Unit ={
    sparkSession.stop()

    this.deleteMetastoreDir(failIfError = true)
    super.afterAll()
  }
}
