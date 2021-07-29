package com.axa

import java.util.{Locale, TimeZone}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}

trait Main {
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("com.axa").setLevel(Level.INFO)

  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  @transient protected final lazy val logger: Logger = Logger.getLogger(getClass)

  private var _spark: SparkSession = _

  protected implicit def spark: SparkSession = _spark

  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .master("local[*]")
        .appName(this.appName)
        .getOrCreate()
    }
  }

  protected def stopSession(): Unit = {
    if (_spark != null) {
      _spark.stop()
      _spark = null
    }
  }

  // Expected args are in format "arg1=value1 arg2=value2 ..."
  def main(args: Array[String]): Unit = {
    initializeSession()
    val argsMap = args.map(
      arg => arg.split("=")(0) -> arg.split("=")(1)
    ).toMap
    try {
      run(sqlContext, argsMap)
    }
    finally stopSession()
  }

  def appName: String

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Unit

}
