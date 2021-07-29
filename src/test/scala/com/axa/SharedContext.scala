package com.axa

import java.util.{Locale, TimeZone}
import org.apache.log4j.{Level, Logger}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.apache.spark.sql._

abstract class SharedContext extends AnyFlatSpec with BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("com.axa").setLevel(Level.INFO)

  Locale.setDefault(Locale.US)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  private var _spark: SparkSession = _

  protected def spark: SparkSession = _spark

  protected def sqlContext: SQLContext = _spark.sqlContext

  protected override def beforeAll(): Unit = {
    if (_spark == null) {
      _spark = SparkSession
        .builder()
        .appName("Tests")
        .master("local[*]")
        .getOrCreate()
    }
    super.beforeAll()
  }

  override def afterAll() {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  def assertDFs(ds1: DataFrame, ds2: DataFrame, debug: Boolean = true) = assertDSs[Row](ds1, ds2, debug)

  def assertDSs[A](ds1: Dataset[A], ds2: Dataset[A], debug: Boolean = true): Unit = {
    val df1 = ds1.toDF
    val df2 = ds2.toDF
    try {

      df1.persist()
      df2.persist()

      if (debug) {
        df1.printSchema()
        df2.printSchema()
        df1.show(100, false)
        df2.show(100, false)
      }
      import RichDataFrame._
      assert(df1 === df2)

    } finally {
      df1.unpersist()
      df2.unpersist()
    }
  }
}
