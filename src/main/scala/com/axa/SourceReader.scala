package com.axa

import org.apache.spark.sql.{DataFrame, SQLContext}

trait SourceReader {

  def read(sqlContext: SQLContext, path: String): DataFrame = {
    sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(path)
  }
}
