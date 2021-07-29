package com.axa

import org.apache.spark.sql.{Dataset, Encoder, Row, SQLContext}

trait Extractor[T] {
  self: SourceReader =>

  def builder(row: Row): Seq[T]

  def extract(sqlContext: SQLContext, dataSource: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    self.read(sqlContext, dataSource).flatMap(builder).distinct()
  }

}
