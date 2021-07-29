package com.axa

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

trait BusETL extends Extractor[Bus] with SourceReader {

  def mostTravellingBus(ds: Dataset[Bus]): DataFrame = {
    ds.filter(_.status == "En ligne").toDF()
      .groupBy(col("lineID"), col("lineName"))
      .agg(count("*").as("numOfBus"))
      .orderBy(desc("numOfBus"))
      .limit(1)
  }

  def destinationsContainsSaint(ds: Dataset[Bus]): DataFrame = {
    ds.filter(bus => bus.destination.getOrElse("").contains("Saint"))
      .select(col("destination"))
      .distinct()
  }

  def nearestBusFromEffelTower(ds: Dataset[Bus]): DataFrame = {
    val distCol = pow((pow(col("lat") - lit(48.858093), lit(2)) + pow(col("lng") - lit(2.294694), lit(2))), lit(0.5))
    ds.withColumn("distance", distCol)
      .orderBy(desc("distance"))
      .limit(1)
  }

  override def builder(row: Row): Seq[Bus] = {
    val busID = row.getAs[String]("busID")
    val busNum = row.getAs[String]("busNum")
    val status = row.getAs[String]("status")
    val lineID = handleNullValue(row.getAs[String]("lineID"))
    val lineName = handleNullValue(row.getAs[String]("lineName"))
    val direction = handleNullValue(row.getAs[String]("direction"))
    val destination = handleNullValue(row.getAs[String]("destination"))
    val onSchedule = handleNullValue(row.getAs[String]("onSchedule"))
    val latlng = row.getAs[String]("coordinate").split(",")
    val lat = latlng(0).toDouble
    val lng = latlng(1).toDouble
    Seq(Bus(busID, busNum, status, lineID, lineName, direction, destination, lat, lng, onSchedule))
  }

  private def handleNullValue[T](value: T): Option[T] = {
    if (value == null) None
    else Some(value)
  }
}
