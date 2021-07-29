package com.axa

import org.apache.spark.sql.SQLContext

object BusMain extends BusETL with Main with Serializable {
  override def appName: String = "Bus"

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Unit = {
    import sqlContext.implicits._
    val dataSource = argsMap.getOrElse("input", "./position-des-bus-en-circulation-sur-le-reseau-star-en-temps-reel.csv")
    val ds = extract(sqlContext, dataSource)
      .cache()
    logger.info("############the line have the most bus travelling")
    ds.transform(mostTravellingBus).show(false)
    logger.info("############the destinations have \"Saint\" in the name")
    ds.transform(destinationsContainsSaint).show(false)
    logger.info("############the nearest bus from the eiffel tower based on provided coordinates")
    ds.transform(nearestBusFromEffelTower).show(false)
  }
}
