package com.axa

class BusSuite extends SharedContext {
  object MockBusETL extends BusETL with Serializable

  "mostTravellingBus" should "return the line have the most bus travelling" in {
    val ctx = sqlContext
    import ctx.implicits._
    val expectedRes = Seq(
      ("0001", "C1", 15L)
    ).toDF("lineID", "lineName", "numOfBus")
    val res = MockBusETL.extract(sqlContext, "./position-des-bus-en-circulation-sur-le-reseau-star-en-temps-reel.csv")
      .transform(MockBusETL.mostTravellingBus)
    assertDFs(expectedRes, res)
  }

  "destinationsContainsSaint" should "get the the destinations have \"Saint\" in the name" in {
    val ctx = sqlContext
    import ctx.implicits._
    val expectedRes = Seq(
      ("Saint-Sulpice-la-Forêt"),
      ("Noyal |  Saint-Erblon"),
      ("ZA Saint-Sulpice"),
      ("Saint-Laurent"),
      ("Noyal-Châtillon | Saint-Erblon"),
      ("Pacé | Saint-Gilles"),
      ("Saint-Grégoire | Betton"),
      ("Saint-Jacques"),
      ("Saint-Grégoire")
    ).toDF("destination")
    val res = MockBusETL.extract(sqlContext, "./position-des-bus-en-circulation-sur-le-reseau-star-en-temps-reel.csv")
      .transform(MockBusETL.destinationsContainsSaint)
    assertDFs(expectedRes, res)
  }

  "nearestBusFromEffelTower" should "the nearest bus from the eiffel tower based on provided coordinates" in {
    val ctx = sqlContext
    import ctx.implicits._
    val expectedRes = Seq(
      ("400528164", "400528164", "Haut-le-pied", "0055", "55", "1", "Rennes", 48.071519D, -1.932587D, Option.empty[String], 4.299837591169809D)
    ).toDF("busID", "BusNum", "status", "lineID", "lineName", "direction", "destination", "lat", "lng", "onSchedule", "distance")
    val res = MockBusETL.extract(sqlContext, "./position-des-bus-en-circulation-sur-le-reseau-star-en-temps-reel.csv")
      .transform(MockBusETL.nearestBusFromEffelTower)
    assertDFs(expectedRes, res)
  }

}
