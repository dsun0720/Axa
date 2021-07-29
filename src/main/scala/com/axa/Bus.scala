package com.axa

case class Bus(
  busID: String,
  BusNum: String,
  status: String,
  lineID: Option[String],
  lineName: Option[String],
  direction: Option[String],
  destination: Option[String],
  lat: Double,
  lng: Double,
  onSchedule: Option[String]
)

