package com.axa

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

class RichDataFrame(dataFrame: DataFrame) {
  def ===(other: DataFrame): Boolean = {

    def checkDuplicateRows: Boolean = {
      val dataFrameGroupedByRows = dataFrame.groupBy(
        dataFrame.columns.head,
        dataFrame.columns.tail: _*
      ).count()
      val otherGroupedByRows = other.groupBy(
        other.columns.head,
        other.columns.tail: _*
      ).count()

      dataFrameGroupedByRows.except(otherGroupedByRows).count() == 0 &&
        otherGroupedByRows.except(dataFrameGroupedByRows).count == 0
    }

    def columnNameType(schema: StructType): Seq[(String, DataType)] = {
      schema.fields.map((field: StructField) => (field.name, field.dataType))
    }

    columnNameType(dataFrame.schema) == columnNameType(other.schema) &&
      checkDuplicateRows
  }
}

object RichDataFrame {
  implicit def toRichDataFrame(dataFrame: DataFrame): RichDataFrame = new RichDataFrame(dataFrame)
}
