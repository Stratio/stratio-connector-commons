package com.stratio.connector.commons

import com.stratio.crossdata.common.data.{Cell, Row}
import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.sql.{Row => SparkSQLRow, SchemaRDD}


class CrossdataConverters$Test extends FlatSpec
with Matchers{
  trait WithRowList {
    val list = List(1, 3, 5, 6)
    val xdRow = new Row ("List", new Cell(list))
    val sparkSqlRow = SparkSQLRow(list)
  }

  trait WithCell extends WithRowList{
    val cell = new Cell(new Cell(list), new Cell(list))
    val sparkCell = SparkSQLRow(list)
  }

  behavior of "A CrossdataConverter"

  it should "convert from XDRow to SparkSQLRow" in new WithRowList {
    CrossdataConverters.toSparkSQLRow(xdRow) should equal (sparkSqlRow)
  }

  it should "convert from Cell to SparkSQLRow" in new WithCell {
    CrossdataConverters.extractCellValue(cell) should equal (sparkCell)
  }
}
