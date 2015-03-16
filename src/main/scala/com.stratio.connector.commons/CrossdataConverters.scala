/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.commons

import com.stratio.crossdata.common.exceptions.UnsupportedException

import scala.language.implicitConversions
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{Row => SparkSQLRow, SchemaRDD}
import com.stratio.crossdata.common.metadata.{TableMetadata, ColumnMetadata, ColumnType}
import com.stratio.crossdata.common.data.{Row => XDRow, Cell, ResultSet}

object CrossdataConverters {

  import scala.collection.JavaConversions._

  type ColumnTypeMap = Map[String, ColumnType]

  /**
   * Compute some SchemaRDD and map it into a Crossdata ResultSet
   * @param schemaRDD Given Schema RDD
   * @param metadata Columns metadata
   * @return An equivalent ResultSet
   */
  def toResultSet(
    schemaRDD: SchemaRDD,
    metadata: List[ColumnMetadata]): ResultSet =
    toResultSet(schemaRDD.toLocalIterator, schemaRDD.schema, metadata)

  /**
   * Compute some SchemaRDD and map it into a Crossdata ResultSet
   * @param rows Iterator of Crossdata rows to be converted
   * @param schema Row structure
   * @param metadata Columns metadata
   * @return A new Resultset with all converted rows
   */
  def toResultSet(
    rows: Iterator[SparkSQLRow],
    schema: StructType,
    metadata: List[ColumnMetadata]): ResultSet = {
    val resultSet = new ResultSet
    resultSet.setColumnMetadata(metadata)
    rows.foreach(row => resultSet.add(toCrossDataRow(row, schema)))
    resultSet
  }

  /**
   * Convert some SparkSQL row into a Crossdata row
   * @param row SparkSQL row to be converted
   * @param schema Row structure
   * @return A new Resultset with all converted rows
   */
  def toCrossDataRow(row: SparkSQLRow, schema: StructType): XDRow = {
    val fields = schema.fields
    val xdRow = new XDRow()
    fields.zipWithIndex.map {
      case (field, idx) => xdRow.addCell(
        field.name,
        new Cell(toCellValue(row(idx), field.dataType)))
    }
    xdRow
  }

  def extractCellValue(value: AnyRef): SparkSQLRow = {
    value match {
      case cell : Cell => extractCellValue(cell.getValue)
      case _ => SparkSQLRow(value)
    }
  }

  def toSparkSQLRow (row: XDRow): SparkSQLRow = {
    new GenericRow(row.getCellList.map { cell => cell.getValue match {
      case value: Cell => extractCellValue(value)
      case _ => cell.getValue
    }
    }.toArray[Any])
  }

  def toStructType(tableMetadata:TableMetadata):StructType ={
    val fields = tableMetadata.getColumns.toMap
    val structType = new StructType(
      fields.map{
        case(columnName, columnMetadata) =>
          new StructField(
            columnName.getName,
            columnMetadata.getColumnType match{
              case ColumnType.BIGINT => LongType
              case ColumnType.BOOLEAN => BooleanType
              case ColumnType.DOUBLE => DoubleType
              case ColumnType.FLOAT => FloatType
              case ColumnType.INT => IntegerType
              case ColumnType.TEXT => StringType
              case ColumnType.VARCHAR => StringType
              //TODO: throw UnsupportedException
              //case _ => UnsupportedException("Type not supported")
            }
          )
      }.toSeq
    )
    structType
  }
  /**
   * Convert some undefined value into Crossdata cell
   * @param value Given value to be converted to cell
   * @param dataType Object data type.
   * @return A new Crossdata cell with converted object.
   */
  def toCellValue(value: Any, dataType: DataType): Any = {
    import scala.collection.JavaConversions._
    Option(value).map { v =>
      (dataType, value) match {
        case (ArrayType(elementType, _), array: ArrayBuffer[Any@unchecked]) =>
          val list: java.util.List[Any] = array.map {
            case obj => toCellValue(obj, elementType)
          }.toList
          list
        case (struct: StructType, value: GenericRow) =>
          val map: java.util.Map[String, Any] = struct.fields.zipWithIndex.map {
            case (field, idx) =>
              field.name -> toCellValue(value(idx), field.dataType)
          }.toMap[String, Any]
          map
        case _ =>
          value
      }
    }.orNull
  }

}
