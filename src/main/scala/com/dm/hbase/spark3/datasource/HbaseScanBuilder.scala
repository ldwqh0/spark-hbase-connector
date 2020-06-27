package com.dm.hbase.spark3.datasource

import java.util

import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

case class HbaseScanBuilder(name: String,
                            rowkey: String,
                            columns: Map[String, HBaseTableColumn],
                            schema: StructType,
                            partitioning: Array[Transform],
                            properties: util.Map[String, String]
                           ) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  val pushedFilterBuffer: ArrayBuffer[Filter] = ArrayBuffer()
  var requiredSchema = schema

  // 根据逻辑计划，创建一个扫描器
  override def build(): Scan = HbaseScan(name, rowkey, columns, requiredSchema, partitioning, properties, pushedFilters)

  // 下推算子，返回不能下推的算子
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val unsupportedFilters: ArrayBuffer[Filter] = ArrayBuffer()
    // 算子下推,将大于，小于等于等算子下推到hbase查询逻辑
    filters.foreach {
      //      case f:StringContains=>
      case f: StringStartsWith => pushedFilterBuffer += f
      case f: StringContains => pushedFilterBuffer += f
      case f: In => pushedFilterBuffer += f
      case f: IsNull => pushedFilterBuffer += f
      case f: IsNotNull => pushedFilterBuffer += f
      case f: EqualTo => pushedFilterBuffer += f
      case f: LessThan => pushedFilterBuffer += f
      case f: LessThanOrEqual => pushedFilterBuffer += f
      case f: GreaterThan => pushedFilterBuffer += f
      case f: GreaterThanOrEqual => pushedFilterBuffer += f
      case f: And => pushedFilterBuffer += f
      case f: Or => pushedFilterBuffer += f
      case f@_ => unsupportedFilters += f
    }
    unsupportedFilters.toArray
  }

  // 返回成功下推的算子
  override def pushedFilters(): Array[Filter] = pushedFilterBuffer.toArray

  override def pruneColumns(requiredSchema: StructType): Unit = {
    val fis = requiredSchema.map(requiredField => schema.find(_.name.equals(requiredField.name)).get)
    this.requiredSchema = StructType(fis)
  }
}
