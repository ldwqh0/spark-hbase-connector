package com.dm.hbase.spark3.datasource

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

case class HbaseTable(name: String,
                      rowkey: String,
                      columns: Map[String, HBaseTableColumn],
                      schema: StructType,
                      override val partitioning: Array[Transform],
                      override val properties: util.Map[String, String]
                     ) extends Table with SupportsRead {
  // 表示表支持的操作
  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ)
    .asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    HbaseScanBuilder(name, rowkey, columns, schema, partitioning, properties)
  }
}


