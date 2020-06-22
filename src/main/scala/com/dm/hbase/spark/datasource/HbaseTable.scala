package com.dm.hbase.spark.datasource

import java.util

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class HbaseTable extends Table with SupportsRead {
  override def name(): String = ???

  override def schema(): StructType = ???

  // 表示表支持的操作
  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ)
    .asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = ???

}

object HbaseTable {

}
