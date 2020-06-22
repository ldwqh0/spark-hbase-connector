package com.dm.hbase.spark.datasource

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

class HbaseScan extends Scan with Batch {
  override def readSchema(): StructType = ???

  // 制定分区读取计划
  override def planInputPartitions(): Array[InputPartition] = ???

  override def createReaderFactory(): PartitionReaderFactory = ???
}
