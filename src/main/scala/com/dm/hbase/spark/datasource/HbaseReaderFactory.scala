package com.dm.hbase.spark.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

class HbaseReaderFactory extends PartitionReaderFactory {
  //读取分区数据的读取器
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = ???
}
