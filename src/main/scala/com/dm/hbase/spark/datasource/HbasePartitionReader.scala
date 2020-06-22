package com.dm.hbase.spark.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader


class HbasePartitionReader extends PartitionReader[InternalRow] {
  override def next(): Boolean = ???

  override def get(): InternalRow = ???

  override def close(): Unit = ???
}
