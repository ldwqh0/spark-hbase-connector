package com.dm.hbase.spark3.datasource

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

case class HbasePartitionReaderFactory(name: String,
                                       rowkey: String,
                                       columns: Map[String, HBaseTableColumn],
                                       schema: StructType,
                                       partitioning: Array[Transform],
                                       properties: util.Map[String, String],
                                       filters: Array[Filter]
                                      ) extends PartitionReaderFactory {
  //读取分区数据的读取器
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    HbasePartitionReader(name, rowkey, columns, schema, partitioning, properties, filters, partition)
  }
}
