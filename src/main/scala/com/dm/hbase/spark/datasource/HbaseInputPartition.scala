package com.dm.hbase.spark.datasource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

case class HbaseInputPartition(options: Map[String, String],
                               name: String,
                               rowkey: String,
                               columns: Map[String, HBaseTableColumn],
                               requiredSchema: StructType,
                               filters: Array[Filter],
                               regions: Array[Array[Byte]]
                              ) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    HbaseInputPartitionReader(options, name, rowkey, columns, requiredSchema, filters, regions)
  }
}
