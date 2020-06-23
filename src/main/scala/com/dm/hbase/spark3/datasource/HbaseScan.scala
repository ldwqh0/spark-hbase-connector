package com.dm.hbase.spark3.datasource

import java.util

import com.dm.hbase.spark3.datasource.HbaseConnectionUtil._
import org.apache.hadoop.hbase.{MetaTableAccessor, TableName}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class HbaseScan(name: String,
                     rowkey: String,
                     columns: Map[String, HBaseTableColumn],
                     requiredSchema: StructType,
                     partitioning: Array[Transform],
                     properties: util.Map[String, String],
                     filters: Array[Filter]
                    ) extends Scan with Batch {
  override def readSchema(): StructType = requiredSchema

  override def toBatch: Batch = this

  // 制定分区读取计划
  override def planInputPartitions(): Array[InputPartition] = {
    def getRegions(): List[(String, Array[Byte], Array[Byte])] = {
      withAdmin(properties) {
        admin => {
          admin.getRegions(TableName.valueOf(name))
            .asScala
            .map(regionInfo => {
              val host: String = MetaTableAccessor.getRegionLocation(admin.getConnection, regionInfo).getHostname
              (host, regionInfo.getStartKey, regionInfo.getEndKey)
            })
            .toList
        }
      }
    }

    getRegions().map(p => p match {
      case (host: String, start: Array[Byte], end: Array[Byte]) => HbaseInputPartition(Array(host), start, end)
    }).toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    HbasePartitionReaderFactory(
      name,
      rowkey,
      columns,
      requiredSchema,
      partitioning,
      properties,
      filters
    )
  }
}
