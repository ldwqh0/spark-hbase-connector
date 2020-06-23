package com.dm.hbase.spark3.datasource

import org.apache.spark.sql.connector.read.InputPartition

case class HbaseInputPartition(override val preferredLocations: Array[String],
                               startKey: Array[Byte],
                               endKey: Array[Byte]
                              ) extends InputPartition {
}
