package com.dm.hbase.spark.datasource

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

/**
 * Hbase spark 连接实现
 */
class DefaultSource extends DataSourceV2 with ReadSupport {
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    HbaseDataSourceReader(options)
  }
}
