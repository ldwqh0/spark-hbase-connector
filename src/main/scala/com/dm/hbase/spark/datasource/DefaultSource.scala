package com.dm.hbase.spark.datasource

import java.util

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

//import org.apache.spark.sql.sources.v2.reader.DataSourceReader
//import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

/**
 * Hbase spark 连接实现
 */
class DefaultSource extends TableProvider   {
//  override def createReader(options: DataSourceOptions): DataSourceReader = {
//    HbaseDataSourceReader(options)
//  }
  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = ???

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table = ???
}
