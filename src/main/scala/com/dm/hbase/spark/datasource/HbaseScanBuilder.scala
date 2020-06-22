package com.dm.hbase.spark.datasource

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}

class HbaseScanBuilder extends ScanBuilder {
  override def build(): Scan = ???
}
