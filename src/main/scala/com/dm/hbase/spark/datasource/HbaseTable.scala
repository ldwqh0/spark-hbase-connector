package com.dm.hbase.spark.datasource

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * 定义在一个在spark-sql中，可供解析的table的结构,主要用于解析 Hbase元数据定义文件，一个典型的元数据定义文件如下
 *
 * |{
 * |  "table": {
 * |    "namespace": "guizhou",
 * |    "name": "t_atm_info"
 * |  },
 * |  "rowkey": "key",
 * |  "columns": {
 * |    "key": {
 * |      "cf": "rowkey",
 * |      "col": "key",
 * |      "type": "string"
 * |    },
 * |    "chinese_name": {
 * |      "cf": "columns",
 * |      "col": "chinese_name",
 * |      "type": "string"
 * |    }
 * |  }
 * |}
 *
 * @param tableName 表名称
 * @param rowkey    rowkey所对应的列名称
 * @param columns   列定义
 */
case class HbaseTable(@JsonProperty("table")
                      tableName: HbaseTableName,
                      rowkey: String,
                      columns: Map[String, HBaseTableColumn]) {
  val name: String = s"${tableName.namespace}:${tableName.name}"

  //  def this(name: String, rowkey: String, columns: Map[String, HBaseTableColumn]) {
  //    this(HbaseTableName(name), rowkey, columns)
  //  }
}

case class HbaseTableName(namespace: String = "default",
                          name: String) {
}

object HbaseTableName {
  def apply(name: String): HbaseTableName = {
    name.split(":").reverse match {
      case Array(name) => HbaseTableName("default", name)
      case Array(name, namespace) => HbaseTableName(namespace, name)
    }
  }
}


case class HBaseTableColumn(@JsonProperty("cf")
                            columnFamily: String,
                            @JsonProperty("col")
                            column: String,
                            @JsonProperty("type")
                            dataType: String) {
}

object HbaseTable {
  val catalog: String = "catalog"
  val rowkey: String = "rowkey"
  val columnFamily: String = "columnFamily"
  val column: String = "column"
}
