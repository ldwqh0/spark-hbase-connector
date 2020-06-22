package com.dm.hbase.spark.datasource

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

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
 *
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

/**
 * 构建一个Hbase column列映射对象
 *
 * @param columnFamily 列族
 * @param column       列
 * @param dataType     数据类型
 */
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
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def apply(str: String): HbaseTable = {
    val catalog: Map[String, Any] = mapper.readValue[Map[String, Any]](str)
    val tableName = catalog("table") match {
      case tableName: String => HbaseTableName(tableName)
      case table: Map[String, String] => HbaseTableName(table("namespace"), table("name"))
      case _ => throw new RuntimeException("error parse tableName from tableName")
    }
    val columns: Map[String, HBaseTableColumn] = catalog("columns") match {
      case columns: List[String] => {
        val columnTuple = columns.map(column => {
          val defs = column.split(" ")
          defs match {
            // "chinese_name string columns col", name type columnFamily col
            case Array(name: String, `type`: String, columnFamily: String, col: String) => (name, HBaseTableColumn(columnFamily, col, `type`))
            // "chinese_name string columns", name type columnFamily
            case Array(name: String, `type`: String, columnFamily: String) => (name, HBaseTableColumn(columnFamily, name, `type`))
          }
        })
        Map(columnTuple: _*)
      }
      case columns: Map[String, Map[String, String]] => columns.mapValues(column => HBaseTableColumn(column("cf"), column("col"), column("type")))
        // mapValues产生的对象无法被序列化，后期任务切分时会报告序列化错误，通过生成一个新对象的方式解决
        .map(v => v)
      case _ => throw new RuntimeException("error parse columns from catalog")
    }
    HbaseTable(tableName, catalog("rowkey").asInstanceOf[String], columns)
  }
}
