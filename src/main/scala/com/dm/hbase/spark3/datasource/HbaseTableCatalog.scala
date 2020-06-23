package com.dm.hbase.spark3.datasource

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

case class HbaseTableCatalog(@JsonProperty("table")
                             tableName: HbaseTableName,
                             rowkey: String,
                             columns: Map[String, HBaseTableColumn]) {
  def name(): String = s"${tableName.namespace}:${tableName.name}"
}

case class HbaseTableName(namespace: String = "default",
                          name: String) {
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

object HbaseTableName {
  def apply(name: String): HbaseTableName = {
    name.split(":").reverse match {
      case Array(name) => HbaseTableName(name)
      case Array(name, namespace) => HbaseTableName(namespace, name)
    }
  }
}

object HbaseTableCatalog {
  // 定义一些json常量
  val CATALOG: String = "catalog"
  val ROWKEY: String = "rowkey"
  val COLUMN_FAMILY: String = "columnFamily"
  val COLUMN: String = "column"

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  def apply(str: String): HbaseTableCatalog = {
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
    HbaseTableCatalog(tableName, catalog("rowkey").asInstanceOf[String], columns)
  }
}
