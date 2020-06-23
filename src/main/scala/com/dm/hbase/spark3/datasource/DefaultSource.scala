package com.dm.hbase.spark3.datasource

import java.util

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Hbase spark 连接实现
 */
class DefaultSource extends TableProvider {
  /**
   * 推断元数据信息
   *
   * @param options 配置参数
   * @return 数据结构
   */
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val tableCatalog = HbaseTableCatalog(options.get(HbaseTableCatalog.CATALOG))
    val fields: Array[StructField] = tableCatalog.columns.map(columnEntry => {
      val (name, column) = columnEntry
      val nullable = !HbaseTableCatalog.ROWKEY.equals(column.columnFamily)
      val metaBuilder = new MetadataBuilder()
        .putString("columnFamily", column.columnFamily)
        .putString("column", column.column)
      StructField(name, getDataType(column.dataType), nullable, metaBuilder.build)
    }).toArray
    StructType(fields)
  }


  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val tableCatalog = HbaseTableCatalog(properties.get(HbaseTableCatalog.CATALOG))
    HbaseTable(tableCatalog.name, tableCatalog.rowkey, tableCatalog.columns, schema, partitioning, properties)
  }

  /**
   * 获取对应的spark数据类型
   *
   * @param typeString 数据类型的字符串字面量
   * @return
   */
  private def getDataType(typeString: String): DataType = typeString match {
    case "boolean" => BooleanType
    case "char" => StringType
    case "varchar" => StringType
    case "string" => StringType
    case "int" => IntegerType
    case "integer" => IntegerType
    case "binary" => BinaryType
    case "byte" => ByteType
    case "calendar" => CalendarIntervalType
    case "date" => DateType
    //    case "" => DecimalType
    case "double" => DoubleType
    case "float" => FloatType
    case "long" => LongType
    case "bigint" => LongType
    case "short" => ShortType
    case "timestamp" => TimestampType
    case _ => throw new RuntimeException(s"Unsupported type $typeString")
  }
}

