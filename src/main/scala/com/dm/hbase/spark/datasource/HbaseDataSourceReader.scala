package com.dm.hbase.spark.datasource

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.{StructType, _}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class HbaseDataSourceReader private(options: DataSourceOptions,
                                         name: String,
                                         rowkey: String,
                                         columns: Map[String, HBaseTableColumn],
                                         structType: StructType
                                        ) extends DataSourceReader
  with SupportsPushDownRequiredColumns with SupportsPushDownFilters {

  /**
   * 注意，如果使用SupportsPushDownRequiredColumns 将列推到下游，必须保证readSchema和pruneColumns方法读取到的元数据信息是一致的
   * 在运行过程中，spark会首先调用readSchema获取完整的列元数据信息，然后根据需要，调用pruneColumns将实际使用的列信息返回给DataSourceReader
   * 并且还会再次调用readSchema获取裁剪之后的列，必须保证返回的数据列等于裁剪之后的列信息
   *
   * 解决方法，在类初始化时，将数据列结构信息初始化为定义的列信息，后期会被更改
   */
  var requiredSchema: StructType = structType

  val pushedFilterBuffer: ArrayBuffer[Filter] = ArrayBuffer()

  override def readSchema(): StructType = {
    requiredSchema
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    Seq[InputPartition[InternalRow]](HbaseInputPartition(Map(
      HConstants.ZOOKEEPER_QUORUM -> options.get(HConstants.ZOOKEEPER_QUORUM).orElse("localhost"),
      HConstants.ZOOKEEPER_CLIENT_PORT -> options.get(HConstants.ZOOKEEPER_CLIENT_PORT).orElse("2181")
    ), name, rowkey, columns, requiredSchema, pushedFilters())).asJava
  }


  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
  }

  /**
   * 进行算子下推
   *
   * 发信跟时间相关的逻辑都不会下推
   *
   * @param filters
   * @return
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    var unsupportedFilters: ArrayBuffer[Filter] = ArrayBuffer()
    // 算子下推,将大于，小于等于等算子下推到hbase查询逻辑
    filters.foreach {
      //      case f:StringContains=>
      case f: StringStartsWith => pushedFilterBuffer += f
      case f: StringContains => pushedFilterBuffer += f
      case f: In => pushedFilterBuffer += f
      case f: IsNull => pushedFilterBuffer += f
      case f: IsNotNull => pushedFilterBuffer += f
      case f: EqualTo => pushedFilterBuffer += f
      case f: LessThan => pushedFilterBuffer += f
      case f: LessThanOrEqual => pushedFilterBuffer += f
      case f: GreaterThan => pushedFilterBuffer += f
      case f: GreaterThanOrEqual => pushedFilterBuffer += f
      case f: And => pushedFilterBuffer += f
      case f: Or => pushedFilterBuffer += f
      case f@_ => unsupportedFilters += f
    }
    unsupportedFilters.toArray
  }

  /**
   * 能够下推的算子
   *
   * @return
   */
  override def pushedFilters(): Array[Filter] = {
    pushedFilterBuffer.toArray
  }
}

object HbaseDataSourceReader {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  /**
   * 构造一个 HbaseDataSourceReader实例
   *
   * @param options
   * @return
   */
  def apply(options: DataSourceOptions): HbaseDataSourceReader = {
    // 读取数据的结构定义
    val tableCatalog = mapper.readValue[HbaseTable](options.get(HbaseTable.catalog).get())
    val fields: Array[StructField] = tableCatalog.columns.map(columnEntry => {
      val (name, column) = columnEntry
      val nullable = !HbaseTable.rowkey.equals(column.columnFamily)
      val metaBuilder = new MetadataBuilder()
      metaBuilder.putString("columnFamily", column.columnFamily).putString("column", column.column)
      StructField(name, getDataType(column.dataType), nullable, metaBuilder.build)
    }).toArray
    HbaseDataSourceReader(
      options,
      s"${tableCatalog.tableName.namespace}:${tableCatalog.tableName.name}",
      tableCatalog.rowkey,
      tableCatalog.columns,
      StructType(fields)
    )
  }

  /**
   * 获取对应的spark数据类型
   *
   * @param ts
   * @return
   */
  private def getDataType(ts: String): DataType = ts match {
    case "boolean" => BooleanType
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
    case "short" => ShortType
    case "timestamp" => TimestampType
    case _ => throw new RuntimeException(s"Unsupported type $ts")
  }
}