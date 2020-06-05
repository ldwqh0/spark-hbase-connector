package com.dm.hbase.spark.datasource

import java.sql.{Date, Timestamp}

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.{Filter => _, _}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CompareOperator, HBaseConfiguration, HConstants, TableName}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/**
 * 构造一个Hbase读取器
 *
 * @param connection     要读取的数据连接
 * @param name           表名称
 * @param rowkey         rowkey的属性名
 * @param columns        列定义
 * @param requiredSchema 需要读取的列的清单
 * @param filters        需要处理的过滤器
 */
case class HbaseInputPartitionReader(connection: Connection,
                                     name: String,
                                     rowkey: String,
                                     columns: Map[String, HBaseTableColumn],
                                     requiredSchema: StructType,
                                     filters: Array[Filter]
                                    ) extends InputPartitionReader[InternalRow] {

  // 定义一个变量，用于处理处理的结果集
  val scannerIterator = getScanner().iterator()

  override def next(): Boolean = {
    scannerIterator.hasNext
  }

  override def get(): InternalRow = {
    val current: Result = scannerIterator.next()
    InternalRow(requiredSchema.fields.map(field => {
      val bytes: Array[Byte] = readValue(current, field)
      field.dataType match {
        // case ArrayType => null;
        case DataTypes.BinaryType => bytes
        case DataTypes.BooleanType => Bytes.toBoolean(bytes)
        case DataTypes.ByteType => bytes(0)
        // ase DataTypes.CalendarIntervalType => bytes; //TODO 这种类型不明白
        // 从毫秒数获取日期
        case DataTypes.DateType => DateTimeUtils.fromJavaDate(new Date(Bytes.toLong(bytes)))
        case DataTypes.ShortType => Bytes.toShort(bytes)
        case DataTypes.IntegerType => Bytes.toInt(bytes)
        case DataTypes.LongType => Bytes.toLong(bytes)
        case DataTypes.NullType => null //TODO 这个待验证
        case DataTypes.FloatType => Bytes.toFloat(bytes)
        case DataTypes.DoubleType => Bytes.toDouble(bytes)
        case DataTypes.StringType => UTF8String.fromBytes(bytes)
        // 从毫秒数获取时间
        case DataTypes.TimestampType => DateTimeUtils.fromMillis(Bytes.toLong(bytes)) //日期怎么处理
      }
    }): _*)
  }

  override def close(): Unit = {
    connection.close()
  }

  // 计算过滤器
  private def getScanner() = {
    val scan: Scan = new Scan()
    val filter = new FilterList(filters.map(f => buildFilter(f)).filter(i => i != null && !i.isInstanceOf[Unit]).toList.asJava)
    scan.setFilter(filter)
    connection.getTable(TableName.valueOf(name)).getScanner(scan)
  }


  private def buildFilter(sparkFilter: Filter): org.apache.hadoop.hbase.filter.Filter = sparkFilter match {
    case EqualTo(attribute, value) => buildFilter(attribute, value, CompareOperator.EQUAL)
    // column like '%value'
    case StringStartsWith(attribute, value) => {
      if (rowkey.equals(attribute)) {
        new RowFilter(CompareOperator.EQUAL, new BinaryPrefixComparator(toBytes(value)))
      } else {
        val HBaseTableColumn(columnFamily, column, dataType) = columns.get(attribute).get
        new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column), CompareOperator.EQUAL, new BinaryPrefixComparator(toBytes(value)))
      }
    }
    // column like "%str%"
    case StringContains(attribute, value) => {
      if (rowkey.equals(attribute)) {
        new RowFilter(CompareOperator.EQUAL, new SubstringComparator(value))
      } else {
        val HBaseTableColumn(columnFamily, column, dataType) = columns.get(attribute).get
        val filter: SingleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column), CompareOperator.EQUAL, new SubstringComparator(value));
        filter
      }
    }
    // in (v1,v2,v3)
    case In(attribute, values) => {
      if (rowkey.equals(attribute)) {
        val filters: Array[org.apache.hadoop.hbase.filter.Filter] = values.map(v => new RowFilter(CompareOperator.EQUAL, new BinaryComparator(toBytes(v))))
        new FilterList(Operator.MUST_PASS_ONE, filters: _*);
        // 这个语法会报错 new FilterListWithOR(filters.toList.asJava);
      } else {
        val filters = values.map(value => buildSingleColumnValueFilter(attribute, CompareOperator.EQUAL, value))
        new FilterList(Operator.MUST_PASS_ONE, filters: _*)
      }
    }
    // column is null
    case IsNull(attribute) => {
      val HBaseTableColumn(columnFamily, column, dataType) = columns.get(attribute).get
      val filter: SingleColumnValueFilter = new SingleColumnValueFilter(toBytes(columnFamily), toBytes(column), CompareOperator.EQUAL, new NullComparator());
      filter.setFilterIfMissing(false); // 当指定的该列不存在时，会返回
      filter;
    }
    // column is not null
    case IsNotNull(attribute) => {
      val HBaseTableColumn(columnFamily, column, dataType) = columns.get(attribute).get
      val filter: SingleColumnValueFilter = new SingleColumnValueFilter(toBytes(columnFamily), toBytes(column), CompareOperator.NOT_EQUAL, new NullComparator());
      filter.setFilterIfMissing(true);
      filter;
    }
    // column < v
    case LessThan(attribute, value) => buildFilter(attribute, value, CompareOperator.LESS)
    // column <= v
    case LessThanOrEqual(attribute, value) => buildFilter(attribute, value, CompareOperator.LESS_OR_EQUAL)
    // column > v
    case GreaterThan(attribute, value) => buildFilter(attribute, value, CompareOperator.GREATER)
    // column >=
    case GreaterThanOrEqual(attribute, value) => buildFilter(attribute, value, CompareOperator.GREATER_OR_EQUAL)
    // and和or待处理
    case And(left, right) => new FilterList(Operator.MUST_PASS_ALL, buildFilter(left), buildFilter(right))
    case Or(left, right) => new FilterList(Operator.MUST_PASS_ONE, buildFilter(left), buildFilter(right))
  }


  /**
   * 构建过滤器
   *
   * @param attribute
   * @param value
   * @param operator
   * @return
   */
  private def buildFilter(attribute: String, value: Any, operator: CompareOperator): org.apache.hadoop.hbase.filter.Filter = {
    if (rowkey.equals(attribute)) {
      new RowFilter(operator, new BinaryComparator(toBytes(value)))
    } else {
      // 构建值过滤器
      buildSingleColumnValueFilter(attribute, operator, value)
    }
  }

  /**
   * 创建列值过滤器
   *
   * @param attribute 要过滤的列值
   * @param operator  比较操作符
   * @param value     要过滤的值
   * @return
   */
  private def buildSingleColumnValueFilter(attribute: String, operator: CompareOperator, value: Any): org.apache.hadoop.hbase.filter.Filter = {
    val HBaseTableColumn(columnFamily, column, dataType) = columns.get(attribute).get
    val filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column), operator, new BinaryComparator(toBytes(value)))
    // 如果没有找到指定的列，不返回该行
    filter.setFilterIfMissing(true)
    filter.setLatestVersionOnly(true)
    filter
  }

  // 将字面值转换为对应的Hbase字节数组
  private def toBytes(value: Any): Array[Byte] = value match {
    case v: Boolean => Bytes.toBytes(v)
    case v: Byte => Bytes.toBytes(v)
    case v: Short => Bytes.toBytes(v)
    case v: Int => Bytes.toBytes(v)
    case v: Long => Bytes.toBytes(v)
    case v: Float => Bytes.toBytes(v)
    case v: Double => Bytes.toBytes(v)
    case v: String => Bytes.toBytes(v)
    case v: Date => Bytes.toBytes(v.getTime)
    case v: Timestamp => Bytes.toBytes(v.getTime)
    case v@_ => throw new RuntimeException(s"Unsupported value $value")
  }

  /**
   * 读取当前数据
   *
   * @param result
   * @param field
   * @return
   */
  private def readValue(result: Result, field: StructField): Array[Byte] = {
    val columnFamily: String = field.metadata.getString(HbaseTable.columnFamily)
    // 如果列所在的列族是rowkey
    if (HbaseTable.rowkey.equals(columnFamily)) {
      // 返回rowkey
      result.getRow
    } else {
      val column: String = field.metadata.getString(HbaseTable.column)
      result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
    }
  }
}

object HbaseInputPartitionReader {
  def apply(options: Map[String, String],
            name: String,
            rowkey: String,
            columns: Map[String, HBaseTableColumn],
            requiredSchema: StructType,
            filters: Array[Filter]): HbaseInputPartitionReader = {
    val configuration = HBaseConfiguration.create
    configuration.set(HConstants.ZOOKEEPER_QUORUM, options(HConstants.ZOOKEEPER_QUORUM))
    configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, options(HConstants.ZOOKEEPER_CLIENT_PORT))
    val cnn = ConnectionFactory.createConnection(configuration);
    HbaseInputPartitionReader(cnn, name, rowkey, columns, requiredSchema, filters)
  }
}