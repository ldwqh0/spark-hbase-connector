package com.dm.hbase.spark3.datasource

import java.sql.{Date, Timestamp}
import java.util

import com.dm.hbase.spark3.datasource.HbaseConnectionUtil._
import org.apache.hadoop.hbase.client.{Connection, Result, ResultScanner, Scan}
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.{BinaryComparator, BinaryPrefixComparator, ByteArrayComparable, FilterList, NullComparator, RowFilter, SingleColumnValueFilter, SubstringComparator, Filter => HbaseFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CompareOperator, TableName}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

case class HbasePartitionReader(name: String,
                                rowkey: String,
                                columns: Map[String, HBaseTableColumn],
                                requiredSchema: StructType,
                                partitioning: Array[Transform],
                                properties: util.Map[String, String],
                                filters: Array[Filter],
                                partition: InputPartition
                               ) extends PartitionReader[InternalRow] {
  val connection: Connection = getConnection(properties)
  // 定义一个变量，用于处理处理的结果集
  val scannerIterator = scanner.iterator

  override def next(): Boolean = scannerIterator.hasNext

  // 将字面值转换为对应的Hbase字节数组
  implicit def toBytes(value: Any): Array[Byte] = value match {
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
    case v@_ => throw new RuntimeException(s"Unsupported value $v")
  }

  implicit def toLong(bytes: Array[Byte]): Long = Bytes.toLong(bytes)

  override def get(): InternalRow = {
    val current: Result = scannerIterator.next()
    InternalRow(requiredSchema.fields.map(field => {
      /**
       *
       * 读取值并应用转换
       * 对读取的值要进行的操作
       *
       * @param func 要应用的转换
       * @tparam R
       * @return
       */
      def readValue[R >: Null](func: Array[Byte] => R): R = {
        val columnFamily: String = field.metadata.getString(HbaseTableCatalog.COLUMN_FAMILY)
        // 如果列所在的列族是rowkey
        if (HbaseTableCatalog.ROWKEY.equals(columnFamily)) {
          // 返回rowkey
          func(current.getRow)
        } else {
          val column: String = field.metadata.getString(HbaseTableCatalog.COLUMN)
          val bytes = current.getValue(columnFamily, column)
          if (bytes == null) null else func(bytes)
        }
      }

      field.dataType match {
        // case ArrayType => null;
        case DataTypes.BinaryType => readValue(v => v)
        case DataTypes.BooleanType => readValue(Bytes.toBoolean)
        case DataTypes.ByteType => readValue(v => v(0)) //待测试
        // case DataTypes.CalendarIntervalType => bytes; //TODO 这种类型不明白
        // 从毫秒数获取日期
        case DataTypes.DateType => readValue(v => DateTimeUtils.fromJavaDate(new Date(v)))
        case DataTypes.ShortType => readValue(Bytes.toShort)
        case DataTypes.IntegerType => readValue(Bytes.toInt)
        case DataTypes.LongType => readValue(Bytes.toLong)
        case DataTypes.NullType => null //TODO 这个待验证
        case DataTypes.FloatType => readValue(Bytes.toFloat)
        case DataTypes.DoubleType => readValue(Bytes.toDouble)
        case DataTypes.StringType => readValue(UTF8String.fromBytes)
        // 从毫秒数获取时间
        case DataTypes.TimestampType => readValue(DateTimeUtils.fromMillis(_)) //日期怎么处理
      }
    }): _*)
  }

  // 关闭hbase连接
  override def close(): Unit = connection.close()


  private def buildFilter(sparkFilter: Filter): HbaseFilter = {


    /**
     * 构建用值做比较的filter
     *
     * @param attribute
     * @param value
     * @param operator
     * @return
     */
    def buildValueComparatorFilter(attribute: String, operator: CompareOperator, value: Any): HbaseFilter = {
      builderByteArrayFilter(attribute, operator, new BinaryComparator(value))
    }

    // 构建filter
    def builderByteArrayFilter(attribute: String, operator: CompareOperator, comparator: ByteArrayComparable): HbaseFilter = {
      if (rowkey.equals(attribute)) {
        new RowFilter(operator, comparator)
      } else {
        buildSingleColumnValueFilter(attribute, operator, comparator)
      }
    }

    /**
     * 创建列值过滤器
     *
     * @param attribute 要过滤的列值
     * @param operator  比较操作符
     * @return
     */
    def buildSingleColumnValueFilter(attribute: String, operator: CompareOperator, comparator: ByteArrayComparable): org.apache.hadoop.hbase.filter.SingleColumnValueFilter = {
      columns.get(attribute)
        .map(v => {
          val HBaseTableColumn(columnFamily, column, dataType) = v
          val filter = new SingleColumnValueFilter(columnFamily, column, operator, comparator)
          // 如果没有找到指定的列，不返回该行
          filter.setFilterIfMissing(true)
          filter
        }) match {
        case Some(v: SingleColumnValueFilter) => v
        case _ => throw new RuntimeException("error")
      }
    }

    sparkFilter match {

      // TODO 如何针对rowkey查询进行优化
      case EqualTo(attribute, value) => buildValueComparatorFilter(attribute, CompareOperator.EQUAL, value)
      // column like '%value'
      case StringStartsWith(attribute, value) => builderByteArrayFilter(attribute, CompareOperator.EQUAL, new BinaryPrefixComparator(value))
      // column like "%str%"
      case StringContains(attribute, value) => builderByteArrayFilter(attribute, CompareOperator.EQUAL, new SubstringComparator(value))
      // in (v1,v2,v3)
      case In(attribute, values) => new FilterList(Operator.MUST_PASS_ONE, values.map(buildValueComparatorFilter(attribute, CompareOperator.EQUAL, _)): _*);
      // column is null
      case IsNull(attribute) => {
        val filter = buildSingleColumnValueFilter(attribute, CompareOperator.EQUAL, new NullComparator());
        filter.setFilterIfMissing(false); // 当指定的该列不存在时，会返回
        filter;
      }
      // column is not null
      case IsNotNull(attribute) => buildSingleColumnValueFilter(attribute, CompareOperator.NOT_EQUAL, new NullComparator())
      // column < v
      case LessThan(attribute, value) => buildValueComparatorFilter(attribute, CompareOperator.LESS, value)
      // column <= v
      case LessThanOrEqual(attribute, value) => buildValueComparatorFilter(attribute, CompareOperator.LESS_OR_EQUAL, value)
      // column > v
      case GreaterThan(attribute, value) => buildValueComparatorFilter(attribute, CompareOperator.GREATER, value)
      // column >=
      case GreaterThanOrEqual(attribute, value) => buildValueComparatorFilter(attribute, CompareOperator.GREATER_OR_EQUAL, value)
      // and和or待处理
      case And(left, right) => new FilterList(Operator.MUST_PASS_ALL, buildFilter(left), buildFilter(right))
      case Or(left, right) => new FilterList(Operator.MUST_PASS_ONE, buildFilter(left), buildFilter(right))
    }
  }

  // 计算过滤器
  private def scanner(): ResultScanner = partition match {
    case HbaseInputPartition(host, startKey, endKey) => {
      val scan: Scan = new Scan()
      val filter = new FilterList(filters.map(buildFilter(_)).filter(i => i != null && !i.isInstanceOf[Unit]).toList.asJava)
      scan.setFilter(filter)
      scan.withStartRow(startKey)
      scan.withStopRow(endKey)
      connection.getTable(TableName.valueOf(name)).getScanner(scan)
    }
  }
}
