package com.dm.hbase.spark.datasource

import java.sql.{Date, Timestamp}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.{NullComparator, Filter => _, _}
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
        val columnFamily: String = field.metadata.getString(HbaseTable.columnFamily)
        // 如果列所在的列族是rowkey
        if (HbaseTable.rowkey.equals(columnFamily)) {
          // 返回rowkey
          func(current.getRow)
        } else {
          val column: String = field.metadata.getString(HbaseTable.column)
          val bytes = current.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column))
          if (bytes == null) null else func(bytes)
        }
      }

      field.dataType match {
        // case ArrayType => null;
        case DataTypes.BinaryType => readValue(v => v)
        case DataTypes.BooleanType => readValue(Bytes.toBoolean)
        case DataTypes.ByteType => readValue(_ (0)) //待测试
        // ase DataTypes.CalendarIntervalType => bytes; //TODO 这种类型不明白
        // 从毫秒数获取日期
        case DataTypes.DateType => readValue(v => DateTimeUtils.fromJavaDate(new Date(Bytes.toLong(v))))
        case DataTypes.ShortType => readValue(Bytes.toShort)
        case DataTypes.IntegerType => readValue(Bytes.toInt)
        case DataTypes.LongType => readValue(Bytes.toLong)
        case DataTypes.NullType => null //TODO 这个待验证
        case DataTypes.FloatType => readValue(Bytes.toFloat)
        case DataTypes.DoubleType => readValue(Bytes.toDouble)
        case DataTypes.StringType => readValue(UTF8String.fromBytes)
        // 从毫秒数获取时间
        case DataTypes.TimestampType => readValue(v => DateTimeUtils.fromMillis(Bytes.toLong(v))) //日期怎么处理
      }
    }): _*)
  }

  override def close(): Unit = {
    connection.close()
  }

  // 计算过滤器
  private def getScanner() = {
    val scan: Scan = new Scan()
    val filter = new FilterList(filters.map(buildFilter(_)).filter(i => i != null && !i.isInstanceOf[Unit]).toList.asJava)
    scan.setFilter(filter)
    connection.getTable(TableName.valueOf(name)).getScanner(scan)
  }

  private def buildFilter(sparkFilter: Filter): org.apache.hadoop.hbase.filter.Filter = {
    // 将字面值转换为对应的Hbase字节数组
    def toBytes(value: Any): Array[Byte] = value match {
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
     * 构建用值做比较的filter
     *
     * @param attribute
     * @param value
     * @param operator
     * @return
     */
    def buildValueComparatorFilter(attribute: String, operator: CompareOperator, value: Any): org.apache.hadoop.hbase.filter.Filter = {
      builderByteArrayFilter(attribute, operator, new BinaryComparator(toBytes(value)))
    }

    // 构建filter
    def builderByteArrayFilter(attribute: String, operator: CompareOperator, comparator: ByteArrayComparable): org.apache.hadoop.hbase.filter.Filter = {
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
          val filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(column), operator, comparator)
          // 如果没有找到指定的列，不返回该行
          filter.setFilterIfMissing(true)
          filter
        }) match {
        case Some(v: SingleColumnValueFilter) => v
        case _ => throw new RuntimeException("error")
      }
    }

    sparkFilter match {
      case EqualTo(attribute, value) => buildValueComparatorFilter(attribute, CompareOperator.EQUAL, value)
      // column like '%value'
      case StringStartsWith(attribute, value) => builderByteArrayFilter(attribute, CompareOperator.EQUAL, new BinaryPrefixComparator(toBytes(value)))
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
}

object HbaseInputPartitionReader {
  def apply(options: Map[String, String],
            name: String,
            rowkey: String,
            columns: Map[String, HBaseTableColumn],
            requiredSchema: StructType,
            filters: Array[Filter]): HbaseInputPartitionReader = {
    val configuration = HBaseConfiguration.create
    val zookeeperQuorum = options(HConstants.ZOOKEEPER_QUORUM)
    val zookeeperClientPort = options(HConstants.ZOOKEEPER_CLIENT_PORT)
    if (StringUtils.isNotEmpty(zookeeperQuorum)) {
      configuration.set(HConstants.ZOOKEEPER_QUORUM, options(HConstants.ZOOKEEPER_QUORUM))
    }
    if (StringUtils.isNotEmpty(zookeeperClientPort)) {
      configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, options(HConstants.ZOOKEEPER_CLIENT_PORT))
    }
    val cnn = ConnectionFactory.createConnection(configuration);
    HbaseInputPartitionReader(cnn, name, rowkey, columns, requiredSchema, filters)
  }
}