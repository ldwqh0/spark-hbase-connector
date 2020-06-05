import java.time.format.DateTimeFormatter
import java.time.{Instant, ZonedDateTime}
import java.util.Date

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

import scala.collection.JavaConverters._

object TestDataWriter {
  def main(args: Array[String]): Unit = {
    val configuration = HBaseConfiguration.create
    configuration.set(HConstants.ZOOKEEPER_QUORUM, "dm105,dm106,dm107")
    val connection = ConnectionFactory.createConnection(configuration);
    val table: Table = connection.getTable(TableName.valueOf("lidong:test"))
    //    writeData(table)
    readData(table)
    connection.close()
  }

  def readData(table: Table): Unit = {
    val scan = new Scan()
    val iterator = table.getScanner(scan).iterator()
    scan.setLimit(10)
    val cf = Bytes.toBytes("columns")
    while (iterator.hasNext) {
      val rs = iterator.next()

      def getValue(name: String): Array[Byte] = {
        rs.getValue(cf, Bytes.toBytes(name))
      }

      val key = Bytes.toLong(rs.getRow)
      val name = Bytes.toString(getValue("name"))
      val alive = Bytes.toBoolean(getValue("alive"))
      val age = Bytes.toInt(getValue("age"))
      val birthDateStr = Bytes.toString(getValue("birthDateStr"))
      val birthDateTime = Bytes.toLong(getValue("birthDateTime")) // ZonedDateTime.ofInstant(Instant.ofEpochMilli(Bytes.toLong(getValue("birthDateTime"))), ZoneId.systemDefault)
      val birthDate = Date.from(Instant.ofEpochMilli(Bytes.toLong(getValue("birthDate"))))
      val height = Bytes.toDouble(getValue("height"))
      val b = getValue("b")(0)
      println(s"key:$key,name:$name,alive:$alive,age:$age,birthDateStr:$birthDateStr,birthDateTime:$birthDateTime,birthDate:$birthDate,height:$height,b:$b")
    }
  }

  def writeData(table: Table): Unit = {
    val cf = Bytes.toBytes("columns")
    val timestamp: ZonedDateTime = ZonedDateTime.now.plusDays(-1000).withHour(0).withMinute(0).withSecond(0).withNano(0)
      .plusSeconds(-1)
    val range = 1L to 1000L
    val list = range.map(i => {
      val birthDatetime = timestamp.plusDays(i) //.plusSeconds(-1)
      val dateStamp = birthDatetime.withHour(0).withMinute(0).withSecond(0).withNano(0).toInstant.toEpochMilli
      new Put(Bytes.toBytes(i))
        .addColumn(cf, Bytes.toBytes("name"), Bytes.toBytes(s"people$i"))
        .addColumn(cf, Bytes.toBytes("alive"), Bytes.toBytes(if (i % 2 == 0) true else false))
        .addColumn(cf, Bytes.toBytes("age"), Bytes.toBytes((i % 60).asInstanceOf[Int]))
        .addColumn(cf, Bytes.toBytes("birthDateStr"), Bytes.toBytes(birthDatetime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
        // 记录1970-01-01 00:00:00.000到当前时间相差的毫秒数 ，精确到毫秒数
        .addColumn(cf, Bytes.toBytes("birthDateTime"), Bytes.toBytes(birthDatetime.toInstant.toEpochMilli))
        // 精确到天的毫秒数
        .addColumn(cf, Bytes.toBytes("birthDate"), Bytes.toBytes(dateStamp))
        .addColumn(cf, Bytes.toBytes("height"), Bytes.toBytes(Math.random() * 10))
        .addColumn(cf, Bytes.toBytes("b"), Array(5.asInstanceOf[Byte]))
    }).asJava
    table.put(list)
  }
}
