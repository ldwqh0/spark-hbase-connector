import java.sql.Timestamp
import java.time.ZonedDateTime

import com.dm.hbase.spark.datasource.HbaseTableCatalog
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.sql.{Row, SparkSession}

object ScalaDatasourceTest {
  def main(args: Array[String]): Unit = {
    val catalog =
      """{
      "table": {
        "namespace": "lidong",
        "name": "test"
      },
      "rowkey": "id",
      "columns": {
        "id": {
          "cf": "rowkey",
          "col": "id",
          "type": "long"
        },
        "name": {
          "cf": "columns",
          "col": "name",
          "type": "string"
        },
        "alive": {
          "cf": "columns",
          "col": "alive",
          "type": "boolean"
        },
        "age": {
          "cf": "columns",
          "col": "age",
          "type": "int"
        },
        "birthDateStr": {
          "cf": "columns",
          "col": "birthDateStr",
          "type": "string"
        },
        "birthDateTime": {
          "cf": "columns",
          "col": "birthDateTime",
          "type": "timestamp"
        },
        "birthDate": {
          "cf": "columns",
          "col": "birthDate",
          "type": "date"
        },
        "height": {
          "cf": "columns",
          "col": "height",
          "type": "double"
        },
        "b": {
          "cf": "columns",
          "col": "b",
          "type": "byte"
        },
        "c":{
          "cf": "columns",
          "col": "c",
          "type": "string"
        }
      }
    }"""
    val session = SparkSession.builder.appName("Test").master("local[*]").getOrCreate
    // 注册一个ISO字符串时间转换函数，用于将ISO时间转换为Sql时间
    session.udf.register("from_iso_string", (v: String) => Timestamp.from(ZonedDateTime.parse(v).toInstant))
    // 测试时间转换是否正确
    val r = session.sql("select date(from_iso_string('2000-01-01T16:00:00Z')) as a ")
    r.foreach((i: Row) => println(i(0)))

    // 测试hbase连接
    val a = session.sqlContext.read
      .option(HbaseTableCatalog.CATALOG, catalog)
      .option(HConstants.ZOOKEEPER_QUORUM, "dm105,dm106,dm107")
      .format("com.dm.hbase.spark.datasource")
      .load
    a.printSchema()
    a.createOrReplaceTempView("test")
    // 部分spark-sql
    // to_date 将字符串转换为日期格式，会丢失时间信息
    // to_timestamp 将字符串转化为日期时间格式，不会丢失时间信息
    val all = session.sql(
      """select id,name,alive,age,birthDateStr,birthDateTime,birthDate,height,b,c from test
        |where id in (1,2,3,4,5) or name='people1'
        |""".stripMargin)
    // 显示执行计划
    all.explain(true)
    //    all.write.text("d:\\r.txt")
    //    all.write.jdbc()
    //    all.write.json("hdfs://ubuntu3:9000/test/result.json")
    all.foreach((row: Row) => println(
      s"""{
         |  "id":"${row(0)}",
         |  "name":"${row(1)}",
         |  "alive":"${row(2)}",
         |  "age":"${row(3)}",
         |  "birthDateStr":"${row(4)}",
         |  "birthDateTime":"${row(5)}",
         |  "birthDate":"${row(6)}",
         |  "height":"${row(7)}",
         |  "b":"${row(8)}",
         |  "c":":${row(9)}"
         |}""".stripMargin))
    val count = session.sql("""select name from test""")

    //    count.write.text("hdfs://ubuntu3:9000/test/result.txt")
    count.foreach(v => println(v))
  }
}
