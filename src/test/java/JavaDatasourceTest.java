import com.dm.hbase.spark3.datasource.HbaseTableCatalog;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZonedDateTime;

public class JavaDatasourceTest implements Serializable {
    public static void main(String[] args) throws IOException {
        String file = "D:\\Users\\LiDong\\Projects\\dmzl\\guizhou\\hadoop-guizhou\\spark-hbase\\src\\main\\resources\\catalogs\\t_atm_info.json";
        String catalog = FileUtils.readFileToString(new File(file), "UTF-8");
        SparkSession session = SparkSession.builder().appName("Test").master("local[*]").getOrCreate();
        // 注册一个ISO字符串时间转换函数，用于将ISO时间转换为Sql时间
        session.udf().register("from_iso_string", (String v) -> Timestamp.from(ZonedDateTime.parse(v).toInstant()), DataTypes.TimestampType);

        // 测试时间转换是否正确
        Dataset<Row> r = session.sql("select date(from_iso_string('2000-01-01T16:00:00Z')) as a ");
        r.foreach(i -> {
            System.out.println(i.get(0));
        });

        // 测试hbase连接
        Dataset<Row> a = session.sqlContext()
            .read()
            .option(HbaseTableCatalog.CATALOG(), catalog)
            .option(HConstants.ZOOKEEPER_QUORUM, "dm105,dm106,dm107")
            .format("com.dm.hbase.spark.datasource")
            .load();
        a.printSchema();
        a.registerTempTable("tt");


        Dataset<Row> as = session.sql("select key, chinese_name from tt where key <= '0642C94A8C1146079385B7502F462D2D' order by key desc");
        as.explain(true);
        as.foreach(i -> {
            System.out.println(i.get(0) + "-" + i.get(1));
        });
    }
}



