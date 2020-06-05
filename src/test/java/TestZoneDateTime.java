import java.util.Date;

public class TestZoneDateTime {
    public static void main(String[] args) {
//        System.out.println(ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        System.out.println(new Date().getTime());
        System.out.println(new java.sql.Timestamp(new Date().getTime()));
    }
}
