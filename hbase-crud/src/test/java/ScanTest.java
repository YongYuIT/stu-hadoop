import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ScanTest {
    @Test
    public void putTestData() throws Exception {
        List<Put> puts = new ArrayList<Put>();
        Put put_1 = new Put(Bytes.toBytes("usc_stu_1001"));
        put_1.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("Yu Yong"));
        put_1.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"), Bytes.toBytes("M"));
        put_1.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes(28L));
        put_1.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("desc"), Bytes.toBytes("hello word"));
        put_1.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("hubei"));
        puts.add(put_1);

        Put put_2 = new Put(Bytes.toBytes("usc_stu_1002"));
        put_2.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("Ben Ben"));
        put_2.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"), Bytes.toBytes("F"));
        put_2.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes(29L));
        put_2.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("desc"), Bytes.toBytes("test aaa"));
        put_2.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("hubei"));
        puts.add(put_2);

        Put put_3 = new Put(Bytes.toBytes("usc_stu_1003"));
        put_3.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("Box"));
        put_3.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"), Bytes.toBytes("F"));
        put_3.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes(30L));
        put_3.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("desc"), Bytes.toBytes("test bbb"));
        put_3.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("jiangxi"));
        puts.add(put_3);

        Put put_4 = new Put(Bytes.toBytes("usc_stu_1004"));
        put_4.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("Joy"));
        put_4.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"), Bytes.toBytes("M"));
        put_4.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes(31L));
        put_4.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("desc"), Bytes.toBytes("test ccc"));
        put_4.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("jiangxi"));
        puts.add(put_4);

        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        table.put(puts);
        System.out.println("puts success!");
    }

    @Test
    public void testScan() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        Scan scan = new Scan(Bytes.toBytes("usc_stu_1002"), Bytes.toBytes("usc_stu_1004"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"))));
        }
        scanner.close();
    }
}
