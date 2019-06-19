import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class GetTest {

    @Test
    public void getTest() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        Get get = new Get(Bytes.toBytes("stu_005"));
        Result result = table.get(get);
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"))));
        System.out.println(Bytes.toLong(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("age"))));
    }

    @Test
    public void getFamilys() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        Get get = new Get(Bytes.toBytes("stu_005"));
        get.addFamily(Bytes.toBytes("basic_info"));

        Result result = table.get(get);
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"))));
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("more_info"), Bytes.toBytes("add"))));
    }

    @Test
    public void getColumns() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        Get get = new Get(Bytes.toBytes("stu_005"));
        get.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"));
        Result result = table.get(get);
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("name"))));
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"))));
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("more_info"), Bytes.toBytes("add"))));

    }

}
