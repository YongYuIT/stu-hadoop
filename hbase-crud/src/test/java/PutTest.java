import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class PutTest {
    @Test
    public void putTest() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        Put putBen = new Put(Bytes.toBytes("stu_001"));
        putBen.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("ben"));
        putBen.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"), Bytes.toBytes("M"));
        putBen.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("desc"), Bytes.toBytes("benben"));
        putBen.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("china"));
        table.put(putBen);
        System.out.println("put success!");


    }

    @Test
    public void checkAndPutTest() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        Put modifyBen = new Put(Bytes.toBytes("stu_001"));
        modifyBen.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("hubei"));
        boolean isSuccess = table.checkAndPut(Bytes.toBytes("stu_001"), Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("china"), modifyBen);
        System.out.println("checkAndPut-->" + isSuccess);
        isSuccess = table.checkAndPut(Bytes.toBytes("stu_001"), Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("china"), modifyBen);
        System.out.println("checkAndPut-->" + isSuccess);
    }

    @Test
    public void checkAndPutOptTest() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        Put putYong = new Put(Bytes.toBytes("stu_002"));
        putYong.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("yong"));
        putYong.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"), Bytes.toBytes("F"));
        putYong.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes("28"));
        putYong.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("desc"), Bytes.toBytes("hello wprd"));
        putYong.addColumn(Bytes.toBytes("more_info"), Bytes.toBytes("add"), Bytes.toBytes("hubei"));
        table.put(putYong);
        System.out.println("put success!");

        Put modifyYong = new Put(Bytes.toBytes("stu_002"));
        modifyYong.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes("29"));
        boolean isSuccess = table.checkAndPut(Bytes.toBytes("stu_002"), Bytes.toBytes("basic_info"), Bytes.toBytes("age"), CompareFilter.CompareOp.GREATER, Bytes.toBytes("29"), modifyYong);
        System.out.println("checkAndPut-->" + isSuccess);
        isSuccess = table.checkAndPut(Bytes.toBytes("stu_002"), Bytes.toBytes("basic_info"), Bytes.toBytes("age"), CompareFilter.CompareOp.GREATER, Bytes.toBytes("29"), modifyYong);
        System.out.println("checkAndPut-->" + isSuccess);
    }

}
