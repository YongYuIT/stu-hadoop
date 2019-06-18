import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.junit.Test;

public class CreateTabTest {
    /*
     * ----------------------------------------
     * |         | basic_info |  more_info    |
     * |   key   |----------------------------|
     * |         | name | gen | desc  | add   |
     * |--------------------------------------|
     * | sid_001 |yuyong|  M  | hello | hubei |
     * |--------------------------------------|
     * | sid_002 |guoben|  F  | fuck  | hubei |
     * |--------------------------------------|
     */
    @Test
    public void createStuTab() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");
        Admin admin = connection.getAdmin();
        System.out.println("get admin success!-->" + admin.getMaster().getHostname());

        TableName tableName = TableName.valueOf("student");
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor basic_info = new HColumnDescriptor("basic_info");
        HColumnDescriptor more_info = new HColumnDescriptor("more_info");
        tableDescriptor.addFamily(basic_info);
        tableDescriptor.addFamily(more_info);

        admin.createTable(tableDescriptor);
        System.out.println("create success");
        admin.close();
        connection.close();
    }

    @Test
    public void modifyTable() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");
        Admin admin = connection.getAdmin();
        System.out.println("get admin success!-->" + admin.getMaster().getHostname());

        HColumnDescriptor basic_info = new HColumnDescriptor("basic_info");
        System.out.println("MaxVersions-->" + basic_info.getMaxVersions());
        basic_info.setMaxVersions(10);//最大版本数，HBase默认版本数是3 ??
        HColumnDescriptor more_info = new HColumnDescriptor("more_info");
        more_info.setCompactionCompressionType(Compression.Algorithm.GZ);//指定列族的压缩算法

        TableName tableName = TableName.valueOf("student");
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
        tableDescriptor.modifyFamily(basic_info);
        tableDescriptor.modifyFamily(more_info);

        admin.modifyTable(tableName, tableDescriptor);
        System.out.println("modifyTable success");
    }

    @Test
    public void modifyTable_add() throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");
        Admin admin = connection.getAdmin();
        System.out.println("get admin success!-->" + admin.getMaster().getHostname());

        HColumnDescriptor basic_info_add = new HColumnDescriptor("basic_info_add");
        System.out.println("MaxVersions-->" + basic_info_add.getMaxVersions());
        basic_info_add.setMaxVersions(10);//最大版本数，HBase默认版本数是3 ??
        basic_info_add.setCompactionCompressionType(Compression.Algorithm.GZ);//指定列族的压缩算法

        TableName tableName = TableName.valueOf("student");
        admin.addColumnFamily(tableName, basic_info_add);
        System.out.println("modifyTable_add success");
    }
}
