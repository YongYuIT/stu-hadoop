import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/*
 * $ hbase shell
 * > list
 * > create 'student',{NAME=>'basic_info'},{NAME=>'more_info'}
 * ERROR: org.apache.hadoop.hbase.PleaseHoldException: Master is initializing
 * check logs
 * java.lang.NoClassDefFoundError: Could not initialize class org.apache.hadoop.hbase.io.asyncfs.FanOutOneBlockAsyncDFSOutputHelper
 * $ stop-hbase.sh
 * $ gedit $HBASE_HOME/conf/hbase-site.xml
   <property>
     <name>hbase.wal.provider</name>
     <value>filesystem</value>
   </property>
 * $ start-hbase.sh
 * $ hbase shell
 * > create 'student',{NAME=>'basic_info'},{NAME=>'more_info'}
 * > list
 * > put 'student','sid_001','basic_info:name','yuyong'
 * > put 'student','sid_001','basic_info:gen','M'
 * > put 'student','sid_001','more_info:desc','hello'
 * > put 'student','sid_001','more_info:add','hubei'
 * > scan 'student'
 *
 * table show like this:
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

public class Main {
    /*
     * $ cd src/main/resources
     * $ mkdir hbase
     * $ cp $HBASE_HOME/conf/hbase-site.xml hbase/
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");
        Admin admin = connection.getAdmin();
        System.out.println("get admin success!-->" + admin.getMaster().getHostname());

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);
        Put put = new Put(Bytes.toBytes("sid_003"));
        put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("guoben"));
        put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"), Bytes.toBytes("F"));
        put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("desc"), Bytes.toBytes("fuck"));
        put.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("add"), Bytes.toBytes("hubei"));
        table.put(put);
        System.out.println("put success!");

    }

}
