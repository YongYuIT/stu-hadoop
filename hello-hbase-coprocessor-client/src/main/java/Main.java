import com.thinking.grpc.proto.StuFractionalProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class Main {
    /*
     * $ cd src/main/resources
     * $ mkdir hbase
     * $ cp $HBASE_HOME/conf/hbase-site.xml hbase/
     */
    public static void main(String[] args) throws Throwable {
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("conn success!");

        TableName tableName = TableName.valueOf("student");
        Table table = connection.getTable(tableName);

        Put putBen = new Put(Bytes.toBytes("stu_001"));
        putBen.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("name"), Bytes.toBytes("ben"));
        putBen.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("gen"), Bytes.toBytes("M"));
        putBen.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("age"), Bytes.toBytes("22"));
        putBen.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("birthplace"), Bytes.toBytes("hubei"));
        putBen.addColumn(Bytes.toBytes("basic_info"), Bytes.toBytes("fractional"), Bytes.toBytes("555"));
        table.put(putBen);

        StuFractionalProto.GetFractionalInfoReq req = StuFractionalProto.GetFractionalInfoReq.newBuilder().setPlace("hubei").setType("MAX").build();
        Map<byte[], Float> result = table.coprocessorService(StuFractionalProto.FractionalInfoService.class, null, null, new EndpointCallable(req));
        for (byte[] bytes : result.keySet()) {
            System.out.println(Bytes.toString(bytes) + "-->" + result.get(bytes));
        }
    }
}
