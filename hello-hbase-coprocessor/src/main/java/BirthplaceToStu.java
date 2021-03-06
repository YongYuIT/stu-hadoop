import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BirthplaceToStu implements RegionObserver, RegionCoprocessor {


    @Override
    public Optional<RegionObserver> getRegionObserver() {
        RegionObserver observer = this;
        return Optional.of(observer);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        //静态加载的协处理器会对所有表的prePut生效，动态加载的协处理器只对指定的表起作用
        long callId = Time.now();
        System.out.println("----------------------call prePut start-->" + callId);
        List<Cell> birthplace = put.get(Bytes.toBytes("basic_info"), Bytes.toBytes("birthplace"));
        if (birthplace == null || birthplace.size() == 0) {
            System.out.println("----------------------birthplace is empty");
            System.out.println("----------------------call prePut end-->" + callId);
            return;
        }
        System.out.println("----------------------birthplace is not empty");
        String stuId = Bytes.toString(put.getRow());
        System.out.println("----------------------stuId=" + stuId);
        String sBirthplace = Bytes.toString(CellUtil.cloneValue(birthplace.get(0)));
        Put index = new Put(Bytes.toBytes(sBirthplace));
        index.addColumn(Bytes.toBytes("birthplace_stu_info"), Bytes.toBytes(stuId), Bytes.toBytes("1"));
        Connection connection = c.getEnvironment().getConnection();
        TableName tableName = TableName.valueOf("birthplace_stu_index");
        Table table = connection.getTable(tableName);
        for (Map.Entry<byte[], List<Cell>> listEntry : index.getFamilyCellMap().entrySet()) {
            System.out.println("---------------------- index family-->" + Bytes.toString(listEntry.getKey()) + "-->" + callId);
        }
        try {
            table.put(index);
        } catch (IOException e) {
            System.out.println("----------------------put error-->" + e.getMessage() + "-->" + callId);
        } finally {
            table.close();//这样干效率不高
        }
        System.out.println("----------------------call prePut end-->" + callId);
    }
}

/*
$ hdfs dfs -mkdir -p /usr/alex
$ hdfs dfs -put /home/yong/stu-hadoop20190717001/hello-hbase-coprocessor/target/hello-hbase-coprocessor-1.0-SNAPSHOT.jar /usr/alex/
$ hbase shell
> list
> create 'student',{NAME=>'basic_info'},{NAME=>'more_info'}
> disable 'student'
> alter 'student', METHOD => 'table_att', 'coprocessor' => '/usr/alex/hello-hbase-coprocessor-1.0-SNAPSHOT.jar|BirthplaceToStu||'
> enable 'student'
> describe 'student'
> create 'birthplace_stu_index',{NAME=>'birthplace_stu_info'},{NAME=>'more_info'}
> put 'student','stu-100001','basic_info:birthplace','hubei'
> scan 'student'
> scan 'birthplace_stu_index'
> disable 'student'
> alter 'student', METHOD => 'table_att_unset', NAME => 'coprocessor$1'
> enable 'student'
> hdfs dfs -rm /usr/alex/hello-hbase-coprocessor-1.0-SNAPSHOT.jar
*/