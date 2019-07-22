package com.thinking.grpc.impl;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.thinking.grpc.proto.StuFractionalProto;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FractionalInfoEndpoint extends StuFractionalProto.FractionalInfoService implements Coprocessor, CoprocessorService {
    @Override
    public void getFractionalInfo(RpcController controller, StuFractionalProto.GetFractionalInfoReq request, RpcCallback<StuFractionalProto.GetFractionalInfoResp> done) {
        String type = request.getType();
        String palce = request.getPlace();
        StuFractionalProto.GetFractionalInfoResp.Builder result = StuFractionalProto.GetFractionalInfoResp.newBuilder().setPlace(palce).setType(type);
        System.out.println("FractionalInfoEndpoint req-->" + type + "-->" + palce);
        try {
            if (type.equals("MAX")) {
                System.out.println("do max");
                result.setFractional(getMax(palce));
            } else if (type.equals("MIN")) {
                result.setFractional(getMin(palce));
            } else if (type.equals("AVG")) {
                result.setFractional(getAvg(palce));
            }
        } catch (IOException e) {
            System.out.println("error-->" + e.getMessage());
            ResponseConverter.setControllerException(controller, e);
        }

        done.run(result.build());
    }

    private RegionCoprocessorEnvironment environment;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        environment = (RegionCoprocessorEnvironment) env;
    }

    @Override
    public Service getService() {
        return this;
    }

    private Result[] getvalue(String place) throws IOException {
        TableName tabPlaceStusName = TableName.valueOf("birthplace_stu_index");
        Table tabPlaceStus = environment.getConnection().getTable(tabPlaceStusName);
        Get getPalceStus = new Get(Bytes.toBytes(place));
        Result result = tabPlaceStus.get(getPalceStus);
        Map<byte[], byte[]> stusMap = result.getFamilyMap(Bytes.toBytes("birthplace_stu_info"));
        List<Get> getStuList = new ArrayList();
        System.out.println("get stu set");
        for (Map.Entry<byte[], byte[]> stu : stusMap.entrySet()) {
            System.out.println("get stu set-->" + Bytes.toString(stu.getKey()));
            getStuList.add(new Get(stu.getKey()));
        }
        tabPlaceStus.close();

        TableName tabStuName = TableName.valueOf("student");
        Table tabStu = environment.getConnection().getTable(tabStuName);
        Result[] results = tabStu.get(getStuList);//可以看出这种方式在大数据量下，内存压力可能会很大。这就是采用mapreduce或者spark的原因之一
        tabStu.close();
        return results;
    }

    private float getMax(String place) throws IOException {
        System.out.println("doing max");
        Result[] results = getvalue(place);
        float max = 0;
        for (Result result : results) {
            float value = Float.parseFloat(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("fractional"))));
            if (value > max) {
                max = value;
            }
        }
        System.out.println("get max --> " + max);
        return max;
    }

    private float getMin(String place) throws IOException {
        Result[] results = getvalue(place);
        float min = Float.MAX_VALUE;
        for (Result result : results) {
            float value = Float.parseFloat(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("fractional"))));
            if (value < min) {
                min = value;
            }
        }
        return min;
    }

    private float getAvg(String place) throws IOException {
        Result[] results = getvalue(place);
        float total = 0;
        for (Result result : results) {
            float value = Float.parseFloat(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("fractional"))));
            total += value;
        }
        return total / results.length;
    }
}

/*
####连同BirthplaceToStu一起采用静态部署
$ stop-hbase.sh
$ rm -rf $HBASE_HOME/lib/hell*
$ cp -r /home/yong/stu-hadoop20190717001/hello-hbase-coprocessor/target/hello-hbase-coprocessor-1.0-SNAPSHOT-jar-with-dependencies.jar $HBASE_HOME/lib/
$ gedit $HBASE_HOME/conf/hbase-site.xml
  <property>
    <name>hbase.coprocessor.user.region.classes</name>
    <value>BirthplaceToStu</value>
  </property>
$ rm -rf $HBASE_HOME/logs/
$ start-hbase.sh
$ hbase shell
> list
> create 'student',{NAME=>'basic_info'},{NAME=>'more_info'}
####不必再用shell命令加载，配置文件里面的协处理器一律全部启用
> describe 'student'
> create 'birthplace_stu_index',{NAME=>'birthplace_stu_info'},{NAME=>'more_info'}
> put 'student','stu-100001','basic_info:birthplace','hubei'
####此时在此处出错，后面解决，先绕过
*/


/*
####com.thinking.grpc.impl.FractionalInfoEndpoint静态部署，BirthplaceToStu动态部署
$ stop-hbase.sh
$ rm -rf $HBASE_HOME/lib/hell*
$ cp -r /home/yong/stu-hadoop20190717001/hello-hbase-coprocessor/target/hello-hbase-coprocessor-1.0-SNAPSHOT-jar-with-dependencies.jar $HBASE_HOME/lib/
$ gedit $HBASE_HOME/conf/hbase-site.xml
    #  <property>
    #    <name>hbase.coprocessor.user.region.classes</name>
    #    <value>BirthplaceToStu</value>
    #  </property>
$ rm -rf $HBASE_HOME/logs/
$ start-hbase.sh
$ hbase shell
> list
> create 'student',{NAME=>'basic_info'},{NAME=>'more_info'}
> disable 'student'
> alter 'student', METHOD => 'table_att', 'coprocessor' => '|BirthplaceToStu||'
> enable 'student'
> describe 'student'
> create 'birthplace_stu_index',{NAME=>'birthplace_stu_info'},{NAME=>'more_info'}
> put 'student','stu-100001','basic_info:birthplace','hubei'
> put 'student','stu-100001','basic_info:fractional','555'
> put 'student','stu-100002','basic_info:birthplace','jiangxi'
> put 'student','stu-100003','basic_info:fractional','556'
> scan 'student'
> scan 'birthplace_stu_index'
> exit
$ stop-hbase.sh
$ gedit $HBASE_HOME/conf/hbase-site.xml
  <property>
    <name>hbase.coprocessor.user.region.classes</name>
    <value>com.thinking.grpc.impl.FractionalInfoEndpoint</value>
  </property>
$ start-hbase.sh
*/