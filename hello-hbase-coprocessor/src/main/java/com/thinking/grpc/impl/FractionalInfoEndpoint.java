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
        try {
            if (type == "MAX") {
                result.setFractional(getMax(palce));
            } else if (type == "MIN") {
                result.setFractional(getMin(palce));
            } else if (type == "AVG") {
                result.setFractional(getAvg(palce));
            }
        } catch (IOException e) {
            ResponseConverter.setControllerException(controller, e);
        }

        done.run(result.build());
    }

    @Override
    public Service getService() {
        return this;
    }

    private RegionCoprocessorEnvironment environment;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        environment = (RegionCoprocessorEnvironment) env;
    }

    private Result[] getvalue(String place) throws IOException {
        TableName tabPlaceStusName = TableName.valueOf("birthplace_stu_index");
        Table tabPlaceStus = environment.getConnection().getTable(tabPlaceStusName);
        Get getPalceStus = new Get(Bytes.toBytes(place));
        Result result = tabPlaceStus.get(getPalceStus);
        Map<byte[], byte[]> stusMap = result.getFamilyMap(Bytes.toBytes("birthplace_stu_info"));
        List<Get> getStuList = new ArrayList();
        for (Map.Entry<byte[], byte[]> stu : stusMap.entrySet()) {
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
        Result[] results = getvalue(place);
        float max = 0;
        for (Result result : results) {
            float value = Float.parseFloat(Bytes.toString(result.getValue(Bytes.toBytes("basic_info"), Bytes.toBytes("fractional"))));
            if (value > max) {
                max = value;
            }
        }
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
$ stop-hbase.sh
$ cp /home/yong/stu-hadoop20190717001/hello-hbase-coprocessor/target/hello-hbase-coprocessor-1.0-SNAPSHOT.jar $HBASE_HOME/lib/
$ gedit $HBASE_HOME/conf/hbase-site.xml
  <property>
    <name>hbase.coprocessor.user.region.classes</name>
    <value>com.thinking.grpc.impl.FractionalInfoEndpoint</value>
  </property>
$ start-hbase.sh
*/