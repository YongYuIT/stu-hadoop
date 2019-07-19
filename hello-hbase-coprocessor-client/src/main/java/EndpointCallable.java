import com.google.protobuf.RpcCallback;
import com.thinking.grpc.proto.StuFractionalProto;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import java.io.IOException;

public class EndpointCallable implements Batch.Call<StuFractionalProto.FractionalInfoService, Float> {

    private StuFractionalProto.GetFractionalInfoReq request;

    public EndpointCallable(StuFractionalProto.GetFractionalInfoReq req) {
        request = req;
    }

    public Float call(StuFractionalProto.FractionalInfoService instance) throws IOException {
        BlockingRpcCallback<StuFractionalProto.GetFractionalInfoResp> callback = new BlockingRpcCallback();
        instance.getFractionalInfo(null, request, (RpcCallback<StuFractionalProto.GetFractionalInfoResp>) callback);
        StuFractionalProto.GetFractionalInfoResp resp = callback.get();
        return resp.getFractional();
    }
}

