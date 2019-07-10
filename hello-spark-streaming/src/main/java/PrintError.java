import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class PrintError {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("PrintError");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaDStream<String> inputStrm = jssc.socketTextStream("0.0.0.0", 7777);
        inputStrm.filter(new Function<String, Boolean>() {
            public Boolean call(String v1) throws Exception {
                return v1.contains("error");
            }
        }).print();
        jssc.start();
        jssc.awaitTermination();
    }
}
/*
$ export HADOOP_CONF_DIR=
$ spark-submit --class PrintError /home/yong/stu-hadoop20190709001/hello-spark-streaming/target/hello-spark-streaming-1.0-SNAPSHOT.jar local[4]
*/

/*
$ nc -l 7777 //服务器socket，监听本机7777端口
$ nc -nvv 0.0.0.0 7777 //客户端socket，连接0.0.0.0的7777端口
*/