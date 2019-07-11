import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class ImputFromHDFSPath {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("ImputFromHDFSPath");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
        JavaDStream<String> DStream = jssc.textFileStream("hdfs://localhost/user/yong/input/ImputFromHDFSPath");

        JavaDStream<String> words = DStream.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                System.out.println("input str-------->" + s);
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairDStream wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}

/*
$ hdfs dfs -rm -r /user/yong/input/ImputFromHDFSPath
$ hdfs dfs -mkdir -p /user/yong/input/ImputFromHDFSPath
$ hdfs dfs -ls /user/yong/input/ImputFromHDFSPath
$ hdfs dfs -put /home/yong/stu-hadoop20190709001/hello-spark-streaming/test_data/test0 /user/yong/input/ImputFromHDFSPath/

$ spark-submit --master yarn --class ImputFromHDFSPath /home/yong/stu-hadoop20190709001/hello-spark-streaming/target/hello-spark-streaming-1.0-SNAPSHOT.jar  > log.txt

$ hdfs dfs -put /home/yong/stu-hadoop20190709001/hello-spark-streaming/test_data/test1 /user/yong/input/ImputFromHDFSPath/
$ hdfs dfs -put /home/yong/stu-hadoop20190709001/hello-spark-streaming/test_data/test2 /user/yong/input/ImputFromHDFSPath/
*/
