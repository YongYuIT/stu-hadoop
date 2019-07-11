import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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

public class WordFrequency {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("WordFrequency");//local[4]代表4个本地线程
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(10));
        JavaDStream<String> DStream = jssc.socketTextStream("0.0.0.0", 9999);
        JavaDStream<String> words = DStream.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
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
        /*
        wordCounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            public void call(JavaPairRDD<String, Integer> o) throws Exception {
                for (Tuple2<String, Integer> stringIntegerTuple2 : o.collect()) {
                    System.out.println("######" + stringIntegerTuple2._1() + "-->" + stringIntegerTuple2._2());
                }
            }
        });
        */

        jssc.start();
        jssc.awaitTermination();
    }
}

/*
$ nc -lk 9999
*/

/*
$ export HADOOP_CONF_DIR=
$ spark-submit --class WordFrequency /home/yong/stu-hadoop20190709001/hello-spark-streaming/target/hello-spark-streaming-1.0-SNAPSHOT.jar
*/