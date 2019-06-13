import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HelloSpark {
    public static void main(String[] args) {
        if (args.length != 2)
            return;
        //local，让spark运行在单机单线程上，而无需连接集群
        //如果环境变量HADOOP_CONF_DIR有效，则setMaster()将失效
        SparkConf conf = new SparkConf().setMaster("local").setAppName("HelloSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(args[0]);
        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                String[] list = s.split(" ");
                List<String> newList = new ArrayList<String>();
                for (int i = 0; i < list.length; i++) {
                    list[i] = list[i].trim().toLowerCase();
                    if (!StringUtils.isEmpty(list[i])) {
                        newList.add(list[i]);
                    }
                }
                return newList.iterator();
            }
        });
        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                s = s.trim();
                if (s.endsWith(",") || s.endsWith(".") || s.endsWith("!") || s.endsWith(":") || s.endsWith("”") || s.endsWith(";") || s.endsWith(")")) {
                    s = s.substring(0, s.length() - 1);
                }
                if (s.startsWith("“") || s.startsWith("(")) {
                    s = s.substring(1, s.length());
                }
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        File outFile = new File(args[1]);
        counts.saveAsTextFile(outFile.getAbsolutePath());
    }
}

/*
 * $ cd hadoop-cluster-env/
 * $ tar zxvf spark-2.4.3-bin-hadoop2.7.tgz
 * $ gedit ~/.bashrc
 * export SPARK_HOME=/mnt/hgfs/hadoop-cluster-env/spark-2.4.3-bin-hadoop2.7
 * export PATH=$PATH:$SPARK_HOME/bin
 * $ source ~/.bashrc
 * $ rm -rf /home/yong/stu-hadoop/hellospark/output
 * $ spark-submit --class HelloSpark /home/yong/stu-hadoop/hellospark/target/hello-spark-1.0-SNAPSHOT.jar /home/yong/stu-hadoop/hellospark/test_file.txt /home/yong/stu-hadoop/hellospark/output
 * */

//在集群上运行
/*
 * $ hadoop dfsadmin -report
 * $ echo $HADOOP_CONF_DIR
 * $ hdfs dfs -mkdir -p /yong/spark
 * $ hdfs dfs -put /home/yong/stu-hadoop/hellospark/test_file.txt /yong/spark/
 * $ hdfs dfs -ls /yong/spark/
 * $ spark-submit --master yarn --class HelloSpark /home/yong/stu-hadoop/hellospark/target/hello-spark-1.0-SNAPSHOT.jar /yong/spark/test_file.txt /yong/spark/output
 * $ hdfs dfs -ls /yong/spark/output
 * $ hdfs dfs -cat /yong/spark/output/part-00000
 */