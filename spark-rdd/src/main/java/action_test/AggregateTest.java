package action_test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Arrays;

public class AggregateTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("action_test.AggregateTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        MyResult result = rdd1.aggregate(new MyResult(0, 0), new Function2<MyResult, Integer, MyResult>() {
            public MyResult call(MyResult v1, Integer v2) throws Exception {
                System.out.println("func 1-->" + v2);
                v1.sum += v2;
                v1.count += 1;
                return v1;
            }
        }, new Function2<MyResult, MyResult, MyResult>() {
            public MyResult call(MyResult v1, MyResult v2) throws Exception {
                System.out.println("func 2-->" + v1.sum + "-->" + v2.sum);
                v1.sum += v2.sum;
                v1.count += v2.count;
                return v1;
            }
        });
        System.out.println("result-->" + result.getAvg());
    }

    public static class MyResult implements Serializable {
        public int sum;
        public int count;

        public MyResult(int _count, int _sum) {
            sum = _sum;
            count = _count;
        }

        public float getAvg() {
            return ((float) sum) / count;
        }
    }

    /*
     * $ hadoop dfsadmin -report
     * $ echo $HADOOP_CONF_DIR
     * $ spark-submit --master yarn --class action_test.AggregateTest /home/yong/stu-hadoop/spark-rdd/target/spark-rdd-1.0-SNAPSHOT.jar
     */
}
