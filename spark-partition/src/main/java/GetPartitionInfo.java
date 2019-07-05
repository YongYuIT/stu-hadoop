import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class GetPartitionInfo {
    static class Student {
        public String name;
        public String stuNum;
        public String discipline;
        public String gender;
        public String _class;
        public String birthplace;
        public String grade;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("com.thinking.DataToHBaseSimplePut");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = sc.textFile(args[0]);
        JavaPairRDD<String, Student> stuRdd = inputRDD.mapToPair(new PairFunction<String, String, Student>() {
            public Tuple2<String, Student> call(String s) throws Exception {
                String[] values = s.split("-->");
                Student stu = new Student();
                stu.name = values[0];
                stu.stuNum = values[1];
                stu.gender = values[2];
                stu.discipline = values[3];
                stu.grade = values[4];
                stu._class = values[5];
                stu.birthplace = values[6];
                return new Tuple2<String, Student>(stu.stuNum, stu);
            }
        });
        Optional<Partitioner> partitionerOptional = stuRdd.partitioner();
        System.out.println("-------------is def-->" + partitionerOptional.isPresent());
        stuRdd = stuRdd.sortByKey();
        partitionerOptional = stuRdd.partitioner();
        System.out.println("-------------is def 2-->" + partitionerOptional.isPresent() + "-->" + stuRdd.partitions().size());

        stuRdd = stuRdd.partitionBy(new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }

            @Override
            public int getPartition(Object key) {
                return ((String) key).charAt(0) % 2;
            }
        });
        partitionerOptional = stuRdd.partitioner();
        System.out.println("-------------is def 3-->" + partitionerOptional.isPresent() + "-->" + stuRdd.partitions().size());

    }
}

/*
$ spark-submit --master yarn --class GetPartitionInfo /home/yong/stu-hadoop20190705001/spark-partition/target/spark-partition-1.0-SNAPSHOT.jar /user/yong/input/spark-partition/
*/
