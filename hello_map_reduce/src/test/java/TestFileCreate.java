import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

public class TestFileCreate {

    /*
     * $ hdfs dfs -rm -r /user/yong/input/
     * $ hdfs dfs -ls /user/yong/input
     * $ hdfs dfs -mkdir -p /user/yong/input
     * $ hdfs dfs -put /home/yong/Desktop/hello_map_reduce/test_data/ /user/yong/input
     * $ hdfs dfs -ls /user/yong/input
     */

    @Test
    public void test() throws Exception {
        File root_path = new File(".");
        System.out.println("file-->" + root_path.getAbsolutePath());
        File testFileDir = new File(root_path.getAbsolutePath() + "/test_data");
        if (!testFileDir.exists()) {
            testFileDir.mkdirs();
        } else {
            testFileDir.delete();
            testFileDir.mkdirs();
        }
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            File testFile = new File(testFileDir.getAbsolutePath() + "/test" + i);
            testFile.createNewFile();
            FileWriter fw = new FileWriter(testFile);
            for (int j = 0; j < 20; j++) {
                //1949~2019 30~55
                fw.write((1949 + random.nextInt(70)) + "-->" + (3000 + random.nextInt(2500) + 0.0) / 100 + "\n");
            }
            fw.close();
        }
    }
}
