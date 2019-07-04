import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.util.UUID;

public class TestTxtFile {

    //name,stuNum,gender,discipline,grade,class,birthplace

    /*
     * $ hdfs dfs -rm -r /user/yong/input/com.thinking.DataToHBaseSimplePut
     * $ hdfs dfs -ls /user/yong/input/com.thinking.DataToHBaseSimplePut
     * $ hdfs dfs -mkdir -p /user/yong/input/com.thinking.DataToHBaseSimplePut
     * $ hdfs dfs -put /home/yong/stu-hadoop/saprk-data-to-hbase/test_data/* /user/yong/input/com.thinking.DataToHBaseSimplePut
     * $ hdfs dfs -ls /user/yong/input/com.thinking.DataToHBaseSimplePut
     */

    @Test
    public void createFileTest() throws Exception {
        File root_path = new File(".");
        System.out.println("file-->" + root_path.getAbsolutePath());
        File testFileDir = new File(root_path.getAbsolutePath() + "/test_data");
        if (!testFileDir.exists()) {
            testFileDir.mkdirs();
        } else {
            testFileDir.delete();
            testFileDir.mkdirs();
        }
        for (int i = 0; i < 5; i++) {
            File testFile = new File(testFileDir.getAbsolutePath() + "/test" + i);
            testFile.createNewFile();
            FileWriter fw = new FileWriter(testFile);
            for (int j = 0; j < 100; j++) {
                fw.write(getStuInfo() + "\n");
            }
            fw.close();
        }
    }

    private static Random random = new Random();

    private static String getStuInfo() {
        return getName() + "-->" + UUID.randomUUID() + "-->" + new String[]{"M", "F"}[random.nextInt(2)] + "-->" + getDiscipline() + "-->" + (random.nextInt(4) + 1) + "-->" + (random.nextInt(3) + 1) + "-->" + getBirthplace();
    }

    private static String getName() {
        int nameLeg = 3 + random.nextInt(15);
        String name = "";
        for (int i = 0; i < nameLeg; i++) {
            char c = (char) ('a' + random.nextInt(26));
            name += c;
        }
        return name;
    }

    private static String getDiscipline() {
        String[] disciplines = new String[]{
                "Philosophy",
                "Philosophy of Marxism",
                "Chinese Philosophy",
                "Foreign Philosophy",
                "Logic",
                "Ethics",
                "Science of Religion",
                "Philosophy of Science and Technology",
                "Oriental Philosophy and Religions",
                "Economics",
                "Theoretical Economics",
                "Political Economy",
                "Western Economics",
                "World Economics",
                "Population, Resources and Environmental Economics",
                "Industrial Economy and Investment",
                "Chemistry",
                "Chemistry and Physics of Polymers",
                "Cartography and Geography Information System",
                "Atmospheric Science",
                "Groundwater Science",
        };
        return disciplines[random.nextInt(disciplines.length)];
    }

    private static String getBirthplace() {
        String[] places = new String[]{
                "beijing",
                "tianjing",
                "hubei",
                "hunan",
                "fujian",
                "guangdong",
                "xizang",
                "xinjiang",
                "qinghai",
                "neimenggu",
                "jiangxi",
                "qinghai",
                "shanxi",
                "chongqing",
                "sichuan",
                "yunnan",
                "guizhou",
                "shandong",
                "heilongjiang",
                "jiling",
                "liaoning",
                "zhejiang",
                "jiangsu",
                "guangxi"
        };
        return places[random.nextInt(places.length)];
    }
}

