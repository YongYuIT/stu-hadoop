import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Random;

public class CreateTxtFile {
    @Test
    public void createFile() throws Exception {
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
                fw.write(getNewLine() + "\n");
            }
            fw.close();
        }
    }

    private static Random random = new Random();

    private static String getNewLine() {
        String str = "";
        for (int i = 0, index = random.nextInt(30); i < index; i++) {
            str += getBirthplace() + " ";
        }
        return str;
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