package gameday;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * SplitFiles
 * Split data in one file to three files that will be dispatched to three hosts for calling Sable
 *
 * @author cn-seo-dev@
 */
public class FileSplitter {

    private void split(final String inputFile, final String outputFile1,
                       final String outputFile2, final String outputFile3) {
        BufferedReader br = null;
        BufferedWriter bw1 = null;
        BufferedWriter bw2 = null;
        BufferedWriter bw3 = null;

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), StandardCharsets.UTF_8));
            bw1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile1), StandardCharsets.UTF_8));
            bw2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile2), StandardCharsets.UTF_8));
            bw3 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile3), StandardCharsets.UTF_8));

            String line;
            int lineNum = 1;
            while ((line = br.readLine()) != null) {
                if (lineNum <= 1500000) {
                    bw1.write(line);
                    bw1.newLine();
                } else if (lineNum <= 3000000) {
                    bw2.write(line);
                    bw2.newLine();
                } else {
                    bw3.write(line);
                    bw3.newLine();
                }
                lineNum ++;
            }

            bw1.flush();
            bw2.flush();
            bw3.flush();
            bw1.close();
            bw2.close();
            bw3.close();

        } catch (final IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String[] args) {
        final String inputFile = "/Users/jcsai/Downloads/My Project/game_day_test/2018/query_400w";
        final String outputFile1 = "/Users/jcsai/Downloads/My Project/game_day_test/2018/query_150w_1";
        final String outputFile2 = "/Users/jcsai/Downloads/My Project/game_day_test/2018/query_150w_2";
        final String outputFile3 = "/Users/jcsai/Downloads/My Project/game_day_test/2018/query_100w_3";

        new FileSplitter().split(inputFile, outputFile1, outputFile2, outputFile3);
    }
}
