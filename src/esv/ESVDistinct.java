package esv;

import org.apache.spark.sql.*;

/**
 * ESVDistinct
 *
 * @author cn-seo-dev@
 */
public class ESVDistinct {

    private void distinct(String inputPath, String outputPath) {
        SparkSession sparkSession = SparkSession.builder().appName("test").getOrCreate();

        Dataset<Row> result = sparkSession.read()
                .option("delimiter", "\t")
                .option("header", true)
                .csv(inputPath)
                .distinct();

        System.out.println("count:" + result.count());

        result.coalesce(50)
                .write()
                .mode(SaveMode.Overwrite)
                .option("delimiter", "\t")
                .option("header", true)
                .csv(outputPath);
    }

    public static void main(String[] args) {
//        String inputPath = "testdata/r1_meta";
//        String inputPath = "/Users/jcsai/Downloads/My Project/indexation_data_generator/S-R1M-Discount.txt";
//        String outputPath = "/Users/jcsai/Downloads/My Project/indexation_data_generator/esv_candidate_2_esv/esv-data-test";

        new ESVDistinct().distinct(args[0], args[1]);
    }
}
