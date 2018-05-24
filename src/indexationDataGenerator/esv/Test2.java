package indexationDataGenerator.esv;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.util.Random;

/**
 * Test2
 *
 * @author cn-seo-dev@
 */
public class Test2 {

    private void addESVColumn(String inputPath, String outputPath) {
        SparkSession sparkSession = SparkSession.builder().appName("test").getOrCreate();

        Dataset<Row> esvCandidate = sparkSession.read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath);

        Dataset<Row> esv = esvCandidate
                .withColumn("esv", functions.lit(0).cast(DataTypes.IntegerType))
                .withColumn("create_date", functions.lit("2018-03-06").cast(DataTypes.StringType))
//                .as(Encoders.bean(ESVDataType.class))
//                .map((MapFunction<ESVDataType, ESVDataType>) row -> {
//                    row.setEsv(new Random().nextInt(10));
//                    return row;
//                }, Encoders.bean(ESVDataType.class))
                .select("marketplace_id", "page_id", "number_of_refinement", "keyword", "check_esv", "create_date", "esv");


        esv.coalesce(100)
                .write()
                .mode(SaveMode.Overwrite)
                .option("delimiter", "\t")
                .option("header", true)
                .csv(outputPath);
    }

    public static void main(String[] args) {
//        String inputPath = "/Users/jcsai/Downloads/My Project/indexation_data_generator/esv_candidate_2_esv/esv-candidate.csv";
//        String outputPath = "/Users/jcsai/Downloads/My Project/indexation_data_generator/esv_candidate_2_esv/esv-data-test";

//        String inputPath = "s3://seo-esv-candidate/1/2018-03-05";
//        String outputPath = "s3://clickstream-full3/esv/1/2018-03-05";

        String inputPath = args[0];
        String outputPath = args[1];

        new Test2().addESVColumn(inputPath, outputPath);
    }
}
