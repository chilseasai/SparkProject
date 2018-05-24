package trafficdata;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * ExtractDataFromTommySearch
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class ExtractDataFromTommySearch {

    private final SparkSession spark;

    public void extract(String inputPath, String outputPath, int numPartitons) {
        Dataset<Row> inputDF = this.spark.read()
                .parquet(inputPath);

        Dataset<Row> extractDF = inputDF.select("query_string", "request_id", "session", "gmt_time")
                .withColumnRenamed("gmt_time", "starttime");

        extractDF.coalesce(numPartitons)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", "\t")
                .option("quote", "") // Make the double quotes not work
                .option("compression", "gzip")
                .csv(outputPath);
    }

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("TommySearchesDataExtractor")
                .getOrCreate();

        ExtractDataFromTommySearch extractor = new ExtractDataFromTommySearch(sparkSession);
        extractor.extract(args[0], args[1], Integer.valueOf(args[2]));
    }
}
