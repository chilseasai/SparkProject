package trafficdata;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * TommySearchesFinder
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class TommySearchesDataFinder {

    private static final String sql =
            "select * from tommysearches where session in ('133-3557303-3839842', '131-8184615-4452932', '143-7312690-5397451')";

    private final SparkSession sparkSession;

    public void find(final String inputPath, final String outputPath) {
        final Dataset<Row> inputDF = readCSV(inputPath);

        inputDF.createOrReplaceTempView("tommysearches");
        final Dataset<Row> searchDF = sparkSession.sql(sql);

        searchDF.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", "\t")
                .option("quote", "") // Make the double quotes not work
//                .option("compression", "gzip")
                .csv(outputPath);
    }

    private Dataset<Row> readParquet(final String inputPath) {
        return this.sparkSession.read().parquet(inputPath);
    }

    private Dataset<Row> readCSV(final String inputPath) {
        return sparkSession.read()
                .option("header", true)
                .option("delimiter", "\t")
                .option("quote", "") // Make the double quotes not work
                .option("compression", "gzip")
                .csv(inputPath);
    }

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder().appName("tommy_searches_finder").getOrCreate();
        final TommySearchesDataFinder finder = new TommySearchesDataFinder(sparkSession);
        finder.find(args[0], args[1]);
    }
}
