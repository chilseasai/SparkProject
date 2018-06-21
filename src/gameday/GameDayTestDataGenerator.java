package gameday;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * GameDayTestDataGenerator
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class GameDayTestDataGenerator {

    private final SparkSession sparkSession;

    private Dataset<Row> read(final String inputPath) {
        return this.sparkSession.read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("encoding", "UTF-8")
                .csv(inputPath);
    }

    private void write(final Dataset<Row> data, final String outputPath) {
        data.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("delimiter", "\t")
                .option("header", true)
                .option("quote", "")
                .option("encoding", "UTF-8")
                .csv(outputPath);
    }


    private void run(final String inputPath, final String outputPath) {
        final Dataset<Row> nodeIdPageIdDF = read(inputPath)
                .where("index = 0")
                .select("browse_node_id", "page_id")
                .limit(3000000);

        nodeIdPageIdDF.show();

        write(nodeIdPageIdDF, outputPath);
    }

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder().master("local").appName("test").getOrCreate();
        final GameDayTestDataGenerator generator = new GameDayTestDataGenerator(sparkSession);

        final String inputPath = "/Users/jcsai/Downloads/My Project/game_day_test/R2/part-*";
        final String outputPath = "/Users/jcsai/Downloads/My Project/game_day_test/R2/noindex";

        generator.run(inputPath, outputPath);
    }
}
