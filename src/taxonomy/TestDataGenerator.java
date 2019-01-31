package taxonomy;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * TestDataGenerator
 * Join node_id and term1 to generate taxonomy test data for team internal.
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class TestDataGenerator {

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

    /**
     * Combine node_id, store_name and seoterm1 columns
     *
     * @param nodeIdPath
     * @param storeNamePath
     * @param termPath
     * @param outputPath
     */
    private void run(final String nodeIdPath, final String storeNamePath, final String termPath, final String outputPath) {
        final Dataset<Row> nodeIdDF = read(nodeIdPath)
                .select("Node Id")
                .withColumnRenamed("Node Id", "node_id");
//        final Dataset<Row> storeNameDF = read(storeNamePath)
//                .select("Node IDs", "store_name")
//                .withColumnRenamed("Node IDs", "node_id");
        final Dataset<Row> termDF = read(termPath)
                .select("NodeId", "SEOTerm1")
                .withColumnRenamed("NodeId", "node_id")
                .withColumnRenamed("SEOTerm1", "term");

        // 内连接：连接的行"node_id"只会出现一次
        final Dataset<Row> joinDF = nodeIdDF
//                .join(storeNameDF,"node_id")
                .join(termDF, "node_id")
//                .sort("store_name", "node_id")
                .sort("node_id")
                .distinct();

        joinDF.show();

        write(joinDF, outputPath);
    }

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder().master("local").appName("test").getOrCreate();
        final TestDataGenerator generator = new TestDataGenerator(sparkSession);

        final String nodeIdPath = "/Users/jcsai/Downloads/My Project/Taxonomy Search/16185_node_experiment/9463_node_only.txt";
        final String storeNamePath = "/Users/jcsai/Downloads/storeName";
        final String termPath = "/Users/jcsai/Downloads/My Project/Taxonomy Search/sable data/term.txt";
        final String outputPath = "/Users/jcsai/Downloads/My Project/Taxonomy Search/16185_node_experiment/9463_node_keyword";

        generator.run(nodeIdPath, storeNamePath, termPath, outputPath);
    }
}
