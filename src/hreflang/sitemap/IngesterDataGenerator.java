/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package hreflang.sitemap;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * Generate Ingester Data for Mapping Generator test.
 * Ingester Data has 6 columns: {id, href, hreflang, last_update_time, is_del, partition_id}
 * Ingester Data is Parquet format.
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class IngesterDataGenerator {

    private final SparkSession sparkSession;

    public void generateIngesterData(final String inputPath, final String outputPath) {
        final Dataset<Row> inputDF = sparkSession.read().parquet(inputPath)
                .withColumnRenamed("item_id", "id")
                .withColumnRenamed("timestamp", "last_update_time")
                .drop("marketplace_id")
                .withColumn("partition_id", functions.lit(1).cast(DataTypes.IntegerType));

        inputDF.show(false);

        inputDF.write()
                .mode(SaveMode.Append)
                .partitionBy("partition_id")
                .parquet(outputPath);
    }

    public static void main(String[] args) {
        final String inputPath = "/Users/jcsai/Downloads/My Project/hreflang_sitemap/ingester_output/partition_id=1";
        final String outputPath = "/Users/jcsai/Downloads/My Project/hreflang_sitemap/my_ingester_output/";

        final SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        new IngesterDataGenerator(sparkSession).generateIngesterData(inputPath, outputPath);
    }
}
