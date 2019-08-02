/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package hreflang.sitemap.ingester;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

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

        // Add a new row.
        final Timestamp timestamp = Timestamp.valueOf("2019-06-21 15:10:18.573");
        final IngesterData newData = new IngesterData("ASIN2", "https://www.amazon.com/dp/ASIN2", "en-US", timestamp, 0, 1);
        final Dataset<Row> newRowDF = sparkSession.createDataFrame(Arrays.asList(newData), IngesterData.class)
                .select("id", "href", "hreflang", "last_update_time", "is_del", "partition_id");

        final Dataset<Row> unionDF = inputDF.union(newRowDF);
        unionDF.show(false);

        unionDF.coalesce(1).write()
                .mode(SaveMode.Append)
                .partitionBy("partition_id")
                .parquet(outputPath);
    }

    public void generateIngesterDataFromCSV(final String inputPath, final String outputPath) {
        final StructField idField = DataTypes.createStructField("id", DataTypes.StringType, true);
        final StructField hrefField = DataTypes.createStructField("href", DataTypes.StringType, true);
        final StructField hreflangField = DataTypes.createStructField("hreflang", DataTypes.StringType, true);
        final StructField lastUpdateTimeField = DataTypes.createStructField("last_update_time", DataTypes.TimestampType, true);
        final StructField isDelField = DataTypes.createStructField("is_del", DataTypes.IntegerType, true);

        final List<StructField> fields = Arrays.asList(idField, hrefField, hreflangField, lastUpdateTimeField, isDelField);
        final StructType schema = DataTypes.createStructType(fields);

        final Dataset<Row> inputDF = sparkSession.read()
                .option("header", true)
                .option("delimiter", "\t")
                .schema(schema)
                .csv(inputPath)
                .withColumn("partition_id", functions.lit(0).cast(DataTypes.IntegerType));

//        inputDF.printSchema();
        inputDF.coalesce(1).write()
                .mode(SaveMode.Overwrite)
                .partitionBy("partition_id")
                .parquet(outputPath);
    }

    public static void main(String[] args) {
        final String inputPath = "/Users/jcsai/Downloads/My Project/hreflang_sitemap/QA/test data/mapper_logic_check/input_data";
        final String outputPath = "/Users/jcsai/Downloads/My Project/hreflang_sitemap/QA/test data/mapper_logic_check/ingester_output_data";

        final SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        new IngesterDataGenerator(sparkSession).generateIngesterData(inputPath, outputPath);
    }
}
