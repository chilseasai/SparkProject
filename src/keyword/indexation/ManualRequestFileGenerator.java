/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package keyword.indexation;

import keyword.indexation.datatype.ManualRequestData;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Due to Loom, some pages are de-indexed by mistake.
 * This class is to generate manual request file to index these new page ids.
 * e.g.
 * original_url	noindex	canonical_page_id
 * https://www.amazon.es/s?k=+978-0-06-207348-8	noindex	k=978-0-06-207348-8
 * https://www.amazon.es/s?k=+bacopa+himalya	noindex	k=bacopa+himalaya
 * https://www.amazon.es/s?k=+correa+garmin+forerunner+910xt&i=sporting&rh=n%3A2454136031	noindex	k=correa+garmin+forerunner+910xt&rh=n%3A2454136031
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class ManualRequestFileGenerator {

    private static final Pattern KEYWORDS_PATTERN_1 = Pattern.compile("^k=(.+?)&");
    private static final Pattern KEYWORDS_PATTERN_2 = Pattern.compile("^k=(.+)");

    private final SparkSession sparkSession;

    public void generate(final String inputPath, final String outputPath, final long marketplaceId) {
        final Dataset<Row> inputDF = sparkSession.read()
                .option("header", true)
                .option("delimiter", "\t")
                .csv(inputPath)
                .distinct().cache();

        System.out.println("################## " + inputDF.count());

        final Dataset<ManualRequestData> renamedDS = inputDF.drop("original_url", "noindex")
                .withColumnRenamed("canonical_page_id", "page_id")
                .withColumn("marketplace_id", functions.lit(marketplaceId).cast(DataTypes.IntegerType))
                .withColumn("keywords", functions.lit(StringUtils.EMPTY).cast(DataTypes.StringType))
                .withColumn("transits", functions.lit(null).cast(DataTypes.IntegerType))
                .withColumn("ops", functions.lit(null).cast(DataTypes.DoubleType))
                .withColumn("index", functions.lit(1).cast(DataTypes.IntegerType))
                .withColumn("index_reason", functions.lit("MM Request(1)").cast(DataTypes.StringType))
                .as(Encoders.bean(ManualRequestData.class));

        // Set keywords
        final Dataset<ManualRequestData> outputDS = renamedDS.filter("page_id is not null")
                .map((MapFunction<ManualRequestData, ManualRequestData>) row -> {
                    final String pageId = row.getPage_id();
                    final Matcher matcher1 = KEYWORDS_PATTERN_1.matcher(pageId);
                    if (matcher1.find()) {
                        final String keywords = matcher1.group(1);
                        row.setKeywords(keywords);
                        return row;
                    }

                    final Matcher matcher2 = KEYWORDS_PATTERN_2.matcher(pageId);
                    if (matcher2.find()) {
                        final String keywords = matcher2.group(1);
                        row.setKeywords(keywords);
                        return row;
                    }
                    return row;
                }, Encoders.bean(ManualRequestData.class));


        /**
         * Since Spark 2.4, empty strings are saved as quoted empty strings "".
         * In version 2.3 and earlier, empty strings are equal to null values and do not reflect to any characters
         * in saved CSV files. For example, the row of "a", null, "", 1 was writted as a,,,1. Since Spark 2.4,
         * the same row is saved as a,,"",1. To restore the previous behavior, set the CSV option emptyValue to empty (not quoted) string.
         */
        outputDS.select("marketplace_id", "page_id", "keywords", "transits", "ops", "index", "index_reason")
                .distinct()
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", "\t")
                .option("emptyValue","") // Since Spark 2.4, empty strings are saved as quoted empty strings ""
                .csv(outputPath);

    }

    public static void main(String[] args) {
        final String inputPath = "/Users/jcsai/Downloads/My Project/keyword_indexation/index_override/2019-10-28/us-noindex.txt";
        final String outputPath = "/Users/jcsai/Downloads/My Project/keyword_indexation/index_override/2019-10-28/manual_request_override/us/";

        final SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        final ManualRequestFileGenerator generator = new ManualRequestFileGenerator(sparkSession);
        generator.generate(inputPath, outputPath, 1);

    }
}
