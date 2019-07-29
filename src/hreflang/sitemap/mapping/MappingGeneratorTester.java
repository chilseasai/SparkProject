/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package hreflang.sitemap.mapping;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;

/**
 * MappingGeneratorTester
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class MappingGeneratorTester implements Serializable {

    private final SparkSession sparkSession;

    private final LongAccumulator longAccumulator;

    public void test(final String inputPath) {
        final Dataset<HreflangData> hreflangDS = sparkSession.read().json(inputPath).as(Encoders.bean(HreflangData.class)).cache();

        hreflangDS.foreach((ForeachFunction<HreflangData>) row -> longAccumulator.add(1L));
        System.out.println("input data:" + longAccumulator.value());
        longAccumulator.reset();

        // ASIN consistency check
        final Dataset<HreflangData> asinConsistencyDS = hreflangDS.filter((FilterFunction<HreflangData>) row -> {
            final String locAsin = getASIN(row.getLoc());
            for (final AlternateData alternate : row.getLink()) {
                final String alternateAsin = getASIN(alternate.getHref());
                return locAsin.equals(alternateAsin);
            }
            return false;
        }).cache();

        asinConsistencyDS.foreach((ForeachFunction<HreflangData>) row -> longAccumulator.add(1L));
        System.out.println("asinConsistencyDS data:" + longAccumulator.value());
        longAccumulator.reset();
        hreflangDS.unpersist();

        final Dataset<AggCheckData> simplifiedDS = asinConsistencyDS.map((MapFunction<HreflangData, AggCheckData>) row ->
            AggCheckData.builder().id(getASIN(row.getLoc()))
                    .loc(row.getLoc())
                    .numOfAlter(row.getLink().length)
                    .build(),
            Encoders.bean(AggCheckData.class));

        final Dataset<Row> aggCheckDF = simplifiedDS.withColumn("numOfAlter2", functions.count("loc").over(Window.partitionBy("id")).cast(DataTypes.IntegerType))
                .filter((FilterFunction<Row>) row -> {
                    final int aggNum = row.getAs("numOfAlter2");
                    final int rawNum = row.getAs("numOfAlter");
                    return rawNum == aggNum;
                });

        aggCheckDF.foreach((ForeachFunction<Row>) row -> longAccumulator.add(1L));
        System.out.println("aggCheckDF data:" + longAccumulator.value());
    }

    private String getASIN(final String url) {
        final String splitContent[] = url.split("/");
        return splitContent[splitContent.length - 1];
    }

    public static void main(String[] args) {
//        final String inputPath = "/Users/jcsai/Downloads/My Project/hreflang_sitemap/QA/test data/part-00000-4deea686-4099-42c3-86c4-0967feaacc5d.c000.json";
//        final SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        final SparkSession sparkSession = SparkSession.builder().getOrCreate();
        final LongAccumulator longAccumulator = sparkSession.sparkContext().longAccumulator("mapping generator");

        new MappingGeneratorTester(sparkSession, longAccumulator).test(args[0]);
    }
}
