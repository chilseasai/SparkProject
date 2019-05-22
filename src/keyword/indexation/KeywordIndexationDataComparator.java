/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package keyword.indexation;

import keyword.indexation.datatype.KeywordIndexationData;
import keyword.indexation.datatype.OffensiveKeyword;
import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Compare keyword indexation data before and after
 * changing keyword matching algorithm.
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class KeywordIndexationDataComparator {

    private final SparkSession sparkSession;

    public void compare(final String oldInputPath, final String newInputPath, final String oldOnlyOutputPath, final String newOnlyOutputPath) {
        final Dataset<KeywordIndexationData> oldData = read(oldInputPath).filter((FilterFunction<KeywordIndexationData>) row -> row.getPage_id() != null);
        final Dataset<KeywordIndexationData> newData = read(newInputPath);

        // Compare all columns
        String[] oldArray = (String[]) oldData.sort("page_id").toDF().map((MapFunction<Row, String>) row -> row.mkString("\t"), Encoders.STRING()).collect();
        String[] newArray = (String[]) newData.sort("page_id").toDF().map((MapFunction<Row, String>) row -> row.mkString("\t"), Encoders.STRING()).collect();

        // Compare all columns except "trigger_keyword"
//        String[] oldArray = (String[]) oldData.sort("page_id").drop("trigger_keyword").toDF().map((MapFunction<Row, String>) row -> row.mkString("\t"), Encoders.STRING()).collect();
//        String[] newArray = (String[]) newData.sort("page_id").drop("trigger_keyword").toDF().map((MapFunction<Row, String>) row -> row.mkString("\t"), Encoders.STRING()).collect();

        int diffNum = 0;
        int len = Math.max(oldArray.length, newArray.length);
        for (int i = 0; i < len; i ++) {
            if (!oldArray[i].equals(newArray[i])) {
                System.out.println("###### old: " + oldArray[i] + "\n###### new: " + newArray[i]);
                diffNum ++;
            }
        }

        System.out.println("$$$$$$$$$$$$$$$ diffNum: " + diffNum);
    }

    public void compareLostOps(final String oldInputPath, final String newInputPath) {
        final Dataset<OffensiveKeyword> oldData = readOffensiveKW(oldInputPath);
        final Dataset<OffensiveKeyword> newData = readOffensiveKW(newInputPath);

        String[] oldArray = (String[]) oldData.sort("trigger_keyword").toDF().map((MapFunction<Row, String>) row -> row.mkString("\t"), Encoders.STRING()).collect();
        String[] newArray = (String[]) newData.sort("trigger_keyword").toDF().map((MapFunction<Row, String>) row -> row.mkString("\t"), Encoders.STRING()).collect();

        int diffNum = 0;
        int len = Math.max(oldArray.length, newArray.length);
        for (int i = 0; i < len; i ++) {
            if (!oldArray[i].equals(newArray[i])) {
                System.out.println("###### old: " + oldArray[i] + "\n###### new: " + newArray[i]);
                diffNum ++;
            }
        }

        System.out.println("$$$$$$$$$$$$$$$ diffNum: " + diffNum);
    }

    public Dataset<KeywordIndexationData> read(final String inputPath) {
        return sparkSession.read()
                .option("header", true)
                .option("delimiter", "\t")
                .option("quote", "")
                .option("inferSchema", true)
                .csv(inputPath)
                .distinct()
                .as(Encoders.bean(KeywordIndexationData.class));
    }

    public Dataset<OffensiveKeyword> readOffensiveKW(final String inputPath) {
        return sparkSession.read()
                .option("header", true)
                .option("delimiter", "\t")
                .option("quote", "")
                .option("inferSchema", true)
                .csv(inputPath)
                .distinct()
                .as(Encoders.bean(OffensiveKeyword.class));
    }

    public void write(final Dataset<KeywordIndexationData> data, final String outputPath) {
        data.coalesce(1)
                .write()
                .option("header", true)
                .option("delimiter", "\t")
                .option("quote", "")
                .csv(outputPath);
    }


    public static void main(String[] args) {
        final String oldInputPath = "/Users/jcsai/diff-indexation-data/old/4/";
        final String newInputPath = "/Users/jcsai/diff-indexation-data/new/4/";
        final String oldOutputPath = "/Users/jcsai/diff-indexation-data/output/old/4/";
        final String newOutputPath = "/Users/jcsai/diff-indexation-data/output/new/4/";

        final SparkSession sparkSession = SparkSession.builder().appName("aaa").master("local").getOrCreate();
        final KeywordIndexationDataComparator comparator = new KeywordIndexationDataComparator(sparkSession);
        comparator.compare(oldInputPath, newInputPath, oldOutputPath, newOutputPath);
//        comparator.compareLostOps(oldInputPath, newInputPath);
    }
}
