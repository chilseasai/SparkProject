/**
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */
package sabledata;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

/**
 * SableDataUpdater
 * Replace Sable data status from "add" to "del" for cleaning up Sable.
 * Before:
 * 'com.amazon.seo.indexation@1.0'::{key:"I_35691_Search_c11e6866f0ed334328879a94b2a3b1ed7a36403b76dfd698d4af20a5a881cf99",status:"add",value:"{upperPageID:\"4327765031\",upperPageType:\"Browse\",isIndexed:0,useCanonicalTag:1}"}
 * After:
 * 'com.amazon.seo.indexation@1.0'::{key:"I_35691_Search_c11e6866f0ed334328879a94b2a3b1ed7a36403b76dfd698d4af20a5a881cf99",status:"del",value:"{upperPageID:\"4327765031\",upperPageType:\"Browse\",isIndexed:0,useCanonicalTag:1}"}
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class SableDataUpdater {

    private final SparkSession sparkSession;

    public void replaceAddToDel(final String inputPath, final String outputPath) {
        final Dataset<String> inputDS = sparkSession.read().option("compression", "gzip").textFile(inputPath);
//        inputDS.show(1, false);

        inputDS.map((MapFunction<String, String>) row ->
                row.replace("status:\"add\"", "status:\"del\""), Encoders.STRING())
                .write().option("compression", "gzip").text(outputPath);

    }

    public static void main(String[] args) {
//        final String inputPath = "/Users/jcsai/indexation-data-test/sable-uploader-cleanup/35691-I-part-00000.gz";
//        final String outputPath = "/Users/jcsai/indexation-data-test/sable-uploader-cleanup/35691-I-part-00000-del.gz";

        final String inputPath = args[0];
        final String outputPath = args[1];

//        final SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        final SparkSession sparkSession = SparkSession.builder().getOrCreate();
        final SableDataUpdater updater = new SableDataUpdater(sparkSession);
        updater.replaceAddToDel(inputPath, outputPath);
    }
}
