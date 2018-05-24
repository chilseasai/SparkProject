package exceptionTest;

import com.google.common.base.Joiner;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * BrowseNodeIdException
 *
 * @author cn-seo-dev@
 */
public class BrowseNodeIdException {

    private void addESVColumn(String inputPath, String outputPath) {
        SparkSession sparkSession = SparkSession.builder().appName("test").getOrCreate();

//        List<StructField> fields = new ArrayList<>();
//
//        fields.add(DataTypes.createStructField("marketplace_id", DataTypes.LongType, true));
//        fields.add(DataTypes.createStructField("page_id", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("refinement_picker_value", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("refinement_short_id", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("refinement_name", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("browse_node_id", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("refinement_picker_node_id", DataTypes.StringType, true));
//
//        StructType schema = DataTypes.createStructType(fields);



        Dataset<R1Meta> result = sparkSession.read()
                .option("delimiter", "\t")
                .option("header", true)
//                .option("inferSchema", true)
//                .schema(schema)
                .csv(inputPath)
//                .drop("create_date")
                .as(Encoders.bean(R1Meta.class));

        result.printSchema();
        System.out.println("Counter ====== " + result.count());
        System.out.println("Distinct Counter ====== " + result.distinct().count());

        result.foreach((ForeachFunction<R1Meta>) row -> {
            System.out.println(row.getPage_id());
        });

//        result.foreach((ForeachFunction<Row>) row ->{
//                    int browseNodeIdIndex = row.fieldIndex("browseNodeId");
////                    try {
//                    int browseNodeId = (int) row.get(browseNodeIdIndex);
//                        System.out.println("Correct browseNodeId: " + browseNodeId);
////                    } catch (Exception e) {
////                        System.out.println("Exception browseNodeId: " + row.mkString(","));
////                    }
//                });
    }

    public static void main(String[] args) {
        String inputPath = "testdata/r1_meta";
//        String inputPath = "/Users/jcsai/Downloads/My Project/indexation_data_generator/S-R1M-Discount.txt";
        String outputPath = "/Users/jcsai/Downloads/My Project/indexation_data_generator/esv_candidate_2_esv/esv-data-test";

//        new BrowseNodeIdException().addESVColumn(inputPath, outputPath);

        Long marketplaceId = null;
        String str = Joiner.on(",").join(marketplaceId == null ? "" : marketplaceId, 2, "3");
        System.out.println("d###" + str);
    }
}
