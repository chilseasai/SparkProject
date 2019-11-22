package sps.mrl.fee.calculation.onsite.ground;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.Serializable;

/**
 * OnsiteGroundFeeCalculator
 * Base on the rule below to calculate fees for Onsite Ground.
 *
 * Rule:
 * v1;USD;TaxExclusive
 * @ZonalDistribution    ;@SizeTier;@ShippingWeight    ;@Amount
 * 1.0     ;UsSmallStandardSize    ;<=10'oz    ;2.41
 * 1.0     ;UsSmallStandardSize    ;>10'oz    ;2.48
 *
 * Test data:
 * >>,Program,Surcharge,SizeTier,ShippingWeight,ShippingWeightUnit,ItemQuantity,Amount,Currency,Tax,ZonalDistribution
 * ,OnsiteGround,,UsSmallStandardSize,0.5,lb,1,2.41,USD,TaxExclusive,1
 * ,OnsiteGround,,UsSmallStandardSize,0.75,lb,1,2.48,USD,TaxExclusive,1
 * ,OnsiteGround,,UsSmallStandardSize,1,lb,1,2.48,USD,TaxExclusive,1
 * ,OnsiteGround,,UsSmallStandardSize,0.75,lb,1,2.48,USD,TaxExclusive,1
 * ,OnsiteGround,,UsSmallStandardSize,1,lb,1,2.48,USD,TaxExclusive,1
 *
 * @author Chilseasai@
 */
@RequiredArgsConstructor
public class OnsiteGroundFeeCalculator implements Serializable {

    private final SparkSession sparkSession;

    private void calculate(final String inputPath, final String outputPath) {
        final Dataset<Row> inputDf = sparkSession.read()
                .option("header", true)
                .option("delimiter", ",")
                .option("inferSchema", true)
                .csv(inputPath);

//        inputDf.show(false);

        final Dataset<TestCaseItem> itemDS = inputDf.drop(">>").as(Encoders.bean(TestCaseItem.class));
//        itemDS.printSchema();

//        System.out.println("################" + itemDS.count());
//        itemDS.show(false);
        final Dataset<TestCaseItem> updatedDS = itemDS.map((MapFunction<TestCaseItem, TestCaseItem>) row -> {
            final double amount = calculateAmount(row);
//            row.setAmount(Double.valueOf(String.format("%.3f", amount * row.getItemQuantity())));
            row.setAmount(Double.valueOf(String.format("%.3f", amount * row.getItemQuantity())));
            return row;
        }, Encoders.bean(TestCaseItem.class));

        updatedDS.withColumn(">>", functions.lit(""))
                .select(">>", "Program","Surcharge","SizeTier","ShippingWeight","ShippingWeightUnit","ItemQuantity","Amount","Currency","Tax","ZonalDistribution")
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", true)
                .option("delimiter", ",")
                .option("emptyValue","")
                .csv(outputPath);
    }

    private double calculateAmount(final TestCaseItem item) {
        final String zonal = String.valueOf(item.getZonalDistribution());
        switch (zonal) {
            case "2.0":
                return new Zonal2_0Calculator().getAmount(item);
            case "2.1":
                return new Zonal2_1Calculator().getAmount(item);
            case "2.2":
                return new Zonal2_2Calculator().getAmount(item);
            case "2.3":
                return new Zonal2_3Calculator().getAmount(item);
            case "2.4":
                return new Zonal2_4Calculator().getAmount(item);
            case "2.5":
                return new Zonal2_5Calculator().getAmount(item);
            case "2.6":
                return new Zonal2_6Calculator().getAmount(item);
            case "2.7":
                return new Zonal2_7Calculator().getAmount(item);
            case "2.8":
                return new Zonal2_8Calculator().getAmount(item);
            case "2.9":
                return new Zonal2_9Calculator().getAmount(item);
            case "3.0":
                return new Zonal3_0Calculator().getAmount(item);
            case "3.1":
                return new Zonal3_1Calculator().getAmount(item);
            case "3.2":
                return new Zonal3_2Calculator().getAmount(item);
            case "3.3":
                return new Zonal3_3Calculator().getAmount(item);
            case "3.4":
                return new Zonal3_4Calculator().getAmount(item);
            case "3.5":
                return new Zonal3_5Calculator().getAmount(item);
            case "3.6":
                return new Zonal3_6Calculator().getAmount(item);
            case "3.7":
                return new Zonal3_7Calculator().getAmount(item);
            case "3.8":
                return new Zonal3_8Calculator().getAmount(item);
            case "3.9":
                return new Zonal3_9Calculator().getAmount(item);
            case "4.0":
                return new Zonal4_0Calculator().getAmount(item);
            case "4.1":
                return new Zonal4_1Calculator().getAmount(item);
            case "4.2":
                return new Zonal4_2Calculator().getAmount(item);
            case "4.3":
                return new Zonal4_3Calculator().getAmount(item);
            case "4.4":
                return new Zonal4_4Calculator().getAmount(item);
            case "4.5":
                return new Zonal4_5Calculator().getAmount(item);
            case "4.6":
                return new Zonal4_6Calculator().getAmount(item);
            case "4.7":
                return new Zonal4_7Calculator().getAmount(item);
            case "4.8":
                return new Zonal4_8Calculator().getAmount(item);
            case "4.9":
                return new Zonal4_9Calculator().getAmount(item);
            case "5.0":
                return new Zonal5_0Calculator().getAmount(item);
            case "5.1":
                return new Zonal5_1Calculator().getAmount(item);
            case "5.2":
                return new Zonal5_2Calculator().getAmount(item);
            case "5.3":
                return new Zonal5_3Calculator().getAmount(item);
            case "5.4":
                return new Zonal5_4Calculator().getAmount(item);
            case "5.5":
                return new Zonal5_5Calculator().getAmount(item);
            case "5.6":
                return new Zonal5_6Calculator().getAmount(item);
            case "5.7":
                return new Zonal5_7Calculator().getAmount(item);
            case "5.8":
                return new Zonal5_8Calculator().getAmount(item);
            case "5.9":
                return new Zonal5_9Calculator().getAmount(item);
            case "6.0":
                return new Zonal6_0Calculator().getAmount(item);
            case "6.1":
                return new Zonal6_1Calculator().getAmount(item);
            case "6.2":
                return new Zonal6_2Calculator().getAmount(item);
            case "6.3":
                return new Zonal6_3Calculator().getAmount(item);
            case "6.4":
                return new Zonal6_4Calculator().getAmount(item);
            case "6.5":
                return new Zonal6_5Calculator().getAmount(item);
            case "6.6":
                return new Zonal6_6Calculator().getAmount(item);
            case "6.7":
                return new Zonal6_7Calculator().getAmount(item);
            case "6.8":
                return new Zonal6_8Calculator().getAmount(item);
            case "6.9":
                return new Zonal6_9Calculator().getAmount(item);
            case "7.0":
                return new Zonal7_0Calculator().getAmount(item);
            case "7.1":
                return new Zonal7_1Calculator().getAmount(item);
            case "7.2":
                return new Zonal7_2Calculator().getAmount(item);
            case "7.3":
                return new Zonal7_3Calculator().getAmount(item);
            case "7.4":
                return new Zonal7_4Calculator().getAmount(item);
            case "7.5":
                return new Zonal7_5Calculator().getAmount(item);
            case "7.6":
                return new Zonal7_6Calculator().getAmount(item);
            case "7.7":
                return new Zonal7_7Calculator().getAmount(item);
            case "7.8":
                return new Zonal7_8Calculator().getAmount(item);
            case "7.9":
                return new Zonal7_9Calculator().getAmount(item);
            case "8.0":
                return new Zonal8_0Calculator().getAmount(item);
            default:
                return item.getAmount();
        }
    }

    public static void main(String[] args) {
        final String inputPath = "/Users/jcsai/Downloads/OnsiteGround.csv";
        final String outputPath = "/Users/jcsai/Downloads/OnsiteGround-output-full";

        final SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        final OnsiteGroundFeeCalculator calculator = new OnsiteGroundFeeCalculator(sparkSession);
        calculator.calculate(inputPath, outputPath);
    }
}
