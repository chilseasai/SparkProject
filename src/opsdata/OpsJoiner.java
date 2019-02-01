package opsdata;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * OpsJoiner.
 *
 * Join keywords traffic data and free search channel ops data
 * to generate keywords page level ops data.
 *
 * Transform Redshift function to Spark SQL function.
 * 1. Upgrade Spark version to 2.3.1 or higher version，such as 2.4.0 to use date_trunc function.
 * from date_trunc('day',convert_timezone('UTC','MESZ', t.start_time))
 * to date_trunc('DAY',convert_timezone('UTC','MESZ', t.start_time))
 * 2. Replace Redshift function convert_timezone('UTC','MESZ', t.start_time)
 * to Spark function from_utc_timestamp(t.start_time, 'MESZ')
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class OpsJoiner {

    public static final String JOIN_QUERY = "WITH tommy AS (\n" +
            "\tSELECT * FROM qianyuez_kw\n" +
            "),\n" +
            "transits AS (\n" +
            "\tSELECT * FROM vadakkum_kw_otransits_de_1yr where first_viewed_page_day = '2018-07-26'\n" +
            "),\n" +
            "q AS \n" +
            "(\n" +
            "\tSELECT\n" +
            "\t\tt.marketplace_id, t.session_id, t.request_id, t.page_type, t.page_id, t.device_type, query_group_id, action_id,\n" +
            "\t\tt.start_time, t.keywords, t.refinement, ot.transit_event_id_click_id, \n" +
            "\t\tt.is_robot AS tommy_is_robot,\t\t\n" +
            "\t\trow_number() over(\tPARTITION BY t.marketplace_id, t.session_id, first_viewed_page_datetime, transit_event_id_click_id\n" +
            "\t\t\t\t\t\t\tORDER BY t.start_time, CASE WHEN request_id = transit_event_id_rid THEN 1 ELSE 2 END, action_id\n" +
            "\t\t\t\t\t\t) AS search_index,\n" +
            "\t\tot.traffic_channel_id, ot.is_robot, ot.is_internal,\n" +
            "\t\tot.transits, ot.one_view_transits, ot.glance_view_count, ot.units, ot.cart_add_ops, ot.ops\n" +
            "\tFROM tommy t\n" +
            "\t\tJOIN transits ot\n" +
            "\t\t\tON  t.marketplace_id = ot.marketplace_id\n" +
//            "\t\t\tAND date_trunc('DAY',convert_timezone('UTC','MESZ', t.start_time)) = ot.first_viewed_page_day\n" +
            "\t\t\tAND date_trunc('DAY',from_utc_timestamp(t.start_time, 'MESZ')) = ot.first_viewed_page_day\n" +
            "\t\t\tAND t.start_time = ot.first_viewed_page_day\n" +
            "\t\t\tAND t.session_id = ot.session_id\n" +
//            "\t\t\tAND (t.request_id = ot.transit_event_id_rid OR convert_timezone('UTC','MESZ', t.start_time) between ot.first_viewed_page_datetime AND ot.last_viewed_page_datetime)\n" +
            "\t\t\tAND (t.request_id = ot.transit_event_id_rid OR from_utc_timestamp(t.start_time,'MESZ') between ot.first_viewed_page_datetime AND ot.last_viewed_page_datetime)\n" +
            ")\n" +
            "SELECT\n" +
            "\tmarketplace_id, session_id, request_id, page_type, page_id, device_type, query_group_id, action_id,\n" +
            "\tstart_time, keywords, refinement,\n" +
            "\ttommy_is_robot,\t\t\n" +
            "\ttraffic_channel_id, is_robot, is_internal, transit_event_id_click_id, \n" +
            "\ttransits, one_view_transits, glance_view_count, units, cart_add_ops, ops\n" +
            "FROM q\n" +
            "WHERE search_index = 1";

    private final SparkSession sparkSession;

    private void join(final String channelOpsInputPath, final String trafficInputPath, final String outputPath) {
        final Dataset<Row> channelOpsDF = read(channelOpsInputPath);
        final Dataset<Row> trafficDF = read(trafficInputPath);

//        channelOpsDF.show(2);
//        trafficDF.show(2);

        channelOpsDF.createOrReplaceTempView("vadakkum_kw_otransits_de_1yr");
        trafficDF.createOrReplaceTempView("qianyuez_kw");

        final Dataset<Row> joinDF = sparkSession.sql(JOIN_QUERY);

        joinDF.show();
    }

    private Dataset<Row> read(final String inputPath) {
        return sparkSession.read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath);
    }

    private void write(final Dataset<Row> opsDF, final String outputPath) {
        opsDF.write()
                .option("delimiter", "\t")
                .option("head", true)
                .csv(outputPath);
    }

    public static void main(String[] args) {
        final String opsInputPath = "/Users/jcsai/Downloads/My Project/ops_data_provider/keywords/keywords_ops_data";
        final String trafficInputPath = "/Users/jcsai/Downloads/My Project/ops_data_provider/keywords/keywords_traffic_data";
        final String outputPath = "/Users/jcsai/Downloads/My Project/ops_data_provider/keywords/join_output";

        final SparkSession sparkSession = SparkSession.builder().appName("opsJoiner").master("local").getOrCreate();
        final OpsJoiner opsJoiner = new OpsJoiner(sparkSession);
        opsJoiner.join(opsInputPath, trafficInputPath, outputPath);
    }
}
