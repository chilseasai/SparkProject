package opsdata;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * OpsJoiner.
 *
 * Join keywords traffic data and free search channel ops data
 * to generate keywords page level ops data.
 *
 * Transform Redshift function to Spark SQL function.
 * 1. Upgrade Spark version to 2.3.0 or higher version，such as 2.4.0 to use date_trunc function.
 * from date_trunc('day',convert_timezone('UTC','MESZ', t.start_time))
 * to date_trunc('DAY',convert_timezone('UTC','MESZ', t.start_time))
 * 2. Replace Redshift function convert_timezone('UTC','MESZ', t.start_time)
 * to Spark function from_utc_timestamp(t.start_time, 'MESZ')
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class OpsJoiner {

    public static final String TEST_JOIN_SQL = "SELECT\n" +
            "\t\tt.marketplace_id, t.session_id, t.request_id, t.page_type, t.page_id, t.device_type, query_group_id, action_id,\n" +
            "\t\tt.start_time, t.keywords, t.refinement, ot.transit_event_id_click_id, \n" +
            "\t\tt.is_robot AS tommy_is_robot, \n" +
            "\t\trow_number() over(PARTITION BY t.marketplace_id, t.session_id, first_viewed_page_datetime, transit_event_id_click_id\n" +
            "\t\t\t\t\t\t\tORDER BY t.start_time, CASE WHEN request_id = transit_event_id_rid THEN 1 ELSE 2 END, action_id\n" +
            "\t\t\t\t\t\t) AS search_index,\n" +
            "\t\tot.traffic_channel_id, ot.is_robot, ot.is_internal,\n" +
            "\t\tot.transits, ot.one_view_transits, ot.glance_view_count, ot.units, ot.cart_add_ops, ot.ops\n" +
            "\tFROM qianyuez_kw t\n" +
            "\t\tJOIN vadakkum_kw_otransits_de_1yr ot\n" +
            "\t\t\tON  t.marketplace_id = ot.marketplace_id\n" +
            "\t\t\tAND date_trunc('DAY',from_utc_timestamp(t.start_time, 'Europe/Berlin')) = ot.first_viewed_page_day\n" +
            "\t\t\tAND t.session_id = ot.session_id\n" +
            "\t\t\tAND (t.request_id = ot.transit_event_id_rid OR from_utc_timestamp(t.start_time, 'Europe/Berlin') between ot.first_viewed_page_datetime AND ot.last_viewed_page_datetime)";

    public static final String TEST_JOIN_SQL_2 = "WITH tommy AS (\n" +
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
            "\t\tt.is_robot AS tommy_is_robot, \n" +
            "\t\trow_number() over(PARTITION BY t.marketplace_id, t.session_id, first_viewed_page_datetime, transit_event_id_click_id\n" +
            "\t\t\t\t\t\t\tORDER BY t.start_time, CASE WHEN request_id = transit_event_id_rid THEN 1 ELSE 2 END, action_id\n" +
            "\t\t\t\t\t\t) AS search_index,\n" +
            "\t\tot.traffic_channel_id, ot.is_robot, ot.is_internal,\n" +
            "\t\tot.transits, ot.one_view_transits, ot.glance_view_count, ot.units, ot.cart_add_ops, ot.ops\n" +
            "\tFROM tommy t\n" +
            "\t\tJOIN transits ot\n" +
            "\t\t\tON  t.marketplace_id = ot.marketplace_id\n" +
            "\t\t\tAND date_trunc('DAY',from_utc_timestamp(t.start_time, 'MESZ')) = ot.first_viewed_page_day\n" +
            "\t\t\tAND t.session_id = ot.session_id\n" +
            "\t\t\tAND (t.request_id = ot.transit_event_id_rid OR from_utc_timestamp(t.start_time, 'MESZ') between ot.first_viewed_page_datetime AND ot.last_viewed_page_datetime)\n" +
            ")\n" +
            "SELECT\n" +
            "\tmarketplace_id, session_id, request_id, page_type, page_id, device_type, query_group_id, action_id,\n" +
            "\tstart_time, keywords, refinement,\n" +
            "\ttommy_is_robot,\t\t\n" +
            "\ttraffic_channel_id, is_robot, is_internal, transit_event_id_click_id, \n" +
            "\ttransits, one_view_transits, glance_view_count, units, cart_add_ops, ops\n" +
            "FROM q\n" +
            "WHERE search_index = 1";

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
            "\t\t\tAND date_trunc('DAY',from_utc_timestamp(t.start_time, 'Europe/Berlin')) = ot.first_viewed_page_day\n" +
            "\t\t\tAND t.session_id = ot.session_id\n" +
            "\t\t\tAND (t.request_id = ot.transit_event_id_rid OR from_utc_timestamp(t.start_time,'Europe/Berlin') between ot.first_viewed_page_datetime AND ot.last_viewed_page_datetime)\n" +
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
        final Dataset<Row> channelOpsDF = readOPS(channelOpsInputPath).cache();
        final Dataset<Row> trafficDF = read(trafficInputPath).cache();

//        channelOpsDF.show(2);
//        trafficDF.show(2);
        System.out.println("channelOpsDF row number: " + channelOpsDF.count());
        System.out.println("trafficDF row number: " + trafficDF.count());

        channelOpsDF.createOrReplaceTempView("vadakkum_kw_otransits_de_1yr");
        trafficDF.createOrReplaceTempView("qianyuez_kw");

        final Dataset<Row> joinDF = sparkSession.sql(JOIN_QUERY);

        joinDF.show();
        System.out.println("joinDF row number: " + joinDF.count());
        write(joinDF, outputPath);

        sparkSession.catalog().clearCache();
    }

    private Dataset<Row> read(final String inputPath) {
        return sparkSession.read()
                .option("delimiter", "\t")
                .option("header", true)
                .option("inferSchema", true)
                .option("nullValue", "")
                .csv(inputPath);
    }

    private Dataset<Row> readOPS(final String inputPath) {
        final Dataset<Row> opsWithoutHeaderDF =  sparkSession.read()
                .option("delimiter", "|")
                .option("inferSchema", true)
                .csv(inputPath);
        return opsWithoutHeaderDF.toDF("marketplace_id",
                "first_viewed_page_day",
                "session_id",
                "transit_event_id_rid",
                "transit_event_id_click_id",
                "traffic_channel_id",
                "is_internal",
                "is_robot",
                "base_currency_code",
                "first_viewed_page_datetime",
                "last_viewed_page_datetime",
                "transits",
                "one_view_transits",
                "glance_view_count",
                "units",
                "cart_add_ops",
                "ops",
                "dw_create_date",
                "dw_update_date");
    }

    private void write(final Dataset<Row> opsDF, final String outputPath) {
        opsDF.write()
                .option("delimiter", "\t")
                .option("header", true)
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
//                .option("quote", "") // Make the double quotes not work
                .option("nullValue", "")
//                .option("quote", "\u0000")
//                .option("quoteAll", "false")
                .mode(SaveMode.Overwrite)
                .csv(outputPath);
    }

    public static void main(String[] args) {
//        final String opsInputPath = "/Users/jcsai/Downloads/My Project/ops_data_provider/keywords/keywords_ops_data";
//        final String opsInputPath = "/Users/jcsai/Downloads/My Project/ops_data_provider/keywords/keywords_ops_de_total";
//        final String trafficInputPath = "/Users/jcsai/Downloads/My Project/ops_data_provider/keywords/keywords_traffic_data";
//        final String outputPath = "/Users/jcsai/Downloads/My Project/ops_data_provider/keywords/join_output";

        final String opsInputPath = "s3://clickstream-full3/keywords_ops_data/2018-07-26/";
        final String trafficInputPath = "s3://clickstream-full3/keywords_traffic_data_header/2018-07-26/";
        final String outputPath = "s3://clickstream-full3/keywords_ops_join/2018-07-26/";

        final SparkSession sparkSession = SparkSession.builder().appName("opsJoiner").getOrCreate();
        final OpsJoiner opsJoiner = new OpsJoiner(sparkSession);
        opsJoiner.join(opsInputPath, trafficInputPath, outputPath);
    }
}
