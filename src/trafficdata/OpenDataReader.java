package trafficdata;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;

/**
 * OpenDataReader
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class OpenDataReader {
    private final SparkSession spark;

    private void readParquet() {
        spark.catalog().setCurrentDatabase("searchdata");
        spark.catalog().listTables().show();
        spark.sql("select page_type,marketplace_id,request_id,session,original_path,query_string,user_agent_id,is_robot,device_type_id,device_type,gmt_time from searchdata.tommy_searches where page_type in ('Search','SearchAW','Landing','LandingAW','Browse') AND partition_date == '20190101' AND org='CN'")
                .show();
    }

    public static void main(String[] args) {
        final SparkSession spark = SparkSession
                .builder()
                .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
                .config("spark.sql.hive.convertMetastoreParquet", false)
                .config("spark.sql.hive.metastorePartitionPruning", true)
                .enableHiveSupport()
                .getOrCreate();

        final OpenDataReader reader = new OpenDataReader(spark);
        reader.readParquet();
    }
}
