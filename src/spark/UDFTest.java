package spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * UDFTest
 *
 * User Defined Functions, or UDF, define functions that perform specific tasks within a larger system.
 * Often used in SQL databases, UDFs provide a mechanism for extending the functionality of the database
 * server by adding a function that can be evaluated in SQL statements.
 *
 * Output:
 * +---+----------+
 * |id |id_squared|
 * +---+----------+
 * |1  |1         |
 * |2  |4         |
 * |3  |9         |
 * |4  |16        |
 * |5  |25        |
 * |6  |36        |
 * |7  |49        |
 * |8  |64        |
 * |9  |81        |
 * |10 |100       |
 * |11 |121       |
 * |12 |144       |
 * |13 |169       |
 * |14 |196       |
 * |15 |225       |
 * |16 |256       |
 * |17 |289       |
 * |18 |324       |
 * |19 |361       |
 * +---+----------+
 *
 * @author Chilseasai@
 */
public class UDFTest {

    private void testUDF(final SparkSession sparkSession) {
        // Register a function as a UDF
        sparkSession.udf().register("squared", (UDF1<Long, Long>) input -> input * input, DataTypes.LongType);

        // Call the UDF in Spark SQL
        sparkSession.range(1, 20).createOrReplaceTempView("ids");
        sparkSession.sql("select id, squared(id) as id_squared from ids").show(false);
    }

    public static void main(String[] args) {
        final SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        new UDFTest().testUDF(sparkSession);
    }
}
