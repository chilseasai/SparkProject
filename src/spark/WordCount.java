package spark;

import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * WordCount
 *
 * @author cn-seo-dev@
 */
@RequiredArgsConstructor
public class WordCount {

    private final JavaSparkContext sc;

    public void count(final String inputPath, final String outputPath) {
        JavaRDD<String> textFile = sc.textFile(inputPath);
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile(outputPath);
    }

    public static void main(String[] args) {
        final SparkConf sparkConf = new SparkConf().setAppName("wordCount");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        final String inputPath = "/Users/jcsai/IdeaProjects/SparkProject/testdata/wordcount/input";
//        final String outputPath = "/Users/jcsai/IdeaProjects/SparkProject/testdata/wordcount/output";

        final String inputPath = args[0];
        final String outputPath = args[1];

        final WordCount wordCount = new WordCount(sc);
        wordCount.count(inputPath, outputPath);
    }
}
