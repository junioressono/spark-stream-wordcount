import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

public class WordCountStreamTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountStreamTask.class);

    public static void main(String[] args) throws InterruptedException {
        new WordCountStreamTask().run(args[0], args[1]);
    }

    private void run(String hostname, String port) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName(WordCountStreamTask.class.getName())
                .setMaster("local");
        JavaStreamingContext spark = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaReceiverInputDStream<String> streamLines = spark.socketTextStream(hostname, Integer.parseInt(port));

        JavaDStream<String> streamWordsLowercase = streamLines.map(line -> line.toLowerCase());
        JavaDStream<String> streamWords = streamWordsLowercase.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> streamWordsTuples = streamWords.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> wordsCount = streamWordsTuples.reduceByKey((a, b) -> a + b);

        wordsCount.print();

        spark.start();
        spark.awaitTermination();
    }
}
