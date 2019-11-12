package fraud.bot;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class SparkBotDetector {

    public static void main (String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("SparkBotDetector");
        //70 seconds batch interval
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(70));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(IKafkaConst.TOPIC_NAME.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", IKafkaConst.KAFKA_BROKERS);

        //Create direct Kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                javaStreamingContext,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
        JavaDStream<String> lines = messages.map(Tuple2::_2);
        
    }
}
