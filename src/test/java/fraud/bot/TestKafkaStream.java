package fraud.bot;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
//import org.apache.kafka.test.TestUtils;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class TestKafkaStream {

    private void createDDOSDetectStream (final StreamsBuilder builder) {
        final KStream<String, String> logLines = builder.stream(IKafkaConst.TOPIC_NAME);
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KTable<String, Long> wordCounts =
                logLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((ignoredKey, word) -> word)
                .count();

        wordCounts.toStream().to(IKafkaConst.OTPT_TOPIC_NAME, Produced.with(Serdes.String(), Serdes.Long()));
    }


    public Properties getStreamsConfiguration () {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "ddos_detector");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "ddos_detector_client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConst.KAFKA_BROKERS);
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        // For illustrative purposes we disable record caches.
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Use a temporary directory for storing state, which will be automatically removed after the test.
//        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        return streamsConfiguration;
    }

    public static void main(String[] args) {
        try {
            TestKafkaStream testKafkaStream = new TestKafkaStream();
            final Properties streamsConfiguration = testKafkaStream.getStreamsConfiguration();
            final StreamsBuilder streamsBuilder = new StreamsBuilder();
            testKafkaStream.createDDOSDetectStream(streamsBuilder);
            final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);
            streams.cleanUp();
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams :: close));

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
