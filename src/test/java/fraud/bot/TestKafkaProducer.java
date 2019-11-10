package fraud.bot;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TestKafkaProducer {

    public Producer<Integer, String> createKafkaProducer () {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConst.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConst.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<Integer, String>(props);
    }

    public static void main(String[] args) {
        try {
            TestKafkaProducer testKafkaProducer = new TestKafkaProducer();
            Producer<Integer, String> producer = testKafkaProducer.createKafkaProducer();

            BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/noor.mazhar/repositories/fraudBotDetector/src/test/resources/ProjectDeveloperapacheaccesslog.txt"));
            int cnt = 0;
            String str = null;
            while ((str = bufferedReader.readLine()) != null) {
                try {
                    ProducerRecord<Integer, String> producerRecord =
                            new ProducerRecord<Integer, String>(IKafkaConst.TOPIC_NAME, str);
                    RecordMetadata recordMetadata = producer.send(producerRecord).get();
                    System.out.println("Record sent:: "  + " to partition:: " + recordMetadata.partition()
                            + " with offset:: " + recordMetadata.offset());

                    ++cnt;

//                    if (cnt == 1000) {
//                        break;
//                    }
                } catch (ExecutionException e1) {
                    e1.printStackTrace();
                } catch (InterruptedException e2) {
                    e2.printStackTrace();
                }
            }


        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
