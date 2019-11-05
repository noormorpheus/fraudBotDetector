package fraud.bot;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TestKafkaConsumer {
    public Consumer<String, String> createConsumer () {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConst.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConst.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConst.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConst.OFFSET_RESET_EARLIER);

        Consumer<String, String> kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.subscribe(Collections.singletonList(IKafkaConst.TOPIC_NAME));

        return kafkaConsumer;

    }


    public static void main (String[] args) {
        try {
               TestKafkaConsumer testKafkaConsumer = new TestKafkaConsumer();
               Consumer kafkaConsumer = testKafkaConsumer.createConsumer();
               int noMessageFound = 0;
               final ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

    //               if (consumerRecords.count() == 0) {
    //                   break;
    //               }else {
    //                   continue;
    //               }

               consumerRecords.forEach(record -> {
                   System.out.println("Record Key " + record.key());
                   System.out.println("Record value " + record.value());
    //               System.out.println("Record partition " + record.partition());
    //               System.out.println("Record offset " + record.offset());
               });
                Thread.sleep(4000);
                kafkaConsumer.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
