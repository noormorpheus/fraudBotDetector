package fraud.bot;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class BotDetector {
    Map<String, Deque<DateTime>> ip_dateTimeMap;
    DateTimeFormatter dateTimeFormatter;
    Set<String> detected_bots;

    public BotDetector () {
        //<sourceIpAddr -> Deque<AccessTime>
        ip_dateTimeMap = new HashMap<>();
        dateTimeFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss");
        //set<sourceIpAddr>
        detected_bots = new HashSet<>();
    }

    public Consumer<Integer, String> createConsumer () {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConst.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConst.GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConst.MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConst.OFFSET_RESET_EARLIER);

        Consumer<Integer, String> kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.subscribe(Collections.singletonList(IKafkaConst.TOPIC_NAME));

        return kafkaConsumer;

    }

    private void performDDOSDetection (Consumer kafkaConsumer) throws Exception {
        int good_threshold  = 6;

        BufferedWriter bufferedWriter =
                new BufferedWriter(
                        new FileWriter("/Users/noor.mazhar/repositories/fraudBotDetector/src/test/resources/detectedBots"));
        int cnt = 0;
        System.out.println("START --> " +new DateTime());
        int cnt_no_record_fetched = 0;
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

            if (consumerRecords.count() == 0) {
                ++cnt_no_record_fetched;
                //no more records to process from Kafka
                if (cnt_no_record_fetched == 5){
                    break;
                } else {
                    continue;
                }
            }
            Thread.sleep(5000);
            consumerRecords.forEach(record -> {
                System.out.println("value::" + record.value());
                String logRecord = record.value();
                String[] logRecordArry = logRecord.split(" ");
                String sourceIpAddr = logRecordArry[0];
                String tmp = logRecordArry[3];
                String logTS = tmp.substring(1, tmp.length());
                DateTime accessTime = dateTimeFormatter.parseDateTime(logTS);

                //only process that are not bots
                if (!detected_bots.contains(sourceIpAddr)) {
                    if (!ip_dateTimeMap.containsKey(sourceIpAddr)) {
                        Deque<DateTime> logDateTimeList = new LinkedList<>();
                        logDateTimeList.addLast(accessTime);
                        ip_dateTimeMap.put(sourceIpAddr, logDateTimeList);
                    } else {
                        //check if time difference is greater than 2 s, if yes then evict old entry
                        //if time delta is less than 2 s, check list count, if list count > 20 then mark as bot
                        Deque<DateTime> tsList = ip_dateTimeMap.get(sourceIpAddr);
                        //do windowing checks to validate if number of hits to endpoint within 1s is within threshold
                        if (((accessTime.getMillis() - tsList.getFirst().getMillis()) > 1000)
                                && tsList.size() < good_threshold){
                            //beyond the 1s window, so evict
                            ip_dateTimeMap.remove(sourceIpAddr);
                        } else if (((accessTime.getMillis() - tsList.getFirst().getMillis()) <= 1000)) {
                            if (tsList.size() >= good_threshold) {
                                //a bot detected capture it
                                detected_bots.add(sourceIpAddr);
                                try {
                                    bufferedWriter.write(sourceIpAddr);
                                    bufferedWriter.newLine();
                                    bufferedWriter.flush();
                                    ip_dateTimeMap.remove(sourceIpAddr);
                                }catch (IOException e) {
                                    e.printStackTrace();
                                }
                            } else {
                                tsList.addLast(accessTime);
                            }
                        }
                    }
                }
            });

        }
        System.out.println("END --> " +new DateTime());
        Thread.sleep(4000);
        kafkaConsumer.close();
    }

    /**
     * Main entry to the application
     * @param args
     */
    public static void main (String[] args) {
        try {
            BotDetector botDetector = new BotDetector () ;
            Consumer kafkaConsumer = botDetector.createConsumer();
            botDetector.performDDOSDetection(kafkaConsumer);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
