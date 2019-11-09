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

public class OptimizedBotDetector {
    Map<String, Deque<DateTime>> ip_dateTimeMap;
    DateTimeFormatter dateTimeFormatter;
    Map<String, AccessCnt> candidate_bots;
    Set<String> finalDetectedBots;

    public OptimizedBotDetector() {
        //<sourceIpAddr -> Deque<AccessTime>
        ip_dateTimeMap = new HashMap<>();
        dateTimeFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss");
        //set<sourceIpAddr>
        candidate_bots = new HashMap<>();
        finalDetectedBots = new HashSet<>();
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
        //how many windows particular ip addr exceeded good_threshold, if a particular ip addr exceeds threshold
        //in a lot of 1s windows then its most likely a bot
        int threshold_4_windows_cnt = 4;

        BufferedWriter bufferedWriter =
                new BufferedWriter(
                        new FileWriter("/Users/noor.mazhar/repositories/fraudBotDetector/src/test/resources/detectedBots"));
        int cnt = 0;
        System.out.println("START --> " +new DateTime());
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

            if (consumerRecords.count() == 0) {
                //no more records to process from Kafka
                break;
            }
            consumerRecords.forEach(record -> {
                System.out.println("value::" + record.value());
                String logRecord = record.value();
                String[] logRecordArry = logRecord.split(" ");
                String sourceIpAddr = logRecordArry[0];
                String tmp = logRecordArry[3];
                String logTS = tmp.substring(1, tmp.length());
                DateTime accessTime = dateTimeFormatter.parseDateTime(logTS);

                //only process that are not bots
                if (!finalDetectedBots.contains(sourceIpAddr)) {
                    if (!ip_dateTimeMap.containsKey(sourceIpAddr)) {
                        Deque<DateTime> logDateTimeList = new LinkedList<>();
                        logDateTimeList.addLast(accessTime);
                        ip_dateTimeMap.put(sourceIpAddr, logDateTimeList);
                    } else {
                        //check if time difference is greater than 1s, if yes then evict old entry
                        //if time delta is less than 1s, check list count, if list count > good_threshold then mark as bot
                        Deque<DateTime> tsList = ip_dateTimeMap.get(sourceIpAddr);
                        //do windowing checks to validate if number of hits to endpoint within 1s is within threshold
                        if (((accessTime.getMillis() - tsList.getFirst().getMillis()) > 1000)){
                            //beyond the 1s window, so evict, this keeps memory pressure low
                            ip_dateTimeMap.remove(sourceIpAddr);
                        } else if (((accessTime.getMillis() - tsList.getFirst().getMillis()) <= 1000)) {
                            if (tsList.size() >= good_threshold) {
                                //a probable bot detected capture it
                                if (candidate_bots.containsKey(sourceIpAddr)) {
                                    //check over a window of 1 to 2 minutes how many times speed threshold exceeded
                                    AccessCnt accessCnt = candidate_bots.get(sourceIpAddr);
                                    int newCnt = accessCnt.getCnt() + 1;
                                    accessCnt.setCnt(newCnt);
                                    if ((accessTime.getMillis() - accessCnt.getFirstAccessTime().getMillis()) > 60000) {
                                        //evict entry as we dont look beyond minute
                                        candidate_bots.remove(sourceIpAddr);
                                    } else {
                                        if (newCnt > threshold_4_windows_cnt) {
                                            //definitely a bot as its repeatedly doing too many hits
                                            try {
                                                finalDetectedBots.add(sourceIpAddr);
                                                bufferedWriter.write(sourceIpAddr);
                                                bufferedWriter.newLine();
                                                bufferedWriter.flush();
                                                ip_dateTimeMap.remove(sourceIpAddr);
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        } else {
                                            candidate_bots.put(sourceIpAddr, accessCnt);
                                        }
                                    }
                                } else {
                                    //a probable candidate for bot detected since within a 1s too many hits
                                    AccessCnt accessCnt = new AccessCnt();
                                    accessCnt.setCnt(1);
                                    accessCnt.setFirstAccessTime(accessTime);
                                    candidate_bots.put(sourceIpAddr, accessCnt);
                                }
                            } else {
                                //within the good threshold
                                tsList.addLast(accessTime);
                            }
                        }
                    }
                }

            });


            ++cnt;
            if (cnt == 1000) {
//                break;
            }
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
            OptimizedBotDetector botDetector = new OptimizedBotDetector() ;
            Consumer kafkaConsumer = botDetector.createConsumer();
            botDetector.performDDOSDetection(kafkaConsumer);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
