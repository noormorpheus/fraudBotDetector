package fraud.bot;

public interface IKafkaConst {

    public static String OFFSET_RESET_LATEST="latest";
    public static String OFFSET_RESET_EARLIER="earliest";
    public static String KAFKA_BROKERS = "phs0v-hdprsm01.drivecam.net:9092";
    public static Integer MESSAGE_COUNT=10000;
    public static String CLIENT_ID="streamClient";
    public static String TOPIC_NAME="inptApacheLog";
    public static String OTPT_TOPIC_NAME="OtptTstJava";
    public static String GROUP_ID_CONFIG="group2";
    public static String STREAMING_GROUP_ID_CONFIG="stream_group1";
    public static Integer MAX_POLL_RECORDS=10;

}
