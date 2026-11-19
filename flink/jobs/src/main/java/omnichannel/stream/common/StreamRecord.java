package omnichannel.stream.common;

/** One Kafka record after validation (Flink POJO: public fields, no-arg constructor). */
public class StreamRecord {
    public boolean valid;
    public String reason;
    public String topic;
    public int partition;
    public long offset;
    public byte[] key;
    public byte[] raw;
    public String eventId;
    public String eventType;
    public String streamRunId;
    public String channel;
    public String orderId;
    public String currency;
    public long eventTsMs;
    public long amountCents;
    public long watermarkMs = Long.MIN_VALUE;
    public String delayClass;

    public StreamRecord() {}
}
