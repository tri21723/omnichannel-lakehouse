package omnichannel.stream.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/** One output line plus the bucket (path under s3://raw/stream) it belongs to. */
public class OutRecord {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public String bucket;
    public String line;

    public OutRecord() {}

    public OutRecord(String bucket, String line) {
        this.bucket = bucket;
        this.line = line;
    }

    static String bucket(String channel, String branch, String runId, long tsMs) {
        String date = Instant.ofEpochMilli(tsMs).toString().substring(0, 10);
        return channel + "/" + branch + "/run=" + runId + "/date=" + date;
    }

    /** The original event plus processing metadata; the raw value stays byte-for-byte parseable. */
    public static OutRecord event(StreamRecord r, String branch) {
        try {
            ObjectNode node = (ObjectNode) MAPPER.readTree(new String(r.raw, StandardCharsets.UTF_8));
            node.put("_stream_branch", branch);
            if (r.delayClass != null) {
                node.put("_delay_class", r.delayClass);
            }
            node.put("_watermark_ms", r.watermarkMs);
            node.put("_kafka_topic", r.topic);
            node.put("_kafka_partition", r.partition);
            node.put("_kafka_offset", r.offset);
            node.put("_processed_ms", System.currentTimeMillis());
            return new OutRecord(bucket(r.channel, branch, r.streamRunId, r.eventTsMs), MAPPER.writeValueAsString(node));
        } catch (Exception e) {
            throw new IllegalStateException("validated record no longer parses", e);
        }
    }

    public static OutRecord window(WindowAcc acc, long start, long end, int firingSeq, long watermarkMs) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("_stream_branch", "windows");
        node.put("stream_run_id", acc.streamRunId);
        node.put("channel", acc.channel);
        node.put("currency", acc.currency);
        node.put("window_start", Instant.ofEpochMilli(start).toString());
        node.put("window_end", Instant.ofEpochMilli(end).toString());
        node.put("events", acc.count);
        node.put("amount", String.format("%d.%02d", acc.cents / 100, acc.cents % 100));
        node.put("firing_seq", firingSeq);
        node.put("_watermark_ms", watermarkMs);
        node.put("_processed_ms", System.currentTimeMillis());
        try {
            return new OutRecord(bucket(acc.channel, "windows", acc.streamRunId, start), MAPPER.writeValueAsString(node));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
