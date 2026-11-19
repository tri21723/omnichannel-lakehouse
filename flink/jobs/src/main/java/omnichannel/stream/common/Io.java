package omnichannel.stream.common;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Sources and sinks (ADR-0016 §3, §7). */
public final class Io {
    private Io() {}

    public static KafkaSource<StreamRecord> source(JobArgs args, String topic, String groupId) {
        return KafkaSource.<StreamRecord>builder()
                .setBootstrapServers(args.bootstrap)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(startingOffsets(args.start))
                .setDeserializer(new Deserializer())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("isolation.level", "read_committed")
                .build();
    }

    static OffsetsInitializer startingOffsets(String start) {
        switch (start) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            default:
                return OffsetsInitializer.timestamp(Long.parseLong(start));
        }
    }

    /** Per-split bounded out-of-orderness; malformed records never advance the watermark. */
    public static WatermarkStrategy<StreamRecord> watermarks() {
        return WatermarkStrategy.<StreamRecord>forBoundedOutOfOrderness(Duration.ofMillis(Rules.WATERMARK_BOUND_MS))
                .withTimestampAssigner((r, kafkaTs) -> r.valid ? r.eventTsMs : Long.MIN_VALUE)
                .withIdleness(Duration.ofMillis(Rules.IDLE_TIMEOUT_MS));
    }

    public static FileSink<OutRecord> files(JobArgs args, String partPrefix) {
        return FileSink.forRowFormat(new Path(args.output), new LineEncoder())
                .withBucketAssigner(new BranchBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(
                        OutputFileConfig.builder().withPartPrefix(partPrefix).withPartSuffix(".jsonl").build())
                .build();
    }

    /** Transactional (exactly-once) DLQ: original key and bytes, read with read_committed. */
    public static KafkaSink<StreamRecord> dlq(JobArgs args, String topic, String txPrefix) {
        return KafkaSink.<StreamRecord>builder()
                .setBootstrapServers(args.bootstrap)
                .setRecordSerializer(new DlqSerializer(topic + ".dlq"))
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(txPrefix)
                .setProperty("transaction.timeout.ms", "600000")
                .build();
    }

    static class Deserializer implements KafkaRecordDeserializationSchema<StreamRecord> {
        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<StreamRecord> out) {
            out.collect(EventParser.parse(record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }

        @Override
        public TypeInformation<StreamRecord> getProducedType() {
            return TypeInformation.of(StreamRecord.class);
        }
    }

    static class DlqSerializer implements KafkaRecordSerializationSchema<StreamRecord> {
        private final String topic;

        DlqSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(StreamRecord r, KafkaSinkContext ctx, Long timestamp) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, r.key, r.raw);
            record.headers().add("dlq_reason", r.reason.getBytes(StandardCharsets.UTF_8));
            record.headers().add("source_offset", (r.partition + ":" + r.offset).getBytes(StandardCharsets.UTF_8));
            return record;
        }
    }

    static class LineEncoder implements Encoder<OutRecord> {
        @Override
        public void encode(OutRecord element, java.io.OutputStream stream) throws java.io.IOException {
            stream.write(element.line.getBytes(StandardCharsets.UTF_8));
            stream.write('\n');
        }
    }

    static class BranchBucketAssigner implements BucketAssigner<OutRecord, String> {
        @Override
        public String getBucketId(OutRecord element, Context context) {
            return element.bucket;
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
