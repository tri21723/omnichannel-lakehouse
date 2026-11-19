package omnichannel.stream.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Validates the event contract (ADR-0016 §4); anything else becomes a DLQ record with a reason. */
public final class EventParser {
    public static final Map<String, String> TOPIC_CHANNEL =
            Map.of(
                    "amazon.order-notifications", "amazon",
                    "tiktok_shop.order-webhooks", "tiktok_shop");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern RUN_ID = Pattern.compile("[0-9a-f]{20}");
    private static final Pattern AMOUNT = Pattern.compile("(\\d{1,15})\\.(\\d{2})");
    private static final Set<String> EVENT_TYPES = Set.of("order_created", "order_updated");

    private EventParser() {}

    public static StreamRecord parse(String topic, int partition, long offset, byte[] key, byte[] value) {
        StreamRecord r = new StreamRecord();
        r.topic = topic;
        r.partition = partition;
        r.offset = offset;
        r.key = key;
        r.raw = value;
        r.reason = validate(r, value, TOPIC_CHANNEL.get(topic));
        r.valid = r.reason == null;
        return r;
    }

    private static String validate(StreamRecord r, byte[] value, String expectedChannel) {
        JsonNode node;
        try {
            node = MAPPER.readTree(value == null ? "" : new String(value, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return "invalid_json";
        }
        if (node == null || !node.isObject()) {
            return "invalid_json";
        }
        if (node.path("schema_version").asInt(-1) != 1) {
            return "unknown_schema";
        }
        r.eventId = text(node, "event_id");
        r.eventType = text(node, "event_type");
        r.streamRunId = text(node, "stream_run_id");
        r.channel = text(node, "channel");
        r.orderId = text(node, "order_id");
        r.currency = text(node, "currency");
        if (r.eventId == null || r.orderId == null || r.currency == null) {
            return "missing_field";
        }
        if (r.eventType == null || !EVENT_TYPES.contains(r.eventType)) {
            return "bad_event_type";
        }
        if (r.streamRunId == null || !RUN_ID.matcher(r.streamRunId).matches()) {
            return "bad_stream_run_id";
        }
        if (r.channel == null || !r.channel.equals(expectedChannel)) {
            return "channel_topic_mismatch";
        }
        try {
            r.eventTsMs = Instant.parse(text(node, "event_ts")).toEpochMilli();
        } catch (DateTimeParseException | NullPointerException e) {
            return "bad_event_ts";
        }
        String amount = text(node, "amount");
        var m = amount == null ? null : AMOUNT.matcher(amount);
        if (m == null || !m.matches()) {
            return "bad_amount";
        }
        r.amountCents = Long.parseLong(m.group(1)) * 100 + Long.parseLong(m.group(2));
        return null;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            return null;
        }
        return value.asText();
    }
}
