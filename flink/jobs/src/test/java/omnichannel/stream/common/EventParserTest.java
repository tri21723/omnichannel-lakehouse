package omnichannel.stream.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class EventParserTest {
    static final String TOPIC = "amazon.order-notifications";
    static final String VALID =
            "{\"schema_version\":1,\"event_id\":\"e1\",\"event_type\":\"order_created\","
                    + "\"stream_run_id\":\"0123456789abcdef0123\",\"channel\":\"amazon\",\"order_id\":\"o1\","
                    + "\"event_ts\":\"2026-02-01T00:00:01.500Z\",\"currency\":\"USD\",\"amount\":\"12.34\","
                    + "\"payload\":{},\"emitted_ts\":\"2026-11-19T00:00:00.000Z\"}";

    static StreamRecord parse(String value) {
        return EventParser.parse(TOPIC, 2, 7L, "k".getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void validEventIsParsedWithCentsAndMillis() {
        StreamRecord r = parse(VALID);
        assertTrue(r.valid);
        assertNull(r.reason);
        assertEquals(1234L, r.amountCents);
        assertEquals(1_769_904_001_500L, r.eventTsMs);
        assertEquals(2, r.partition);
        assertEquals(7L, r.offset);
    }

    @Test
    void contractViolationsGetAReason() {
        String[][] cases = {
            {"{\"schema_version\":1,\"event_id\":\"e1\"", "invalid_json"},
            {VALID.replace("\"schema_version\":1", "\"schema_version\":99"), "unknown_schema"},
            {VALID.replace("\"event_id\":\"e1\",", ""), "missing_field"},
            {VALID.replace("2026-02-01T00:00:01.500Z", "not-a-timestamp"), "bad_event_ts"},
            {VALID.replace("\"channel\":\"amazon\"", "\"channel\":\"tiktok_shop\""), "channel_topic_mismatch"},
            {VALID.replace("\"12.34\"", "12.34"), "bad_amount"},
            {VALID.replace("order_created", "order_deleted"), "bad_event_type"},
            {VALID.replace("0123456789abcdef0123", "RUN"), "bad_stream_run_id"},
        };
        for (String[] c : cases) {
            StreamRecord r = parse(c[0]);
            assertFalse(r.valid, c[1]);
            assertEquals(c[1], r.reason);
        }
    }
}
