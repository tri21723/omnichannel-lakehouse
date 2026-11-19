package omnichannel.stream.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.jupiter.api.Test;

class FunctionsTest {
    static StreamRecord rec(String id, long eventTs, long cents) {
        StreamRecord r = new StreamRecord();
        r.valid = true;
        r.eventId = id;
        r.streamRunId = "0123456789abcdef0123";
        r.channel = "tiktok_shop";
        r.currency = "USD";
        r.eventTsMs = eventTs;
        r.amountCents = cents;
        return r;
    }

    static List<String> ids(Iterable<org.apache.flink.streaming.runtime.streamrecord.StreamRecord<StreamRecord>> out) {
        java.util.ArrayList<String> ids = new java.util.ArrayList<>();
        out.forEach(r -> ids.add(r.getValue().eventId));
        return ids;
    }

    /** Within the TTL a retry goes to DUPLICATES; after it the id passes again (ADR limitation). */
    @Test
    void dedupUsesProcessingTimeTtl() throws Exception {
        KeyedProcessOperator<String, StreamRecord, StreamRecord> op = new KeyedProcessOperator<>(new Functions.Dedup());
        try (var h = new KeyedOneInputStreamOperatorTestHarness<String, StreamRecord, StreamRecord>(
                op, r -> r.eventId, Types.STRING)) {
            h.open();
            h.setStateTtlProcessingTime(0);
            h.processElement(rec("e1", 1, 100), 1);
            h.processElement(rec("e2", 2, 100), 2);
            h.setStateTtlProcessingTime(Rules.DEDUP_TTL_MS - 1);
            h.processElement(rec("e1", 1, 100), 3);
            h.processElement(rec("e2", 2, 100), 4);
            h.setStateTtlProcessingTime(Rules.DEDUP_TTL_MS + 1);
            h.processElement(rec("e1", 1, 100), 5);
            List<String> passed = h.extractOutputValues().stream().map(r -> r.eventId).collect(Collectors.toList());
            assertEquals(List.of("e1", "e2", "e1"), passed);
            assertEquals(List.of("e1", "e2"), ids(h.getSideOutput(Functions.DUPLICATES)));
        }
    }

    /** Classification uses the operator watermark and too-late records leave the main output. */
    @Test
    void classifyRecordsWatermarkAndSplitsTooLate() throws Exception {
        long w = Rules.AMAZON_WINDOW_MS;
        long l = Rules.AMAZON_ALLOWED_LATENESS_MS;
        try (var h = new OneInputStreamOperatorTestHarness<>(new ProcessOperator<>(new Functions.Classify(w, l)))) {
            h.open();
            h.processWatermark(new Watermark(w - 2));
            h.processElement(rec("on", 0, 1), 0);
            h.processWatermark(new Watermark(w - 1));
            h.processElement(rec("late", 0, 1), 0);
            h.processWatermark(new Watermark(w - 1 + l));
            h.processElement(rec("too", 0, 1), 0);
            List<StreamRecord> main = h.extractOutputValues();
            assertEquals(List.of(Rules.ON_TIME, Rules.LATE_ACCEPTED), main.stream().map(r -> r.delayClass).collect(Collectors.toList()));
            assertEquals(w - 1, main.get(1).watermarkMs);
            assertEquals(List.of("too"), ids(h.getSideOutput(Functions.TOO_LATE)));
        }
    }

    @Test
    void aggregateCountsAndSumsCents() {
        Functions.SumAgg agg = new Functions.SumAgg();
        WindowAcc a = agg.add(rec("a", 0, 1234), agg.createAccumulator());
        a = agg.add(rec("b", 0, 66), a);
        WindowAcc b = agg.add(rec("c", 0, 100), agg.createAccumulator());
        WindowAcc merged = agg.merge(a, b);
        assertEquals(3, merged.count);
        assertEquals(1400, merged.cents);
        String line = OutRecord.window(merged, 0, 60_000, 1, 59_999).line;
        assertEquals(true, line.contains("\"amount\":\"14.00\""), line);
    }
}
