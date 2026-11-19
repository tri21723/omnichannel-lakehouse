package omnichannel.stream.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RulesTest {
    static final long MIN = 60_000L;

    @Test
    void boundariesFollowFlinkWindowSemantics() {
        long w = Rules.AMAZON_WINDOW_MS;
        long l = Rules.AMAZON_ALLOWED_LATENESS_MS;
        long maxTs = w - 1; // window [0, 5 min)
        assertEquals(Rules.ON_TIME, Rules.classify(0, maxTs - 1, w, l));
        assertEquals(Rules.LATE_ACCEPTED, Rules.classify(0, maxTs, w, l));
        assertEquals(Rules.LATE_ACCEPTED, Rules.classify(0, maxTs + l - 1, w, l));
        assertEquals(Rules.TOO_LATE, Rules.classify(0, maxTs + l, w, l));
        assertEquals(Rules.ON_TIME, Rules.classify(0, Long.MIN_VALUE, w, l));
        assertEquals(Rules.TOO_LATE, Rules.classify(0, maxTs, w, 0));
    }

    /** Generator delay classes (stream-v2) stay in their class for every offset in the window and
     * a watermark up to 239 s behind the ideal one (ADR-0016 §6: 4-minute margins). */
    @Test
    void generatorDelayClassesAreRobustToWatermarkLag() {
        long w = Rules.AMAZON_WINDOW_MS;
        long l = Rules.AMAZON_ALLOWED_LATENESS_MS;
        long[][] classes = {{0, MIN}, {11 * MIN, 16 * MIN}, {31 * MIN, 60 * MIN}};
        String[] expected = {Rules.ON_TIME, Rules.LATE_ACCEPTED, Rules.TOO_LATE};
        for (int c = 0; c < classes.length; c++) {
            for (long delay : classes[c]) {
                for (long offset = 0; offset < w; offset += 997) {
                    for (long lag : new long[] {0, 60_000, 180_000, 239_000}) {
                        long eventTs = 1_769_904_000_000L + offset;
                        long watermark = eventTs + delay - Rules.WATERMARK_BOUND_MS - lag;
                        String got = Rules.classify(eventTs, watermark, w, l);
                        assertEquals(expected[c], got, "delay=" + delay + " offset=" + offset + " lag=" + lag);
                    }
                }
            }
        }
    }

    @Test
    void tiktokOnTimeEventsAreNeverLate() {
        for (long offset = 0; offset < Rules.TIKTOK_WINDOW_MS; offset += 101) {
            long eventTs = 1_769_904_000_000L + offset;
            long watermark = eventTs + MIN - Rules.WATERMARK_BOUND_MS;
            assertEquals(Rules.ON_TIME, Rules.classify(eventTs, watermark, Rules.TIKTOK_WINDOW_MS, 0));
        }
    }
}
