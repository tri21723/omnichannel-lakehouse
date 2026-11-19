package omnichannel.stream.common;

/**
 * Event-time rules shared with the generator (generators/stream/config.py, ADR-0016 §6).
 *
 * <p>Boundaries follow Flink windowing exactly: a tumbling window [start, end) has maxTimestamp
 * end - 1; it fires when watermark >= maxTimestamp and drops an element when maxTimestamp +
 * allowedLateness <= watermark. Classifying with any other boundary would let the window operator
 * silently drop an element this class called accepted.
 */
public final class Rules {
    public static final long WATERMARK_BOUND_MS = 2 * 60_000L;
    public static final long IDLE_TIMEOUT_MS = 60_000L;
    public static final long AMAZON_WINDOW_MS = 5 * 60_000L;
    public static final long TIKTOK_WINDOW_MS = 60_000L;
    public static final long AMAZON_ALLOWED_LATENESS_MS = 20 * 60_000L;
    public static final long TIKTOK_ALLOWED_LATENESS_MS = 0L;
    public static final long DEDUP_TTL_MS = 30 * 60_000L;

    public static final String ON_TIME = "on_time";
    public static final String LATE_ACCEPTED = "late_accepted";
    public static final String TOO_LATE = "too_late";

    private Rules() {}

    public static long windowStart(long eventTsMs, long windowMs) {
        return eventTsMs - Math.floorMod(eventTsMs, windowMs);
    }

    public static String classify(long eventTsMs, long watermarkMs, long windowMs, long latenessMs) {
        long maxTimestamp = windowStart(eventTsMs, windowMs) + windowMs - 1;
        if (watermarkMs < maxTimestamp) {
            return ON_TIME;
        }
        if (watermarkMs < maxTimestamp + latenessMs) {
            return LATE_ACCEPTED;
        }
        return TOO_LATE;
    }
}
