package omnichannel.stream.tiktokdedup;

import omnichannel.stream.common.Functions;
import omnichannel.stream.common.Io;
import omnichannel.stream.common.JobArgs;
import omnichannel.stream.common.OutRecord;
import omnichannel.stream.common.Rules;
import omnichannel.stream.common.StreamRecord;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import java.time.Duration;

/**
 * TikTok Shop order webhooks: retries and bursts (ADR-0016 §6).
 *
 * <p>keyBy(event_id) with a ValueState TTL of 30 min (processing time, OnCreateAndWrite, never
 * return expired): the first occurrence continues, retries go to the persisted `duplicates`
 * branch. Unique events are classified (window 1 min, no allowed lateness; anything late goes to
 * `late`, never dropped) and counted in tumbling 1-min windows. Malformed records go to the DLQ.
 */
public final class TiktokDedupJob {
    static final String TOPIC = "tiktok_shop.order-webhooks";

    private TiktokDedupJob() {}

    public static void main(String[] argv) throws Exception {
        JobArgs args = JobArgs.parse(argv);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(args.checkpointMs, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<StreamRecord> valid =
                env.fromSource(Io.source(args, TOPIC, "flink-tiktok-dedup"), Io.watermarks(), "tiktok-source")
                        .uid("tiktok-source")
                        .process(new Functions.SplitMalformed())
                        .uid("tiktok-validate")
                        .name("validate");
        valid.getSideOutput(Functions.DLQ).sinkTo(Io.dlq(args, TOPIC, "tiktok-dedup-dlq")).uid("tiktok-dlq").name("dlq");

        SingleOutputStreamOperator<StreamRecord> unique =
                valid.keyBy(r -> r.eventId).process(new Functions.Dedup()).uid("tiktok-dedup").name("dedup");

        SingleOutputStreamOperator<StreamRecord> accepted =
                unique.process(new Functions.Classify(Rules.TIKTOK_WINDOW_MS, Rules.TIKTOK_ALLOWED_LATENESS_MS))
                        .uid("tiktok-classify")
                        .name("classify");

        DataStream<OutRecord> windows =
                accepted.keyBy(Functions::windowKey)
                        .window(TumblingEventTimeWindows.of(Duration.ofMillis(Rules.TIKTOK_WINDOW_MS)))
                        .aggregate(new Functions.SumAgg(), new Functions.WindowEmit())
                        .uid("tiktok-window")
                        .name("window-1m");

        DataStream<OutRecord> lines =
                accepted.map(new Functions.ToEvent("accepted"))
                        .uid("tiktok-accepted-lines")
                        .union(
                                unique.getSideOutput(Functions.DUPLICATES)
                                        .map(new Functions.ToEvent("duplicates"))
                                        .uid("tiktok-duplicate-lines"),
                                accepted.getSideOutput(Functions.TOO_LATE)
                                        .map(new Functions.ToEvent("late"))
                                        .uid("tiktok-late-lines"),
                                windows);
        lines.sinkTo(Io.files(args, "tiktok")).uid("tiktok-files").name("files");

        env.execute("tiktok_dedup");
    }
}
