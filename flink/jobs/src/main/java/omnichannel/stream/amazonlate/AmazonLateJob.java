package omnichannel.stream.amazonlate;

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
 * Amazon order notifications: late arrival (ADR-0016 §6).
 *
 * <p>Watermark: bounded out-of-orderness 2 min, idle timeout 1 min. Tumbling 5-min windows with
 * allowed lateness 20 min. Each record is classified against the watermark before the window:
 * on time / late but accepted go to `accepted` (and the window), too late go to the persisted
 * `late` branch. Malformed records go to the transactional DLQ. Nothing is dropped.
 */
public final class AmazonLateJob {
    static final String TOPIC = "amazon.order-notifications";

    private AmazonLateJob() {}

    public static void main(String[] argv) throws Exception {
        JobArgs args = JobArgs.parse(argv);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(args.checkpointMs, CheckpointingMode.EXACTLY_ONCE);

        SingleOutputStreamOperator<StreamRecord> valid =
                env.fromSource(Io.source(args, TOPIC, "flink-amazon-late"), Io.watermarks(), "amazon-source")
                        .uid("amazon-source")
                        .process(new Functions.SplitMalformed())
                        .uid("amazon-validate")
                        .name("validate");
        valid.getSideOutput(Functions.DLQ).sinkTo(Io.dlq(args, TOPIC, "amazon-late-dlq")).uid("amazon-dlq").name("dlq");

        SingleOutputStreamOperator<StreamRecord> accepted =
                valid.process(new Functions.Classify(Rules.AMAZON_WINDOW_MS, Rules.AMAZON_ALLOWED_LATENESS_MS))
                        .uid("amazon-classify")
                        .name("classify");

        DataStream<OutRecord> windows =
                accepted.keyBy(Functions::windowKey)
                        .window(TumblingEventTimeWindows.of(Duration.ofMillis(Rules.AMAZON_WINDOW_MS)))
                        .allowedLateness(Duration.ofMillis(Rules.AMAZON_ALLOWED_LATENESS_MS))
                        .aggregate(new Functions.SumAgg(), new Functions.WindowEmit())
                        .uid("amazon-window")
                        .name("window-5m");

        DataStream<OutRecord> lines =
                accepted.map(new Functions.ToEvent("accepted"))
                        .uid("amazon-accepted-lines")
                        .union(
                                accepted.getSideOutput(Functions.TOO_LATE)
                                        .map(new Functions.ToEvent("late"))
                                        .uid("amazon-late-lines"),
                                windows);
        lines.sinkTo(Io.files(args, "amazon")).uid("amazon-files").name("files");

        env.execute("amazon_late");
    }
}
