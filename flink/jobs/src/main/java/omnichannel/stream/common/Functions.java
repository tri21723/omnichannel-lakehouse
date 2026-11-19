package omnichannel.stream.common;

import java.time.Duration;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Operators shared by both jobs. */
public final class Functions {
    public static final OutputTag<StreamRecord> DLQ = new OutputTag<>("dlq") {};
    public static final OutputTag<StreamRecord> TOO_LATE = new OutputTag<>("too-late") {};
    public static final OutputTag<StreamRecord> DUPLICATES = new OutputTag<>("duplicates") {};

    private Functions() {}

    /** Malformed records go to the DLQ side output; nothing is dropped. */
    public static class SplitMalformed extends ProcessFunction<StreamRecord, StreamRecord> {
        @Override
        public void processElement(StreamRecord r, Context ctx, Collector<StreamRecord> out) {
            if (r.valid) {
                out.collect(r);
            } else {
                ctx.output(DLQ, r);
            }
        }
    }

    /** Classifies against the current watermark before windowing and records that watermark. */
    public static class Classify extends ProcessFunction<StreamRecord, StreamRecord> {
        private final long windowMs;
        private final long latenessMs;

        public Classify(long windowMs, long latenessMs) {
            this.windowMs = windowMs;
            this.latenessMs = latenessMs;
        }

        @Override
        public void processElement(StreamRecord r, Context ctx, Collector<StreamRecord> out) {
            r.watermarkMs = ctx.timerService().currentWatermark();
            r.delayClass = Rules.classify(r.eventTsMs, r.watermarkMs, windowMs, latenessMs);
            if (Rules.TOO_LATE.equals(r.delayClass)) {
                ctx.output(TOO_LATE, r);
            } else {
                out.collect(r);
            }
        }
    }

    /** First occurrence of an event_id passes; later ones within the TTL go to DUPLICATES. */
    public static class Dedup extends KeyedProcessFunction<String, StreamRecord, StreamRecord> {
        private transient ValueState<Boolean> seen;

        @Override
        public void open(OpenContext context) {
            StateTtlConfig ttl =
                    StateTtlConfig.newBuilder(Duration.ofMillis(Rules.DEDUP_TTL_MS))
                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .build();
            ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Types.BOOLEAN);
            descriptor.enableTimeToLive(ttl);
            seen = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(StreamRecord r, Context ctx, Collector<StreamRecord> out)
                throws Exception {
            if (seen.value() == null) {
                seen.update(Boolean.TRUE);
                out.collect(r);
            } else {
                r.watermarkMs = ctx.timerService().currentWatermark();
                ctx.output(DUPLICATES, r);
            }
        }
    }

    public static class SumAgg implements AggregateFunction<StreamRecord, WindowAcc, WindowAcc> {
        @Override
        public WindowAcc createAccumulator() {
            return new WindowAcc();
        }

        @Override
        public WindowAcc add(StreamRecord r, WindowAcc acc) {
            acc.streamRunId = r.streamRunId;
            acc.channel = r.channel;
            acc.currency = r.currency;
            acc.count += 1;
            acc.cents += r.amountCents;
            return acc;
        }

        @Override
        public WindowAcc getResult(WindowAcc acc) {
            return acc;
        }

        @Override
        public WindowAcc merge(WindowAcc a, WindowAcc b) {
            a.count += b.count;
            a.cents += b.cents;
            return a;
        }
    }

    /** Emits each firing with a sequence number; late firings update the same window. */
    public static class WindowEmit extends ProcessWindowFunction<WindowAcc, OutRecord, String, TimeWindow> {
        private final ValueStateDescriptor<Integer> firings = new ValueStateDescriptor<>("firings", Types.INT);

        @Override
        public void process(String key, Context ctx, Iterable<WindowAcc> results, Collector<OutRecord> out)
                throws Exception {
            ValueState<Integer> state = ctx.windowState().getState(firings);
            int seq = state.value() == null ? 1 : state.value() + 1;
            state.update(seq);
            WindowAcc acc = results.iterator().next();
            out.collect(OutRecord.window(acc, ctx.window().getStart(), ctx.window().getEnd(), seq, ctx.currentWatermark()));
        }

        @Override
        public void clear(Context ctx) throws Exception {
            ctx.windowState().getState(firings).clear();
        }
    }

    public static class ToEvent implements MapFunction<StreamRecord, OutRecord> {
        private final String branch;

        public ToEvent(String branch) {
            this.branch = branch;
        }

        @Override
        public OutRecord map(StreamRecord r) {
            return OutRecord.event(r, branch);
        }
    }

    public static String windowKey(StreamRecord r) {
        return r.streamRunId + "|" + r.channel + "|" + r.currency;
    }
}
