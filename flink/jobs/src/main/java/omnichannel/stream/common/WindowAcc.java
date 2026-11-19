package omnichannel.stream.common;

/** Window accumulator: count and amount in cents (never floating point). */
public class WindowAcc {
    public String streamRunId;
    public String channel;
    public String currency;
    public long count;
    public long cents;

    public WindowAcc() {}
}
