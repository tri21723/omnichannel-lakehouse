package omnichannel.stream.common;

import java.util.HashMap;
import java.util.Map;

/** `--key value` arguments; no credentials are ever passed here (S3 uses the environment). */
public final class JobArgs {
    public final String bootstrap;
    public final String start;
    public final String output;
    public final long checkpointMs;

    private JobArgs(Map<String, String> values) {
        bootstrap = values.getOrDefault("bootstrap", "kafka:29092");
        start = values.getOrDefault("start", "latest");
        output = values.getOrDefault("output", "s3://raw/stream");
        checkpointMs = Long.parseLong(values.getOrDefault("checkpoint-ms", "30000"));
        if (!start.matches("earliest|latest|\\d{13}")) {
            throw new IllegalArgumentException("--start must be earliest, latest or epoch millis");
        }
    }

    public static JobArgs parse(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("expected --key value pairs");
            }
            values.put(args[i].substring(2), args[i + 1]);
        }
        return new JobArgs(values);
    }
}
