package omnichannel.stream.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class JobArgsTest {
    @Test
    void defaultsAndOverrides() {
        JobArgs args = JobArgs.parse(new String[] {"--start", "earliest", "--checkpoint-ms", "10000"});
        assertEquals("kafka:29092", args.bootstrap);
        assertEquals("earliest", args.start);
        assertEquals("s3://raw/stream", args.output);
        assertEquals(10_000L, args.checkpointMs);
        assertEquals("1769904000000", JobArgs.parse(new String[] {"--start", "1769904000000"}).start);
    }

    @Test
    void rejectsBadValues() {
        assertThrows(IllegalArgumentException.class, () -> JobArgs.parse(new String[] {"--start", "yesterday"}));
        assertThrows(IllegalArgumentException.class, () -> JobArgs.parse(new String[] {"--start"}));
    }
}
