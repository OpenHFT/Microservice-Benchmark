package services.openmicro.framework;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.time.Duration;
import java.util.List;

public class Workload extends SelfDescribingMarshallable {
    List<Integer> throughputs;
    Duration duration = Duration.ofMinutes(5);
}
