package services.openmicro.framework;

import net.openhft.chronicle.wire.AbstractMarshallableCfg;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.time.Duration;

public class Workload extends SelfDescribingMarshallable {
    int throughput;
    Duration duration = Duration.ofMinutes(5);
}
