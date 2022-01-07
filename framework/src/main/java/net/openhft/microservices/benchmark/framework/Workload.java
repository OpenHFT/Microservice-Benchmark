package net.openhft.microservices.benchmark.framework;

import net.openhft.chronicle.wire.AbstractMarshallableCfg;
import java.time.Duration;

public class Workload extends AbstractMarshallableCfg {
    int throughput;
    Duration duration = Duration.ofMinutes(5);
}
