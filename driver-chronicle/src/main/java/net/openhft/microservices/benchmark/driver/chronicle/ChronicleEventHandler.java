package net.openhft.microservices.benchmark.driver.chronicle;

import net.openhft.chronicle.bytes.MethodId;
import net.openhft.microservices.benchmark.driver.api.EventHandler;

public interface ChronicleEventHandler extends EventHandler<ChronicleEvent> {
    @MethodId(1)
    @Override
    void event(ChronicleEvent event) ;
}
