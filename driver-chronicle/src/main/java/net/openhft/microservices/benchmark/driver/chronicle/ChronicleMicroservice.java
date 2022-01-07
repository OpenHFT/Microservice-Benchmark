package net.openhft.microservices.benchmark.driver.chronicle;

public class ChronicleMicroservice implements ChronicleEventHandler {
    final transient ChronicleEventHandler out;

    public ChronicleMicroservice(ChronicleEventHandler out) {
        this.out = out;
    }

    @Override
    public void event(ChronicleEvent event) {
        event.transactTimeNS(System.nanoTime());
        out.event(event);
    }
}
