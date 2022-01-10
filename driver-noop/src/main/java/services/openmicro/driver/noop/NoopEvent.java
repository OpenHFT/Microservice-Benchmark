package services.openmicro.driver.noop;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import services.openmicro.driver.api.Event;

public class NoopEvent extends SelfDescribingMarshallable implements Event {
    private long sendingTimeNS;
    private long transactTimeNS;

    @Override
    public void sendingTimeNS(long sendingTimeNS) {
        this.sendingTimeNS = sendingTimeNS;
    }

    @Override
    public long sendingTimeNS() {
        return sendingTimeNS;
    }

    @Override
    public void transactTimeNS(long transactTimeNS) {
        this.transactTimeNS = transactTimeNS;
    }

    @Override
    public long transactTimeNS() {
        return transactTimeNS;
    }
}
