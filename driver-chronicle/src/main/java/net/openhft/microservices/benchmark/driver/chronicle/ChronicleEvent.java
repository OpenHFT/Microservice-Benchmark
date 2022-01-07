package net.openhft.microservices.benchmark.driver.chronicle;

import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.NanoTimestampLongConverter;
import net.openhft.microservices.benchmark.driver.api.Event;

public class ChronicleEvent extends BytesInBinaryMarshallable implements Event {
    private long sendingTimeNS;
    private long transactTimeNS;
    @LongConversion(NanoTimestampLongConverter.class)
    private long dateTime1, dateTime2, dateTime3, dateTime4;
    private String text1, text2, text3, text4;
    private long number1, number2, number3, number4;
    private double value1, value2, value3, value4, value5, value6, value7, value8;

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
