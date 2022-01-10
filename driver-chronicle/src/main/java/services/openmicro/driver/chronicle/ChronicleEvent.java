package services.openmicro.driver.chronicle;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.BytesUtil;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.Base128LongConverter;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.NanoTimestampLongConverter;
import services.openmicro.driver.api.Event;

public class ChronicleEvent extends BytesInBinaryMarshallable implements Event {
    static final int START_BYTES = BytesUtil.triviallyCopyableStart(ChronicleEvent.class);
    static final int LENGTH_BYTES = BytesUtil.triviallyCopyableLength(ChronicleEvent.class);

    private long sendingTimeNS;
    private long transactTimeNS;

    @LongConversion(NanoTimestampLongConverter.class)
    private long dateTime1, dateTime2, dateTime3, dateTime4;

    @LongConversion(Base128LongConverter.class)
    private long text1, text2; // up to 9 ASCII chars
    private String text3, text4;

    private int number1, number2;
    private long number3, number4;
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

    @Override
    public final void readMarshallable(BytesIn bytes) throws IORuntimeException {
        bytes.unsafeReadObject(this, START_BYTES, LENGTH_BYTES);
        text3 = bytes.readUtf8();
        text4 = bytes.readUtf8();
    }

    @Override
    public final void writeMarshallable(BytesOut bytes) {
        bytes.unsafeWriteObject(this, START_BYTES, LENGTH_BYTES);
        bytes.writeUtf8(text3);
        bytes.writeUtf8(text4);
    }
}
