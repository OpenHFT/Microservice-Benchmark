package run.chronicle.queue;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

class Data extends BytesInBinaryMarshallable {
    long timeNS;
    byte[] data;

    @Override
    public void readMarshallable(BytesIn bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException {
        timeNS = bytes.readLong();
        int len = bytes.readUnsignedShort();
        if (data == null || data.length != len)
            data = new byte[len];
        bytes.read(data);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException {
        bytes.writeLong(timeNS);
        bytes.writeUnsignedShort(data.length);
        bytes.write(data);
    }
}
