package run.chronicle.channel.impl;

import net.openhft.chronicle.core.io.IORuntimeException;

public class ClosedIORuntimeException extends IORuntimeException {
    public ClosedIORuntimeException(String message) {
        super(message);
    }
}
