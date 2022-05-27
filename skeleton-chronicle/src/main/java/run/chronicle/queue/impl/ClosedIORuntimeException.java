package run.chronicle.queue.impl;

import net.openhft.chronicle.core.io.IORuntimeException;

public class ClosedIORuntimeException extends IORuntimeException {
    public ClosedIORuntimeException(String message) {
        super(message);
    }
}
