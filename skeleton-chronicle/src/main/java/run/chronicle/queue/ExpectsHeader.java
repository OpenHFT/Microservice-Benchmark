package run.chronicle.queue;

import net.openhft.chronicle.wire.Marshallable;

interface ExpectsHeader {
    void header(Marshallable marshallable);
}
