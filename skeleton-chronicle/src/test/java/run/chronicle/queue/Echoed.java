package run.chronicle.queue;

import net.openhft.chronicle.bytes.MethodId;

interface Echoed {
    @MethodId(2)
    void echoed(Data data);
}
