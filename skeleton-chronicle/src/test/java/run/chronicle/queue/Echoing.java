package run.chronicle.queue;

import net.openhft.chronicle.bytes.MethodId;

interface Echoing {
    @MethodId(1)
    void echo(Data data);
}
