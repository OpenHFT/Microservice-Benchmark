package run.chronicle.queue;

import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class SessionCfg extends SelfDescribingMarshallable {
    private boolean initiator;
    private boolean buffered;
    private PauserMode pauser = PauserMode.yielding;
    private String hostname;
    private int port;

    public SessionCfg initiator(boolean initiator) {
        this.initiator = initiator;
        return this;
    }

    public boolean initiator() {
        return initiator;
    }

    public String hostname() {
        return hostname;
    }

    public SessionCfg hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public int port() {
        return port;
    }

    public SessionCfg port(int port) {
        this.port = port;
        return this;
    }

    public SocketAddress remote() {
        return new InetSocketAddress(hostname, port);
    }

    public boolean buffered() {
        return buffered;
    }

    public SessionCfg buffered(boolean buffered) {
        this.buffered = buffered;
        return this;
    }

    public PauserMode pauser() {
        return pauser;
    }

    public SessionCfg pauser(PauserMode pauser) {
        this.pauser = pauser;
        return this;
    }
}
