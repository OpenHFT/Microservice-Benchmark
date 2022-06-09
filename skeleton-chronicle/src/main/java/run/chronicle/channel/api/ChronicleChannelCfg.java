package run.chronicle.channel.api;

import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ChronicleChannelCfg extends SelfDescribingMarshallable {
    private boolean initiator;
    private boolean buffered;
    private PauserMode pauser = PauserMode.yielding;
    private String hostname;
    private int port;

    private double connectionTimeoutSecs = 1.0;

    public ChronicleChannelCfg initiator(boolean initiator) {
        this.initiator = initiator;
        return this;
    }

    public boolean initiator() {
        return initiator;
    }

    public String hostname() {
        return hostname;
    }

    public ChronicleChannelCfg hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public int port() {
        return port;
    }

    public ChronicleChannelCfg port(int port) {
        this.port = port;
        return this;
    }

    public SocketAddress remote() {
        return new InetSocketAddress(hostname, port);
    }

    public boolean buffered() {
        return buffered;
    }

    public ChronicleChannelCfg buffered(boolean buffered) {
        this.buffered = buffered;
        return this;
    }

    public PauserMode pauserMode() {
        return pauser;
    }

    public ChronicleChannelCfg pauserMode(PauserMode pauser) {
        this.pauser = pauser;
        return this;
    }

    public double connectionTimeoutSecs() {
        return connectionTimeoutSecs;
    }

    public ChronicleChannelCfg connectionTimeoutSecs(double connectionTimeoutSecs) {
        this.connectionTimeoutSecs = connectionTimeoutSecs;
        return this;
    }
}
