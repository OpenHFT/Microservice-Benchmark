package run.chronicle.channel.api;

public interface EventPoller {
    boolean onPoll(ChronicleChannel channel);
}
