package run.chronicle.queue;

public interface EventPoller {
    boolean onPoll(Connection connection);
}
