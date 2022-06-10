package run.chronicle.channel.api;

public interface PubSub {
    void subscribe(Subscribe subscribe);

    void unsubscribe(String name);
}
