package run.chronicle.queue;

public interface MicroserviceOut<O, D> {
    O out();

    D via(String stream);

    void subscribe(String stream);

    void unsubscribe(String stream);
}
