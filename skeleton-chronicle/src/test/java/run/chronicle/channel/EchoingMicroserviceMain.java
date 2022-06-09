package run.chronicle.channel;

import java.io.IOException;

public class EchoingMicroserviceMain {
    public static void main(String... args) throws IOException {
        ChronicleServiceMain.main("echoing.yaml");
    }
}
