package run.chronicle.channel;

import net.openhft.chronicle.core.io.IOTools;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class PipeHandlerTest {

    private final String url;

    public PipeHandlerTest(String name, String url) {
        this.url = url;
    }

    @Parameterized.Parameters(name = "name: {0}, url: {1}")
    public static List<Object[]> combinations() {
        return Arrays.asList(
                new Object[]{"internal", "internal://"},
                new Object[]{"client-only", "tcp://localhost:65441"},
                new Object[]{"server", "tcp://:65442"}
        );
    }

    @Test
    public void testPipes() throws IOException {
        IOTools.deleteDirWithFiles("test-q");

        try (ChronicleContext context = ChronicleContext.newContext(url)) {
            // do we assume a server is running
            if (url.contains("/localhost:")) {
                ChronicleGatewayMain gateway = new ChronicleGatewayMain(this.url);
                context.addCloseable(gateway);
                gateway.start();
            }

            ChronicleChannel channel = context.newChannelSupplier(new PipeHandler().subscribe("test-q").publish("test-q")).get();
            Says says = channel.methodWriter(Says.class);
            says.say("Hello World");

            StringBuilder eventType = new StringBuilder();
            String text = channel.readOne(eventType, String.class);
            assertEquals("say: Hello World",
                    eventType + ": " + text);
        }
    }
}