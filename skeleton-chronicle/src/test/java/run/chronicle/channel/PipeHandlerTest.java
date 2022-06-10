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
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class PipeHandlerTest {

    private String name;
    private String hostname;
    private int port;

    public PipeHandlerTest(String name, String hostname, int port) {
        this.name = name;
        this.hostname = hostname;
        this.port = port;
    }

    @Parameterized.Parameters(name = "name: {0}, hostname: {1}, port: {2}")
    public static List<Object[]> combinations() {
        return Arrays.asList(
                new Object[]{"in-memory", null, 0},
                new Object[]{"server", null, 65441},
                new Object[]{"client", "localhost", 65442}
        );
    }

    @Test
    public void testPubSub() throws IOException {
        IOTools.deleteDirWithFiles("test-q");

        try (ChronicleContext context = ChronicleContext.newContext().hostname(hostname).port(port)) {
            // do we assume a broker is running
            if (hostname != null) {
                ChronicleGatewayMain gateway = new ChronicleGatewayMain().port(port);
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
        } catch (UnsupportedOperationException uos) {
            assumeTrue(port > 0);
        }
    }
}