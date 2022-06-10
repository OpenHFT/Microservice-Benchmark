package run.chronicle.channel;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.YamlWire;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleContext;
import run.chronicle.channel.api.PubSub;
import run.chronicle.channel.api.Subscribe;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class PubSubHandlerTest {

    private String name;
    private String hostname;
    private int port;

    public PubSubHandlerTest(String name, String hostname, int port) {
        this.name = name;
        this.hostname = hostname;
        this.port = port;
    }

    @Parameterized.Parameters(name = "name: {0}, hostname: {1}, port: {2}")
    public static List<Object[]> combinations() {
        return Arrays.asList(
//                new Object[]{"in-memory", null, 0},
                new Object[]{"server", null, 65451},
                new Object[]{"client", "localhost", 65452}
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

            ChronicleChannel channel = context.newChannelSupplier(new PubSubHandler()).get();
            PubSubSays pss = channel.methodWriter(PubSubSays.class);
            pss.subscribe(new Subscribe().eventType("from").name("test-q"));

            pss.to("test-q").say("Hello");

            Wire wire = new YamlWire(Bytes.allocateElasticOnHeap()).useTextDocuments();
            final FromSays fromSays = wire.methodWriter(FromSays.class);
            final MethodReader reader = channel.methodReader(fromSays);
            reader.readOne();
            assertEquals("" +
                            "from: test-q\n" +
                            "say: Hello\n" +
                            "...\n",
                    wire.toString());
        } catch (UnsupportedOperationException uos) {
            assumeTrue(port > 0);
        }
    }

    interface PubSubSays extends PubSub {
        Says to(String name);
    }

    interface FromSays {
        Says from(String name);
    }
}