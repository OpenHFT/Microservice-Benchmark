package run.chronicle.channel;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.Wire;
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
import static org.junit.Assume.assumeFalse;

@RunWith(Parameterized.class)
public class PubSubHandlerTest {

    private final String url;

    public PubSubHandlerTest(String name, String url) {
        this.url = url;
    }

    @Parameterized.Parameters(name = "name: {0}, url: {1}")
    public static List<Object[]> combinations() {
        return Arrays.asList(
                new Object[]{"internal", "internal://"},
                new Object[]{"client-only", "tcp://127.0.0.1:65451"},
                new Object[]{"server", "tcp://localhost:65452"}
        );
    }

    @Test
    public void testPubSub() throws IOException {
        IOTools.deleteDirWithFiles("test-q");

        try (ChronicleContext context = ChronicleContext.newContext(url)) {
            // do we assume a server is running
            if (url.contains("/127.0.0.1:")) {
                ChronicleGatewayMain gateway = new ChronicleGatewayMain(this.url);
                context.addCloseable(gateway);
                gateway.start();
            }

            ChronicleChannel channel = context.newChannelSupplier(new PubSubHandler()).get();
            PubSubSays pss = channel.methodWriter(PubSubSays.class);
            pss.subscribe(new Subscribe().eventType("from").name("test-q"));

            pss.to("test-q").say("Hello");

            Wire wire = Wire.newYamlWireOnHeap();
            final FromSays fromSays = wire.methodWriter(FromSays.class);
            final MethodReader reader = channel.methodReader(fromSays);
            reader.readOne();
            assertEquals("" +
                            "from: test-q\n" +
                            "say: Hello\n" +
                            "...\n",
                    wire.toString());
        } catch (UnsupportedOperationException uos) {
            assumeFalse(url.startsWith("internal"));
        }
    }

    interface PubSubSays extends PubSub {
        Says to(String name);
    }

    interface FromSays {
        Says from(String name);
    }
}