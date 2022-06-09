package run.chronicle.channel;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleChannelCfg;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

interface Says {
    void say(String say);
}

public class ChronicleGatewayMainTest {

    static String waitForText(ExcerptTailer tailer, int time, TimeUnit timeUnit) {
        long end = System.nanoTime() + timeUnit.toNanos(time);
        do {
            String text = tailer.readText();
            if (text != null)
                return text;
            Thread.yield();
        } while (System.nanoTime() < end);
        throw new AssertionError("timeout");
    }

    @Test
    public void main() throws IOException {
        final String test_q = "test.q";
        IOTools.deleteDirWithFiles(test_q);
        String cfg = "" +
                "port: 65432";
        try (ChronicleGatewayMain main = Marshallable.fromString(ChronicleGatewayMain.class, cfg).start()) {

            final ChronicleChannelCfg channelCfg = new ChronicleChannelCfg().hostname("localhost").port(65432).initiator(true);
            ChronicleChannel client = ChronicleChannel.newChannel(channelCfg, new PipeHandler().publish(test_q).subscribe(test_q));
            final Says says = client.methodWriter(Says.class);
            says.say("Hello World");

            try (final DocumentContext dc = client.readingDocument()) {
                assertTrue(dc.isPresent());
                final Wire wire = dc.wire();
                assertNotNull(wire);
                assertEquals("say", wire.readEvent(String.class));
                assertEquals("Hello World", wire.getValueIn().text());
            }
            client.close();
        }
        try (ChronicleQueue queue = ChronicleQueue.single(test_q)) {
            String s = queue.dump();
            final String contains = "" +
                    "--- !!data #binary\n" +
                    "say: Hello World\n" +
                    "...";
            if (!s.contains(contains))
                fail(s + "\ndoesn't contain\n" + contains);
        }
    }

    @Test
    public void twoConnections() throws IOException {
        final ChronicleChannelCfg channelCfg = new ChronicleChannelCfg().hostname("localhost").port(65432).initiator(true);
        final String two_qs = "two.qs";
        IOTools.deleteDirWithFiles(two_qs);

        try (final ChronicleGatewayMain broker = new ChronicleGatewayMain().port(channelCfg.port()).start();
             ChronicleChannel clientA = ChronicleChannel.newChannel(channelCfg, new PipeHandler().publish(two_qs + "/A").subscribe(two_qs + "/B"));
             ChronicleChannel clientB = ChronicleChannel.newChannel(channelCfg, new PipeHandler().publish(two_qs + "/B").subscribe(two_qs + "/A"));
             ChronicleQueue qA = ChronicleQueue.single(two_qs + "/A");
             ExcerptTailer qAt = qA.createTailer();
             ChronicleQueue qB = ChronicleQueue.single(two_qs + "/B");
             ExcerptTailer qBt = qB.createTailer()) {

            clientA.writeText("Hello world");
            String read = waitForText(qAt, 1, TimeUnit.SECONDS);
            assertEquals("Hello world", read);

            String read2 = clientB.readText();
            assertEquals("Hello world", read2);
        }
    }
}
