package run.chronicle.queue;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;
import org.junit.Test;

import static org.junit.Assert.*;

interface Says {
    void say(String say);
}

public class ChronicleBrokerMainTest {

    @Test
    public void main() {
        final String test_q = "test.q";
        IOTools.deleteDirWithFiles(test_q);
        String cfg = "" +
                "port: 65432";
        ChronicleBrokerMain main = Marshallable.fromString(ChronicleBrokerMain.class, cfg);
        Thread t = new Thread(main::run);
        t.setDaemon(true);
        t.start();

        final ConnectionCfg connectionCfg = new ConnectionCfg().hostname("localhost").port(65432).initiator(true);
        Connection client = Connection.createFor(connectionCfg, new SimplePipeHandler().publish(test_q).subscribe(test_q));
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
        main.close();
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
}