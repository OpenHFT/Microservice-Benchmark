package run.chronicle.channel;

import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.wire.DocumentContext;
import run.chronicle.channel.api.Channel;
import run.chronicle.channel.api.ChannelHandler;
import run.chronicle.channel.api.ChronicleContext;

public class Main {
    static final String HOSTNAME = System.getProperty("hostname");
    static final int PORT = Integer.getInteger("port", 0);

    public static void main(String[] args) {
        final String in = "in";
        final String out = "out";
        IOTools.deleteDirWithFiles(in, out);
        try (ChronicleContext context = ChronicleContext.create().hostname(HOSTNAME).port(PORT)) {
            // start a service
            final ChannelHandler handler0 = new SimplePipeHandler().publish(in).subscribe(out);
            Runnable runs = context.serviceAsRunnable(handler0, EchoingMicroservice::new, Echoed.class);
            new Thread(runs).start();

            // start a client
            final ChannelHandler handler = new SimplePipeHandler().publish(out).subscribe(in);
            final Channel channel = context.newChannelSupplier(handler).get();

            // write a message
            final Echoing echoing = channel.methodWriter(Echoing.class);
            echoing.echo(new Data());

            // wait for the reply
            try (final DocumentContext dc = channel.readingDocument()) {
                Data data = dc.wire().read("echoed").object(Data.class);
            }
        }
    }

}
