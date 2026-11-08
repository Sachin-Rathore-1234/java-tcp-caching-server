
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerMetrics {

    private final LongAdder requests = new LongAdder();
    private final LongAdder rateLimited = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final AtomicInteger activeConnections =
        new AtomicInteger();

    public void request() {
        requests.increment();
    }

    public void rejected() {
        rateLimited.increment();
    }

    public void error() {
        errors.increment();
    }

    public void connectionOpened() {
        activeConnections.incrementAndGet();
    }

    public void connectionClosed() {
        activeConnections.decrementAndGet();
    }

    public String snapshot() {
        return String.format(
            "requests=%d rateLimited=%d errors=%d active=%d",
            requests.sum(),
            rateLimited.sum(),
            errors.sum(),
            activeConnections.get()
        );
    }
}
