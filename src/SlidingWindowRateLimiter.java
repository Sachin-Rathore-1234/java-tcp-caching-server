
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class SlidingWindowRateLimiter {

    private static class Window {
        final ArrayDeque<Long> requests = new ArrayDeque<>();
        long lastActivity = System.nanoTime();
    }

    private final ConcurrentHashMap<String, Window> clients =
        new ConcurrentHashMap<>();

    private final int maxRequests;
    private final long windowNanos;

    public SlidingWindowRateLimiter(
        int maxRequests,
        long windowSeconds
    ) {
        if (maxRequests <= 0 ||
            windowSeconds <= 0 ||
            windowSeconds > 86400) {
            throw new IllegalArgumentException(
                "Invalid rate limit"
            );
        }

        this.maxRequests = maxRequests;
        this.windowNanos =
            TimeUnit.SECONDS.toNanos(windowSeconds);
    }

    public boolean allowRequest(String clientId) {

        Window window = clients.computeIfAbsent(
            clientId,
            id -> new Window()
        );

        synchronized (window) {

            long now = System.nanoTime();
            window.lastActivity = now;

            // Remove timestamps outside the window.
            while (!window.requests.isEmpty() &&
                   now - window.requests.peekFirst()
                       >= windowNanos) {

                window.requests.removeFirst();
            }

            if (window.requests.size() >= maxRequests) {
                return false;
            }

            window.requests.addLast(now);
            return true;
        }
    }

    public int trackedClients() {
        return clients.size();
    }
}
