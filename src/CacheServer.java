
import java.io.IOException;
import java.net.*;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class CacheServer {

    private static final Logger logger =
        Logger.getLogger(CacheServer.class.getName());

    private static final int PORT = 6379;
    private static final int WORKERS = 8;
    private static final int QUEUE_SIZE = 100;

    public static void main(String[] args)
            throws IOException {

        ConcurrentLRUCache cache =
            new ConcurrentLRUCache(1000);

        SlidingWindowRateLimiter rateLimiter =
            new SlidingWindowRateLimiter(100000, 60);

        ServerMetrics metrics = new ServerMetrics();

        Set<Socket> activeSockets =
            ConcurrentHashMap.newKeySet();

        ThreadPoolExecutor pool = new ThreadPoolExecutor(
            WORKERS,
            WORKERS,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(QUEUE_SIZE),
            new ThreadPoolExecutor.AbortPolicy()
        );

        AtomicBoolean running = new AtomicBoolean(true);

        // Bind to localhost for development
        ServerSocket server = new ServerSocket();
        server.setReuseAddress(true);

        try {
            server.bind(new InetSocketAddress(
                InetAddress.getLoopbackAddress(), PORT
            ), 128);
        } catch (IOException e) {
            pool.shutdownNow();
            cache.close();
            server.close();
            throw e;
        }

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                running.set(false);

                try {
                    server.close();
                } catch (IOException ignored) {}

                pool.shutdown();

                try {
                    if (!pool.awaitTermination(
                            5, TimeUnit.SECONDS)) {
                        for (Socket s : activeSockets) {
                            try {
                                s.close();
                            } catch (IOException ignored) {}
                        }
                        pool.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    for (Socket s : activeSockets) {
                        try {
                            s.close();
                        } catch (IOException ignored) {}
                    }
                    pool.shutdownNow();
                    Thread.currentThread().interrupt();
                }

                cache.close();
            })
        );

        logger.info("Cache server started");

        while (running.get()) {

            Socket client;

            try {
                client = server.accept();
            } catch (SocketException e) {
                if (!running.get()) {
                    break;
                }
                throw e;
            }

            activeSockets.add(client);

            try {
                pool.execute(
                    new ClientHandler(
                        client,
                        cache,
                        rateLimiter,
                        metrics,
                        activeSockets
                    )
                );

            } catch (RejectedExecutionException e) {
                // Overloaded: close excess connections
                activeSockets.remove(client);
                try {
                    client.close();
                } catch (IOException ignored) {}

                logger.warning("Connection rejected: pool saturated");
            }
        }
    }
}
