
import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientHandler implements Runnable {

    private static final Logger logger =
        Logger.getLogger(ClientHandler.class.getName());

    private final Socket socket;
    private final ConcurrentLRUCache cache;
    private final SlidingWindowRateLimiter rateLimiter;
    private final ServerMetrics metrics;
    private final Set<Socket> activeSockets;

    private static final int MAX_LINE = 4096;
    private static final int MAX_COMMANDS = 100;

    public ClientHandler(
        Socket socket,
        ConcurrentLRUCache cache,
        SlidingWindowRateLimiter rateLimiter,
        ServerMetrics metrics,
        Set<Socket> activeSockets
    ) {
        this.socket = socket;
        this.cache = cache;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
        this.activeSockets = activeSockets;
    }

    public ClientHandler(
        Socket socket,
        ConcurrentLRUCache cache,
        SlidingWindowRateLimiter rateLimiter,
        ServerMetrics metrics
    ) {
        this(socket, cache, rateLimiter, metrics, null);
    }

    public ClientHandler(
        Socket socket,
        ConcurrentLRUCache cache,
        SlidingWindowRateLimiter rateLimiter
    ) {
        this(socket, cache, rateLimiter, new ServerMetrics(), null);
    }

    @Override
    public void run() {
        metrics.connectionOpened();
        try (Socket client = socket;
             BufferedReader reader = new BufferedReader(
                 new InputStreamReader(
                     client.getInputStream(),
                     StandardCharsets.UTF_8
                 )
             );
             PrintWriter writer = new PrintWriter(
                 new OutputStreamWriter(
                     client.getOutputStream(),
                     StandardCharsets.UTF_8
                 ), true
             )) {

            client.setSoTimeout(30000);

            String clientId =
                client.getInetAddress().getHostAddress();

            int commands = 0;

            while (commands < MAX_COMMANDS) {
                String line = readLimitedLine(reader);

                if (line == null) {
                    break;
                }

                commands++;
                metrics.request();

                if (!rateLimiter.allowRequest(clientId)) {
                    metrics.rejected();
                    writer.println("RATE_LIMITED");

                    if (writer.checkError()) {
                        break;
                    }

                    continue;
                }

                String response = execute(line);
                if (response.startsWith("ERROR")) {
                    metrics.error();
                }
                writer.println(response);

                if (writer.checkError()) {
                    break;
                }

                if (response.equals("BYE")) {
                    break;
                }
            }

        } catch (SocketTimeoutException e) {
            logger.info("Client timed out");

        } catch (IOException e) {
            metrics.error();
            logger.log(
                Level.WARNING,
                "Client handling failed",
                e
            );
        } finally {
            if (activeSockets != null) {
                activeSockets.remove(socket);
            }
            metrics.connectionClosed();
        }
    }

    // Prevent unbounded line allocation
    private String readLimitedLine(BufferedReader reader)
            throws IOException {

        StringBuilder sb = new StringBuilder();

        while (true) {
            int c = reader.read();

            if (c == -1) {
                return sb.length() == 0
                    ? null : sb.toString();
            }

            if (c == '\n') {
                return sb.toString();
            }

            if (c == '\r') {
                continue;
            }

            if (sb.length() >= MAX_LINE) {
                throw new IOException("Command too long");
            }

            sb.append((char) c);
        }
    }

    private String execute(String line) {

        String[] parts = line.trim().split("\\s+");

        if (parts.length == 0 ||
            parts[0].isEmpty()) {
            return "ERROR Empty command";
        }

        String command = parts[0].toUpperCase(
            java.util.Locale.ROOT
        );

        try {
            switch (command) {

                case "PING":
                    return parts.length == 1
                        ? "PONG" : "ERROR Usage: PING";

                case "SET":
                    if (parts.length != 4) {
                        return "ERROR Usage: SET key value ttlMs";
                    }

                    long ttl = Long.parseLong(parts[3]);

                    cache.put(parts[1], parts[2], ttl);
                    return "OK";

                case "GET":
                    if (parts.length != 2) {
                        return "ERROR Usage: GET key";
                    }

                    String value = cache.get(parts[1]);

                    return value == null
                        ? "NOT_FOUND"
                        : "VALUE " + value;

                case "DELETE":
                    if (parts.length != 2) {
                        return "ERROR Usage: DELETE key";
                    }

                    return cache.delete(parts[1])
                        ? "DELETED"
                        : "NOT_FOUND";

                case "STATS":
                    return parts.length == 1
                        ? "STATS " + cache.stats()
                        : "ERROR Usage: STATS";

                case "METRICS":
                    return parts.length == 1
                        ? "METRICS " + metrics.snapshot()
                        : "ERROR Usage: METRICS";

                case "QUIT":
                    return parts.length == 1
                        ? "BYE" : "ERROR Usage: QUIT";

                default:
                    return "ERROR Unknown command";
            }

        } catch (NumberFormatException e) {
            return "ERROR Invalid TTL";

        } catch (IllegalArgumentException e) {
            return "ERROR " + e.getMessage();
        }
    }
}
