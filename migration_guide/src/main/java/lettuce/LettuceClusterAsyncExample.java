package lettuce;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ExpireArgs;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Delay;
import io.lettuce.core.resource.DirContextDnsResolver;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LettuceClusterAsyncExample {
    public static void main(String[] args) {
        System.out.println("Starting Lettuce example");
        // Create Redis Cluster Client
        RedisClusterClient clusterClient = createRecommendedClusterClient("localhost", 48300, false);

        // Establish a connection to the Redis cluster
        try (StatefulRedisClusterConnection<String, String> connection = clusterClient.connect()) {
            RedisClusterAsyncCommands<String, String> asyncCommands = connection.async();

            // Perform Redis operations
            runRedisOperations(asyncCommands).thenRun(() -> {
                System.out.println("All operations completed.");
            }).join();

        // ZADD operation
        asyncCommands.flushall().get();
        } catch (Exception e) {
            e.printStackTrace(); // Handle the exception or log it
        } finally {
            clusterClient.shutdown();
        }
    }

    private static CompletableFuture<Void> runRedisOperations(RedisClusterAsyncCommands<String, String> asyncCommands) {
        // SET operation
        RedisFuture<String> setFuture = asyncCommands.set("key1", "value1");
        CompletableFuture<Void> setOp = CompletableFuture.supplyAsync(() -> {
            try {
                return setFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(result -> System.out.println("SET result: " + result));

        // GET operation
        RedisFuture<String> getFuture = asyncCommands.get("key1");
        CompletableFuture<Void> getOp = CompletableFuture.supplyAsync(() -> {
            try {
                return getFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(result -> System.out.println("GET result: " + result));

        // MSET operation
        Map<String, String> msetMap = new HashMap<>();
        msetMap.put("key2", "value2");
        msetMap.put("key3", "value3");
        RedisFuture<String> msetFuture = asyncCommands.mset(msetMap);
        CompletableFuture<Void> msetOp = CompletableFuture.supplyAsync(() -> {
            try {
                return msetFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(result -> System.out.println("MSET result: " + result));

        // PEXPIRE operation
        RedisFuture<Boolean> pexpireFuture = asyncCommands.pexpire("key1", 5000, ExpireArgs.Builder.nx()); // 5000ms = 5 seconds
        CompletableFuture<Void> pexpireOp = CompletableFuture.supplyAsync(() -> {
            try {
                return pexpireFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(result -> System.out.println("PEXPIRE result: " + result));

        // ZADD operation
        RedisFuture<Long> zaddFuture = asyncCommands.zadd("myzset", 1.0, "member1", 2.0, "member2");
        CompletableFuture<Void> zaddOp = CompletableFuture.supplyAsync(() -> {
            try {
                return zaddFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(result -> System.out.println("ZADD result: " + result));
        // Combine all futures
        return CompletableFuture.allOf(setOp, getOp, msetOp, pexpireOp, zaddOp);
    }

    public static RedisClusterClient createRecommendedClusterClient(String host, int port, boolean with_ssl) {
    final ClientResources clientResources =
        DefaultClientResources.builder()
            .dnsResolver(new DirContextDnsResolver())
            .reconnectDelay(
                Delay.fullJitter(
                    Duration.ofMillis(100), // minimum 100 millisecond delay
                    Duration.ofSeconds(5), // maximum 5 second delay
                    100,
                    TimeUnit.MILLISECONDS)) // 100 millisecond base
            .build();
    final RedisURI redisUriCluster =
        RedisURI.Builder.redis(host)
            .withPort(port)
            .withSsl(with_ssl)
            .withDatabase(0)
            .build();
    final RedisClusterClient redisClusterClient =
        RedisClusterClient.create(clientResources, redisUriCluster);
    final ClusterTopologyRefreshOptions topologyOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .enablePeriodicRefresh()
            .dynamicRefreshSources(false)
            .build();
    final SocketOptions socketOptions =
        SocketOptions.builder().connectTimeout(Duration.ofMillis(2000)).keepAlive(true).build();

    ClusterClientOptions clusterClientOptions =
        ClusterClientOptions.builder()
            .topologyRefreshOptions(topologyOptions)
            .socketOptions(socketOptions)
            .autoReconnect(true)
            .nodeFilter(
                it ->
                    !(it.is(RedisClusterNode.NodeFlag.FAIL)
                        || it.is(RedisClusterNode.NodeFlag.EVENTUAL_FAIL)
                        || it.is(RedisClusterNode.NodeFlag.NOADDR))) // Filter out Predicate
            .validateClusterNodeMembership(false)
            .build();
    redisClusterClient.setOptions(clusterClientOptions);
    return redisClusterClient;
  }
}
