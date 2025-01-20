package glide;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import glide.api.GlideClusterClient;
import glide.api.models.ClusterValue;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.exceptions.ClosingException;
import glide.api.models.exceptions.ConnectionException;
import glide.api.models.exceptions.TimeoutException;
import glide.api.models.commands.ExpireOptions;
import java.util.Collections;
import java.util.List;
public class GlideFromLettuceClusterAsyncExample {
    public static void main(String[] args) {
        System.out.println("Starting Glide example");
        // Create Redis Cluster Client
        // Establish a connection to the Redis cluster
        GlideClusterClient clusterClient = null; 
        try {
            clusterClient = createRecommendedClusterClient("localhost", 48300, false);
            // Perform Redis operations
            runRedisOperations(clusterClient).thenRun(() -> {
                System.out.println("All operations completed.");
            }).join();
            clusterClient.flushall().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace(); // Handle the exception or log it
        } finally {
            try {
                clusterClient.close(); // Handle ExecutionException
            } catch (ExecutionException e) {
                e.printStackTrace(); // Handle the exception or log it
            }
        }
    }

    private static CompletableFuture<Void> runRedisOperations(GlideClusterClient clusterClient) {
        // SET operation
        CompletableFuture<String> setFuture = clusterClient.set("key1", "value1");
        CompletableFuture<Void> setOp = CompletableFuture.supplyAsync(() -> {
            try {
                return setFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(result -> System.out.println("SET result: " + result));

        // GET operation
        CompletableFuture<String> getFuture = clusterClient.get("key1");
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
        CompletableFuture<String> msetFuture = clusterClient.mset(msetMap);
        CompletableFuture<Void> msetOp = CompletableFuture.supplyAsync(() -> {
            try {
                return msetFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(result -> System.out.println("MSET result: " + result));

        // PEXPIRE operation
        CompletableFuture<Boolean> pexpireFuture = clusterClient.pexpire("key1", 5000, ExpireOptions.HAS_NO_EXPIRY); // 5000ms = 5 seconds
        CompletableFuture<Void> pexpireOp = CompletableFuture.supplyAsync(() -> {
            try {
                return pexpireFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).thenAccept(result -> System.out.println("PEXPIRE result: " + result));

        // ZADD operation
        CompletableFuture<Long> zaddFuture = clusterClient.zadd("myzset", Map.of("member1", 1.0, "member2", 2.0));
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

    public static GlideClusterClient createRecommendedClusterClient(String host, int port, boolean with_ssl) throws InterruptedException, ExecutionException  {
        List<NodeAddress> nodeList =
            Collections.singletonList(NodeAddress.builder().host(host).port(port).build());

        GlideClusterClientConfiguration config =
            GlideClusterClientConfiguration.builder()
                    .addresses(nodeList)
                    // Enable this field if the servers are configured with TLS.
                    .useTLS(with_ssl)
                    .build();

        GlideClusterClient redisClusterClient = GlideClusterClient.createClient(config).get();

        return redisClusterClient;
    }
}
