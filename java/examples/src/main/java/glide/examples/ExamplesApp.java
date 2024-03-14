/** Copyright GLIDE-for-Redis Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.examples;

import glide.api.RedisClient;
import glide.api.RedisClusterClient;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.RedisClientConfiguration;
import glide.api.models.configuration.RedisClusterClientConfiguration;
import java.util.concurrent.ExecutionException;
import glide.api.models.configuration.RedisCredentials;

public class ExamplesApp {

    // main application entrypoint
    public static void main(String[] args) {
        runGlideStandaloneExamples();
        runGlideClusterExamples();
    }

    private static void runGlideStandaloneExamples() {
        String host = "localhost";
        Integer port = 14761;
        boolean useSsl = false;
        RedisClientConfiguration config =
                RedisClientConfiguration.builder()
                        .address(NodeAddress.builder().host(host).port(port).build())
                        .useTLS(useSsl)
                        .build();
        System.out.println("====> Starting Standalone test with config = " + host + ":" + port + " TLS=" + useSsl );

        try {
            RedisClient client = RedisClient.CreateClient(config).get();

            System.out.println("SET(apples, oranges): " + client.set("apples", "oranges").get());
            System.out.println("GET(apples): " + client.get("apples").get());
            System.out.println("CUSTOM COMMAND: " + client.customCommand(new String[]{ "CLIENT", "LIST"}).get());

            // Configure auth token
            String password = "somepassword";
            System.out.println("CUSTOM COMMAND set password: " + client.customCommand(new String[]{ "CONFIG", "SET", "requirepass", password}).get());
            // Create client with auth
            config =
                RedisClientConfiguration.builder()
                        .address(NodeAddress.builder().host(host).port(port).build())
                        .useTLS(useSsl)
                        .credentials(RedisCredentials.builder()
                            .password(password)
                            .build())
                        .build();
            RedisClient authClient = RedisClient.CreateClient(config).get();
            System.out.println("WITH AUTH: GET(apples): " + authClient.get("apples").get());
            // remove auth
            System.out.println("CUSTOM COMMAND remove auth: " + authClient.customCommand(new String[]{ "CONFIG", "SET", "requirepass", ""}).get());


        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Glide example failed with an exception: ");
            e.printStackTrace();
        }
    }
    private static void runGlideClusterExamples() {
        String host = "localhost";
        Integer port = 15679;
        boolean useSsl = false;

        NodeAddress address = NodeAddress.builder()
            .host(host)
            .port(port)
            .build();

        RedisClusterClientConfiguration config = RedisClusterClientConfiguration.builder()
            .address(address)
            .useTLS(useSsl)
            .build();
        System.out.println("\n\n====> Starting Cluster test with config = " + host + ":" + port + " TLS=" + useSsl );


        try {
            RedisClusterClient clusterClient = RedisClusterClient.CreateClient(config).get();

            System.out.println("SET(apples, oranges): " + clusterClient.set("apples", "oranges").get());
            System.out.println("GET(apples): " + clusterClient.get("apples").get());
            System.out.println("CUSTOM COMMAND: " + clusterClient.customCommand(new String[]{ "CLIENT", "LIST"}).get().getMultiValue());
            // Configure auth token
            String password = "somepassword";
            System.out.println("CUSTOM COMMAND: " + clusterClient.customCommand(new String[]{ "CONFIG", "SET", "requirepass", password}).get().getSingleValue());
            // Create client with auth
            config = RedisClusterClientConfiguration.builder()
                .address(address)
                .useTLS(useSsl)
                .credentials(RedisCredentials.builder()
                    .password(password)
                    .build())
                .build();
            RedisClusterClient authClusterClient = RedisClusterClient.CreateClient(config).get();
            System.out.println("WITH AUTH: GET(apples): " + authClusterClient.get("apples").get());
            // remove auth
            System.out.println("CUSTOM COMMAND remove auth: " + authClusterClient.customCommand(new String[]{ "CONFIG", "SET", "requirepass", ""}).get().getSingleValue());

        } catch (ExecutionException | InterruptedException e) {
            System.out.println("Glide example failed with an exception: ");
            e.printStackTrace();
        }
    }
}
