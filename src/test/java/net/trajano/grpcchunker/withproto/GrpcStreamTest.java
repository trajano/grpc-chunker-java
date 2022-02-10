package net.trajano.grpcchunker.withproto;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GrpcStreamTest {

    private static Server server;
    private static Channel channel;

    @BeforeAll
    @SneakyThrows
    static void setupServer() {
        server = ServerBuilder.forPort(0)
                .addService(new GrpcStreamsServer())
                .build()
                .start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

    }

    @AfterAll
    @SneakyThrows
    static void teardownServer() {
        server
                .shutdown()
                .awaitTermination();
    }


    @Test
    @SneakyThrows
    void clientServer() {
        final var grpcStreamsClient = new GrpcStreamsClient(channel);
        var tape = Stream.of(
                new SampleEntity().withMeta("0").withData("FooBar"),
                new SampleEntity().withMeta("1").withData("FoodBard"),
                new SampleEntity().withMeta("2").withData("FoodBard")
        );
        var responseTape = grpcStreamsClient.sendEntities(tape).collect(Collectors.toList());
        assertThat(responseTape)
                .containsExactly(
                        new ResponseSampleEntity().withMeta("0").withData("FooBar"),
                        new ResponseSampleEntity().withMeta("1").withData("FoodBard"),
                        new ResponseSampleEntity().withMeta("2").withData("FoodBard")
                );
    }
}
