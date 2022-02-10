package net.trajano.grpcchunker.withproto;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class GrpcStreamTest {

  private static Server server;
  private static ManagedChannel channel;

  @BeforeAll
  @SneakyThrows
  static void setupServer() {
    final var name = InProcessServerBuilder.generateName();
    server =
        InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(new GrpcStreamsService())
            .build()
            .start();

    channel = InProcessChannelBuilder.forName(name).usePlaintext().build();
  }

  @AfterAll
  @SneakyThrows
  static void teardownServer() {
    channel.shutdown().awaitTermination(1, TimeUnit.MINUTES);
    server.shutdown().awaitTermination(1, TimeUnit.MINUTES);
  }

  @Test
  @SneakyThrows
  void sendEntities() {
    final var grpcStreamsClient = new GrpcStreamsClient(channel);
    var tape =
        Stream.of(
            new SampleEntity().withMeta("0").withData("FooBar"),
            new SampleEntity().withMeta("1").withData("FoodBard"),
            new SampleEntity().withMeta("2").withData("FoodBard"));
    var responseTape = grpcStreamsClient.sendEntities(tape).collect(Collectors.toList());
    assertThat(responseTape)
        .containsExactly(
            new ResponseSampleEntity().withMeta("0").withData("FooBar"),
            new ResponseSampleEntity().withMeta("1").withData("FoodBard"),
            new ResponseSampleEntity().withMeta("2").withData("FoodBard"));
  }

  @Test
  @SneakyThrows
  void download() {
    final var grpcStreamsClient = new GrpcStreamsClient(channel);
    var responseTape =
        grpcStreamsClient
            .download(new SampleEntity().withMeta("0").withData("FooBar"))
            .collect(Collectors.toList());
    assertThat(responseTape)
        .containsExactly(
            new ResponseSampleEntity().withMeta("0").withData("FooBar"),
            new ResponseSampleEntity().withMeta("0").withData("FooBar"),
            new ResponseSampleEntity().withMeta("0").withData("FooBar"));
  }

  @Test
  @SneakyThrows
  void downloadBlocking() {
    final var grpcStreamsClient = new GrpcStreamsClient(channel);
    var responseTape =
        grpcStreamsClient
            .downloadBlocking(new SampleEntity().withMeta("0").withData("FooBar"))
            .collect(Collectors.toList());
    assertThat(responseTape)
        .containsExactly(
            new ResponseSampleEntity().withMeta("0").withData("FooBar"),
            new ResponseSampleEntity().withMeta("0").withData("FooBar"),
            new ResponseSampleEntity().withMeta("0").withData("FooBar"));
  }

  @Test
  @SneakyThrows
  void upload() {
    final var grpcStreamsClient = new GrpcStreamsClient(channel);
    var tape =
        Stream.of(
            new SampleEntity().withMeta("0").withData("FooBar"),
            new SampleEntity().withMeta("1").withData("FoodBard"),
            new SampleEntity().withMeta("2").withData("FoodBard"));
    var response = grpcStreamsClient.upload(tape);
    assertThat(response)
        .isEqualTo(new ResponseSampleEntity().withMeta("2").withData("FooBarFoodBardFoodBard"));
  }

  @Test
  @SneakyThrows
  void uploadWithTimeout() {
    final var grpcStreamsClient = new GrpcStreamsClient(channel);
    var tape =
        Stream.of(
            new SampleEntity().withMeta("0").withData("FooBar"),
            new SampleEntity().withMeta("1").withData("FoodBard"),
            new SampleEntity().withMeta("2").withData("FoodBard"));
    var response = grpcStreamsClient.upload(tape, 600);
    assertThat(response)
        .isPresent()
        .get()
        .isEqualTo(new ResponseSampleEntity().withMeta("2").withData("FooBarFoodBardFoodBard"));
  }
}
