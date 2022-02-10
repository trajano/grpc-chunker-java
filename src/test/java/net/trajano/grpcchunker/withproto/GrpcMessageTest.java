package net.trajano.grpcchunker.withproto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import net.trajano.grpcchunker.GrpcChunker;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormMeta;
import org.junit.jupiter.api.Test;

class GrpcMessageTest {

  @Test
  void dechunk() {
    final var fooBar = new ResponseSampleEntity().withMeta("0").withData("FooBar");
    final var metaChunk = fooBar.toMetaChunk();
    final var dataChunk =
        ResponseFormChunk.newBuilder().setData(ByteString.copyFromUtf8(fooBar.getData())).build();
    final var tape = List.of(metaChunk, dataChunk, metaChunk, dataChunk, metaChunk, dataChunk);
    final var dechunkedStream =
        GrpcChunker.dechunk(
            tape.iterator(),
            ResponseFormChunk::hasMeta,
            ResponseSampleEntity::buildFromMetaChunk,
            ResponseSampleEntity::combineWithAddedDataChunk);
    assertThat(dechunkedStream)
        .containsExactly(
            new ResponseSampleEntity().withMeta("0").withData("FooBar"),
            new ResponseSampleEntity().withMeta("0").withData("FooBar"),
            new ResponseSampleEntity().withMeta("0").withData("FooBar"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void dechunkingStreamObserver() {

    final StreamObserver<SampleEntity> responseObserver = mock(StreamObserver.class);

    // metas start with _
    final var tape =
        List.of(
            SavedFormChunk.newBuilder()
                .setMeta(SavedFormMeta.newBuilder().setId("0").build())
                .build(),
            SavedFormChunk.newBuilder()
                .setData(ByteString.copyFrom("Foo", StandardCharsets.UTF_8))
                .build(),
            SavedFormChunk.newBuilder()
                .setData(ByteString.copyFrom("Bar", StandardCharsets.UTF_8))
                .build(),
            SavedFormChunk.newBuilder()
                .setMeta(SavedFormMeta.newBuilder().setId("1").build())
                .build(),
            SavedFormChunk.newBuilder()
                .setData(ByteString.copyFrom("Food", StandardCharsets.UTF_8))
                .build(),
            SavedFormChunk.newBuilder()
                .setData(ByteString.copyFrom("Bard", StandardCharsets.UTF_8))
                .build(),
            SavedFormChunk.newBuilder()
                .setMeta(SavedFormMeta.newBuilder().setId("2").build())
                .build(),
            SavedFormChunk.newBuilder()
                .setData(ByteString.copyFrom("Food", StandardCharsets.UTF_8))
                .build(),
            SavedFormChunk.newBuilder()
                .setData(ByteString.copyFrom("Bard", StandardCharsets.UTF_8))
                .build());
    final var captured = new ArrayList<>();
    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            SavedFormChunk::hasMeta,
            (SavedFormChunk chunk) -> new SampleEntity().withMetaChunk(chunk),
            (current, chunk) ->
                new SampleEntity()
                    .withMeta(current.getMeta())
                    .withData(current.getData())
                    .withDataChunkAdded(chunk),
            (o) -> {
              captured.add(o);
              responseObserver.onNext(o);
            },
            responseObserver);
    tape.forEach(requestObserver::onNext);
    requestObserver.onCompleted();
    var inOrder = inOrder(responseObserver);
    inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("0").withData("FooBar"));
    inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("1").withData("FoodBard"));
    inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("2").withData("FoodBard"));
    inOrder.verify(responseObserver).onCompleted();
    assertThat(captured)
        .containsExactly(
            new SampleEntity().withMeta("0").withData("FooBar"),
            new SampleEntity().withMeta("1").withData("FoodBard"),
            new SampleEntity().withMeta("2").withData("FoodBard"));
  }
}
