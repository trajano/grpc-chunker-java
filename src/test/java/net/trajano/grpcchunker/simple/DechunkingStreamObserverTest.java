package net.trajano.grpcchunker.simple;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import net.trajano.grpcchunker.GrpcChunker;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DechunkingStreamObserverTest {

  @Test
  void dechunkingStreamObserver() {

    final StreamObserver<SampleEntity> responseObserver = mock(StreamObserver.class);

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "food", "bard");
    final var captured = new ArrayList<>();
    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) ->
                new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk),
            (o) -> {
              captured.add(o);
              responseObserver.onNext(o);
            },
            responseObserver);
    tape.forEach(requestObserver::onNext);
    requestObserver.onCompleted();
    var inOrder = inOrder(responseObserver);
    inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 0").withData("foobar"));
    inOrder
        .verify(responseObserver)
        .onNext(new SampleEntity().withMeta("_ 1").withData("foodbard"));
    inOrder
        .verify(responseObserver)
        .onNext(new SampleEntity().withMeta("_ 2").withData("foodbard"));
    inOrder.verify(responseObserver).onCompleted();
    assertThat(captured)
        .containsExactly(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"),
            new SampleEntity().withMeta("_ 2").withData("foodbard"));
  }

  @Test
  void dechunkingStreamObserverEmpty() {

    final var responseObserver = (StreamObserver<SampleEntity>) mock(StreamObserver.class);

    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) ->
                new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk),
            responseObserver::onNext,
            responseObserver);
    requestObserver.onCompleted();
    verify(responseObserver, never()).onNext(any());
    verify(responseObserver, never()).onError(any());
    verify(responseObserver, times(1)).onCompleted();
  }

  @Test
  void dechunkingStreamObserverWithError() {

    final var responseObserver = (StreamObserver<SampleEntity>) mock(StreamObserver.class);

    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) ->
                new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk),
            responseObserver::onNext,
            responseObserver);
    requestObserver.onError(new IllegalStateException("FOO"));
    verify(responseObserver, never()).onNext(any());
    verify(responseObserver, never()).onCompleted();

    var captor = ArgumentCaptor.forClass(IllegalStateException.class);
    verify(responseObserver).onError(captor.capture());
    assertThat(captor.getValue().getMessage()).isEqualTo("FOO");
  }

  @Test
  void dechunkingStreamObserverWithErrorAssembling() {

    final StreamObserver<SampleEntity> responseObserver = mock(StreamObserver.class);

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "food");
    final var captured = new ArrayList<>();
    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) -> {
              if (current.getMeta().equals("_ 2")) {
                throw new IllegalStateException("BLAH");
              }
              return new SampleEntity()
                  .withMeta(current.getMeta())
                  .withData(current.getData() + chunk);
            },
            (o) -> {
              captured.add(o);
              responseObserver.onNext(o);
            },
            responseObserver);
    tape.forEach(requestObserver::onNext);
    requestObserver.onCompleted();
    var inOrder = inOrder(responseObserver);
    inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 0").withData("foobar"));
    inOrder
        .verify(responseObserver)
        .onNext(new SampleEntity().withMeta("_ 1").withData("foodbard"));
    inOrder.verify(responseObserver).onError(any(IllegalStateException.class));
    verify(responseObserver, never()).onCompleted();
    assertThat(captured)
        .containsExactly(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"));
  }

  @Test
  void dechunkingStreamObserverWithErrorAssemblingAtEnd() {

    final StreamObserver<SampleEntity> responseObserver = mock(StreamObserver.class);

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "error here");
    final var captured = new ArrayList<>();
    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) ->
                new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk),
            (o) -> {
              if ("error here".equals(o.getData())) {
                throw new IllegalStateException("BLAH");
              }
              captured.add(o);
              responseObserver.onNext(o);
            },
            responseObserver);
    tape.forEach(requestObserver::onNext);
    requestObserver.onCompleted();
    var inOrder = inOrder(responseObserver);
    inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 0").withData("foobar"));
    inOrder
        .verify(responseObserver)
        .onNext(new SampleEntity().withMeta("_ 1").withData("foodbard"));
    inOrder.verify(responseObserver).onError(any(IllegalStateException.class));
    verify(responseObserver, never()).onCompleted();
    assertThat(captured)
        .containsExactly(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"));
  }

  @Test
  void dechunkingStreamObserverWithErrorAssemblingMidStream() {

    final StreamObserver<SampleEntity> responseObserver = mock(StreamObserver.class);

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "food");
    final var captured = new ArrayList<>();
    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) -> {
              if (current.getMeta().equals("_ 1")) {
                throw new IllegalStateException("BLAH");
              }
              return new SampleEntity()
                  .withMeta(current.getMeta())
                  .withData(current.getData() + chunk);
            },
            (o) -> {
              captured.add(o);
              responseObserver.onNext(o);
            },
            responseObserver);
    tape.forEach(requestObserver::onNext);
    var inOrder = inOrder(responseObserver);
    inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 0").withData("foobar"));
    inOrder.verify(responseObserver).onError(any(IllegalStateException.class));
    verify(responseObserver, never()).onCompleted();
    assertThat(captured).containsExactly(new SampleEntity().withMeta("_ 0").withData("foobar"));
  }
}
