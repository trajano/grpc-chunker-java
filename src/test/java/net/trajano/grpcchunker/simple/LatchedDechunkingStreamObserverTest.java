package net.trajano.grpcchunker.simple;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import lombok.SneakyThrows;
import net.trajano.grpcchunker.GrpcChunker;
import org.junit.jupiter.api.Test;

class LatchedDechunkingStreamObserverTest {

  @Test
  @SneakyThrows
  void dechunkingStreamObserver() {

    final var latch = new CountDownLatch(1);

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "food", "bard");
    final var captured = new ArrayList<>();

    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) ->
                new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk),
            captured::add,
            latch);
    tape.forEach(requestObserver::onNext);
    requestObserver.onCompleted();
    latch.await();
    assertThat(captured)
        .containsExactly(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"),
            new SampleEntity().withMeta("_ 2").withData("foodbard"));
  }

  @Test
  @SneakyThrows
  void dechunkingStreamObserverEmpty() {

    final var latch = new CountDownLatch(1);
    final var captured = new ArrayList<>();

    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) ->
                new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk),
            captured::add,
            latch);
    requestObserver.onCompleted();
    assertThat(captured).isEmpty();
  }

  @Test
  void dechunkingStreamObserverWithError() {

    final var latch = new CountDownLatch(1);
    final var captured = new ArrayList<>();

    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) ->
                new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk),
            captured::add,
            latch);
    final var ex = new IllegalStateException("FOO");
    assertThatThrownBy(() -> requestObserver.onError(ex)).isInstanceOf(IllegalStateException.class);
    assertThat(captured).isEmpty();
  }

  @Test
  void dechunkingStreamObserverWithCheckedException() {

    final var latch = new CountDownLatch(1);
    final var captured = new ArrayList<>();

    final var requestObserver =
        GrpcChunker.dechunkingStreamObserver(
            chunk -> chunk.startsWith("_"),
            (String chunk) -> new SampleEntity().withMeta(chunk),
            (current, chunk) ->
                new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk),
            captured::add,
            latch);
    final var ex = new Exception("FOO");
    assertThatThrownBy(() -> requestObserver.onError(ex)).isInstanceOf(IllegalStateException.class);
    assertThat(captured).isEmpty();
  }

  @Test
  void dechunkingStreamObserverWithErrorAssembling() {

    final var latch = new CountDownLatch(1);
    final var captured = new ArrayList<>();

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "food");

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
            captured::add,
            latch);
    final var iterator = tape.iterator();
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    final var failingNext = iterator.next();
    assertThatThrownBy(() -> requestObserver.onNext(failingNext))
        .isInstanceOf(IllegalStateException.class);
    assertThat(captured)
        .containsExactly(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"));
  }

  @Test
  void dechunkingStreamObserverWithErrorAssemblingAtEnd() {

    final var latch = new CountDownLatch(1);
    final var captured = new ArrayList<>();

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "error here");
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
            },
            latch);
    tape.forEach(requestObserver::onNext);
    assertThatThrownBy(requestObserver::onCompleted).isInstanceOf(IllegalStateException.class);
    assertThat(captured)
        .containsExactly(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"));
  }

  @Test
  void dechunkingStreamObserverWithErrorAssemblingMidStream() {

    final var latch = new CountDownLatch(1);
    final var captured = new ArrayList<>();

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "food");
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
            captured::add,
            latch);
    final var iterator = tape.iterator();
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    requestObserver.onNext(iterator.next());
    final var failingNext = iterator.next();
    assertThatThrownBy(() -> requestObserver.onNext(failingNext))
        .isInstanceOf(IllegalStateException.class);
    assertThat(captured).containsExactly(new SampleEntity().withMeta("_ 0").withData("foobar"));
  }
}
