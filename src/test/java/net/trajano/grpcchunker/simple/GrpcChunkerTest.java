package net.trajano.grpcchunker.simple;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.trajano.grpcchunker.GrpcChunker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GrpcChunkerTest {

  @Test
  void chunk(@Mock StreamObserver<String> observer) {

    final var tape =
        List.of(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"),
            new SampleEntity().withMeta("_ 2").withData("foodbard"));

    final var captor = ArgumentCaptor.forClass(String.class);

    GrpcChunker.chunk(
        tape.stream(), entity -> Stream.of(entity.getMeta(), entity.getData()), observer);
    verify(observer, times(6)).onNext(captor.capture());

    final var chunkedTape = captor.getAllValues();
    final var stream =
        GrpcChunker.dechunk(
                chunkedTape.iterator(),
                chunk -> chunk.startsWith("_"),
                chunk -> new SampleEntity().withMeta(chunk),
                (current, chunk) ->
                    new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk))
            .limit(5);
    assertThat(stream.collect(Collectors.toList())).isEqualTo(tape);
  }

  @Test
  @SuppressWarnings("unchecked")
  void chunk0(@Mock StreamObserver<String> observer) {

    final var captor = ArgumentCaptor.forClass(String.class);

    GrpcChunker.chunk(
        Stream.<SampleEntity>empty(),
        entity -> Stream.of(entity.getMeta(), entity.getData()),
        observer);
    verify(observer, never()).onNext(captor.capture());

    final var chunkedTape = captor.getAllValues();
    final var stream =
        GrpcChunker.dechunk(
                chunkedTape.iterator(),
                chunk -> chunk.startsWith("_"),
                chunk -> new SampleEntity().withMeta(chunk),
                (current, chunk) ->
                    new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk))
            .limit(5);
    assertThat(stream.collect(Collectors.toList())).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  void chunkHigherLevel(@Mock StreamObserver<String> observer) {

    final var tape =
        List.of(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"),
            new SampleEntity().withMeta("_ 2").withData("foodbard"));

    final var captor = ArgumentCaptor.forClass(String.class);

    GrpcChunker.chunk(
        tape.stream(), SampleEntity::getMeta, entity -> Stream.of(entity.getData()), observer);
    verify(observer, times(6)).onNext(captor.capture());

    final var chunkedTape = captor.getAllValues();
    final var stream =
        GrpcChunker.dechunk(
                chunkedTape.iterator(),
                chunk -> chunk.startsWith("_"),
                chunk -> new SampleEntity().withMeta(chunk),
                (current, chunk) ->
                    new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk))
            .limit(5);
    assertThat(stream.collect(Collectors.toList())).isEqualTo(tape);
  }

  @Test
  void chunkHigherLevel2(@Mock StreamObserver<String> observer) {

    final var tape =
        List.of(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"),
            new SampleEntity().withMeta("_ 2").withData("foodbard"));

    final var captor = ArgumentCaptor.forClass(String.class);

    GrpcChunker.chunk(
        tape.stream(), SampleEntity::getMeta, entity -> Stream.of(entity.getData()), observer);
    verify(observer, times(6)).onNext(captor.capture());

    final var chunkedTape = captor.getAllValues();
    final var stream =
        GrpcChunker.dechunk(
                chunkedTape.iterator(),
                chunk -> chunk.startsWith("_"),
                chunk -> new SampleEntity().withMeta(chunk),
                (current, chunk) ->
                    new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk))
            .limit(5);
    assertThat(stream.collect(Collectors.toList())).isEqualTo(tape);
  }

  @Test
  void dechunk0() {

    // metas start with _
    final List<String> tape = List.of();
    var stream =
        GrpcChunker.dechunk(
                tape.iterator(),
                chunk -> chunk.startsWith("_"),
                chunk -> new SampleEntity().withMeta(chunk),
                (current, chunk) ->
                    new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk))
            .limit(5);

    assertThat(stream.collect(Collectors.toList())).asList().isEmpty();
  }

  @Test
  void dechunk1() {

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar");
    var stream =
        GrpcChunker.dechunk(
                tape.iterator(),
                chunk -> chunk.startsWith("_"),
                chunk -> new SampleEntity().withMeta(chunk),
                (current, chunk) ->
                    new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk))
            .limit(5);

    assertThat(stream.collect(Collectors.toList()))
        .asList()
        .containsExactly(new SampleEntity().withMeta("_ 0").withData("foobar"));
  }

  @Test
  void dechunk2() {

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard");
    var stream =
        GrpcChunker.dechunk(
                tape.iterator(),
                chunk -> chunk.startsWith("_"),
                chunk -> new SampleEntity().withMeta(chunk),
                (current, chunk) ->
                    new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk))
            .limit(5);

    assertThat(stream.collect(Collectors.toList()))
        .asList()
        .containsExactly(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"));
  }

  @Test
  void dechunk3() {

    // metas start with _
    final var tape = List.of("_ 0", "foo", "bar", "_ 1", "food", "bard", "_ 2", "food", "bard");
    var stream =
        GrpcChunker.dechunk(
                tape.iterator(),
                chunk -> chunk.startsWith("_"),
                chunk -> new SampleEntity().withMeta(chunk),
                (current, chunk) ->
                    new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk))
            .limit(5);

    assertThat(stream.collect(Collectors.toList()))
        .asList()
        .containsExactly(
            new SampleEntity().withMeta("_ 0").withData("foobar"),
            new SampleEntity().withMeta("_ 1").withData("foodbard"),
            new SampleEntity().withMeta("_ 2").withData("foodbard"));
  }
}
