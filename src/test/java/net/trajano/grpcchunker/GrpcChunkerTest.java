package net.trajano.grpcchunker;

import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


class GrpcChunkerTest {

    @Test
    void chunk() {

        final var observer = (StreamObserver<String>) mock(StreamObserver.class);
        final var tape = List.of(new SampleEntity().withMeta("_ 0").withData("foobar"),
                new SampleEntity().withMeta("_ 1").withData("foodbard"),
                new SampleEntity().withMeta("_ 2").withData("foodbard")
        );

        final var captor = ArgumentCaptor.forClass(String.class);

        GrpcChunker.chunk(tape.stream(), entity -> Stream.of(entity.getMeta(), entity.getData()), observer);
        verify(observer, times(6)).onNext(captor.capture());

        final var chunkedTape = captor.getAllValues();
        final var stream = GrpcChunker.dechunk(chunkedTape.iterator(), chunk -> chunk.startsWith("_"),
                        chunk -> new SampleEntity().withMeta(chunk),
                        (current, chunk) -> new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk))
                .limit(5);
        assertThat(stream.collect(Collectors.toList()))
                .isEqualTo(tape);

    }

    @Test
    @SuppressWarnings("unchecked")
    void chunk0() {

        final var observer = (StreamObserver<String>) mock(StreamObserver.class);

        final var captor = ArgumentCaptor.forClass(String.class);

        GrpcChunker.chunk(Stream.<SampleEntity>empty(), entity -> Stream.of(entity.getMeta(), entity.getData()), observer);
        verify(observer, never()).onNext(captor.capture());

        final var chunkedTape = captor.getAllValues();
        final var stream = GrpcChunker.dechunk(chunkedTape.iterator(), chunk -> chunk.startsWith("_"),
                        chunk -> new SampleEntity().withMeta(chunk),
                        (current, chunk) -> new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk))
                .limit(5);
        assertThat(stream.collect(Collectors.toList()))
                .isEmpty();

    }

    @Test
    @SuppressWarnings("unchecked")
    void chunkHigherLevel() {


        final var observer = (StreamObserver<String>) mock(StreamObserver.class);
        final var tape = List.of(new SampleEntity().withMeta("_ 0").withData("foobar"),
                new SampleEntity().withMeta("_ 1").withData("foodbard"),
                new SampleEntity().withMeta("_ 2").withData("foodbard")
        );

        final var captor = ArgumentCaptor.forClass(String.class);

        GrpcChunker.chunk(tape.stream(),
                SampleEntity::getMeta,
                entity -> Stream.of(entity.getData()),
                observer);
        verify(observer, times(6)).onNext(captor.capture());

        final var chunkedTape = captor.getAllValues();
        final var stream = GrpcChunker.dechunk(chunkedTape.iterator(), chunk -> chunk.startsWith("_"),
                        chunk -> new SampleEntity().withMeta(chunk),
                        (current, chunk) -> new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk))
                .limit(5);
        assertThat(stream.collect(Collectors.toList()))
                .isEqualTo(tape);

    }

    @Test
    void chunkHigherLevel2() {

        final var observer = (StreamObserver<String>) mock(StreamObserver.class);
        final var tape = List.of(new SampleEntity().withMeta("_ 0").withData("foobar"),
                new SampleEntity().withMeta("_ 1").withData("foodbard"),
                new SampleEntity().withMeta("_ 2").withData("foodbard")
        );

        final var captor = ArgumentCaptor.forClass(String.class);

        GrpcChunker.chunk(tape.stream(),
                SampleEntity::getMeta,
                entity -> Stream.of(entity.getData()),
                observer);
        verify(observer, times(6)).onNext(captor.capture());

        final var chunkedTape = captor.getAllValues();
        final var stream = GrpcChunker.dechunk(chunkedTape.iterator(), chunk -> chunk.startsWith("_"),
                        chunk -> new SampleEntity().withMeta(chunk),
                        (current, chunk) -> new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk))
                .limit(5);
        assertThat(stream.collect(Collectors.toList()))
                .isEqualTo(tape);

    }

    @Test
    void dechunk0() {

        // metas start with _
        final List<String> tape = List.of();
        var stream = GrpcChunker.dechunk(tape.iterator(), chunk -> chunk.startsWith("_"),
                        chunk -> new SampleEntity().withMeta(chunk),
                        (current, chunk) -> new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk))
                .limit(5);

        assertThat(stream.collect(Collectors.toList()))
                .asList()
                .isEmpty();
    }

    @Test
    void dechunk1() {


        // metas start with _
        final var tape = List.of(
                "_ 0",
                "foo",
                "bar");
        var stream = GrpcChunker.dechunk(tape.iterator(), chunk -> chunk.startsWith("_"),
                        chunk -> new SampleEntity().withMeta(chunk),
                        (current, chunk) -> new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk))
                .limit(5);

        assertThat(stream.collect(Collectors.toList()))
                .asList()
                .containsExactly(
                        new SampleEntity().withMeta("_ 0").withData("foobar")
                );
    }

    @Test
    void dechunk2() {


        // metas start with _
        final var tape = List.of(
                "_ 0",
                "foo",
                "bar",
                "_ 1",
                "food",
                "bard"
        );
        var stream = GrpcChunker.dechunk(tape.iterator(), chunk -> chunk.startsWith("_"),
                        chunk -> new SampleEntity().withMeta(chunk),
                        (current, chunk) -> new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk))
                .limit(5);

        assertThat(stream.collect(Collectors.toList()))
                .asList()
                .containsExactly(
                        new SampleEntity().withMeta("_ 0").withData("foobar"),
                        new SampleEntity().withMeta("_ 1").withData("foodbard")
                );
    }

    @Test
    void dechunk3() {


        // metas start with _
        final var tape = List.of(
                "_ 0",
                "foo",
                "bar",
                "_ 1",
                "food",
                "bard",
                "_ 2",
                "food",
                "bard"
        );
        var stream = GrpcChunker.dechunk(
                        tape.iterator(),
                        chunk -> chunk.startsWith("_"),
                        chunk -> new SampleEntity().withMeta(chunk),
                        (current, chunk) -> new SampleEntity().withMeta(current.getMeta()).withData(current.getData() + chunk))
                .limit(5);

        assertThat(stream.collect(Collectors.toList()))
                .asList()
                .containsExactly(
                        new SampleEntity().withMeta("_ 0").withData("foobar"),
                        new SampleEntity().withMeta("_ 1").withData("foodbard"),
                        new SampleEntity().withMeta("_ 2").withData("foodbard")
                );
    }

    @Test
    void dechunkingStreamObserver() {

        final StreamObserver<SampleEntity> responseObserver = mock(StreamObserver.class);

        // metas start with _
        final var tape = List.of(
                "_ 0",
                "foo",
                "bar",
                "_ 1",
                "food",
                "bard",
                "_ 2",
                "food",
                "bard"
        );
        final var captured = new ArrayList<>();
        final var requestObserver = GrpcChunker.dechunkingStreamObserver(
                chunk -> chunk.startsWith("_"),
                (String chunk) -> new SampleEntity()
                        .withMeta(chunk),
                (current, chunk) -> new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk),
                (o, ro) -> {
                    captured.add(o);
                    ro.onNext(o);
                },
                responseObserver
        );
        tape.forEach(requestObserver::onNext);
        requestObserver.onCompleted();
        var inOrder = inOrder(responseObserver);
        inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 0").withData("foobar"));
        inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 1").withData("foodbard"));
        inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 2").withData("foodbard"));
        inOrder.verify(responseObserver).onCompleted();
        assertThat(captured)
                .containsExactly(
                        new SampleEntity().withMeta("_ 0").withData("foobar"),
                        new SampleEntity().withMeta("_ 1").withData("foodbard"),
                        new SampleEntity().withMeta("_ 2").withData("foodbard")
                );

    }

    @Test
    void dechunkingStreamObserverEmpty() {

        final var responseObserver = (StreamObserver<SampleEntity>) mock(StreamObserver.class);

        final var requestObserver = GrpcChunker.dechunkingStreamObserver(
                chunk -> chunk.startsWith("_"),
                (String chunk) -> new SampleEntity()
                        .withMeta(chunk),
                (current, chunk) -> new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk),
                (o, ro) -> ro.onNext(o),
                responseObserver
        );
        requestObserver.onCompleted();
        verify(responseObserver, never()).onNext(any());
        verify(responseObserver, never()).onError(any());
        verify(responseObserver, times(1)).onCompleted();

    }

    @Test
    void dechunkingStreamObserverWithError() {

        final var responseObserver = (StreamObserver<SampleEntity>) mock(StreamObserver.class);

        final var requestObserver = GrpcChunker.dechunkingStreamObserver(
                chunk -> chunk.startsWith("_"),
                (String chunk) -> new SampleEntity()
                        .withMeta(chunk),
                (current, chunk) -> new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk),
                (o, ro) -> ro.onNext(o),
                responseObserver
        );
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
        final var tape = List.of(
                "_ 0",
                "foo",
                "bar",
                "_ 1",
                "food",
                "bard",
                "_ 2",
                "food"
        );
        final var captured = new ArrayList<>();
        final var requestObserver = GrpcChunker.dechunkingStreamObserver(
                chunk -> chunk.startsWith("_"),
                (String chunk) -> new SampleEntity()
                        .withMeta(chunk),
                (current, chunk) -> {
                    if (current.getMeta().equals("_ 2")) {
                        throw new IllegalStateException("BLAH");
                    }
                    return new SampleEntity()
                            .withMeta(current.getMeta())
                            .withData(current.getData() + chunk);
                },
                (o, ro) -> {
                    captured.add(o);
                    ro.onNext(o);
                },
                responseObserver
        );
        tape.forEach(requestObserver::onNext);
        requestObserver.onCompleted();
        var inOrder = inOrder(responseObserver);
        inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 0").withData("foobar"));
        inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 1").withData("foodbard"));
        inOrder.verify(responseObserver).onError(any(IllegalStateException.class));
        verify(responseObserver, never()).onCompleted();
        assertThat(captured)
                .containsExactly(
                        new SampleEntity().withMeta("_ 0").withData("foobar"),
                        new SampleEntity().withMeta("_ 1").withData("foodbard")
                );

    }

    @Test
    void dechunkingStreamObserverWithErrorAssemblingAtEnd() {

        final StreamObserver<SampleEntity> responseObserver = mock(StreamObserver.class);

        // metas start with _
        final var tape = List.of(
                "_ 0",
                "foo",
                "bar",
                "_ 1",
                "food",
                "bard",
                "_ 2",
                "error here"
        );
        final var captured = new ArrayList<>();
        final var requestObserver = GrpcChunker.dechunkingStreamObserver(
                chunk -> chunk.startsWith("_"),
                (String chunk) -> new SampleEntity()
                        .withMeta(chunk),
                (current, chunk) -> new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData() + chunk),
                (o, ro) -> {
                    if ("error here".equals(o.getData())) {
                        throw new IllegalStateException("BLAH");
                    }
                    captured.add(o);
                    ro.onNext(o);
                },
                responseObserver
        );
        tape.forEach(requestObserver::onNext);
        requestObserver.onCompleted();
        var inOrder = inOrder(responseObserver);
        inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 0").withData("foobar"));
        inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 1").withData("foodbard"));
        inOrder.verify(responseObserver).onError(any(IllegalStateException.class));
        verify(responseObserver, never()).onCompleted();
        assertThat(captured)
                .containsExactly(
                        new SampleEntity().withMeta("_ 0").withData("foobar"),
                        new SampleEntity().withMeta("_ 1").withData("foodbard")
                );

    }

    @Test
    void dechunkingStreamObserverWithErrorAssemblingMidStream() {

        final StreamObserver<SampleEntity> responseObserver = mock(StreamObserver.class);

        // metas start with _
        final var tape = List.of(
                "_ 0",
                "foo",
                "bar",
                "_ 1",
                "food",
                "bard",
                "_ 2",
                "food"
        );
        final var captured = new ArrayList<>();
        final var requestObserver = GrpcChunker.dechunkingStreamObserver(
                chunk -> chunk.startsWith("_"),
                (String chunk) -> new SampleEntity()
                        .withMeta(chunk),
                (current, chunk) -> {
                    if (current.getMeta().equals("_ 1")) {
                        throw new IllegalStateException("BLAH");
                    }
                    return new SampleEntity()
                            .withMeta(current.getMeta())
                            .withData(current.getData() + chunk);
                },
                (o, ro) -> {
                    captured.add(o);
                    ro.onNext(o);
                },
                responseObserver
        );
        tape.forEach(requestObserver::onNext);
        var inOrder = inOrder(responseObserver);
        inOrder.verify(responseObserver).onNext(new SampleEntity().withMeta("_ 0").withData("foobar"));
        inOrder.verify(responseObserver).onError(any(IllegalStateException.class));
        verify(responseObserver, never()).onCompleted();
        assertThat(captured)
                .containsExactly(
                        new SampleEntity().withMeta("_ 0").withData("foobar")
                );

    }

    /**
     * Tests undersanding on how stream.iterate works.
     */
    @Test
    void streamIterate() {

        Predicate<Integer> hasNext = (i) -> i < 10;
        UnaryOperator<Integer> next = i -> i + 1;
        assertThat(Stream.iterate(0, hasNext, next)
                .collect(Collectors.toList()))
                .asList()
                .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    /**
     * Tests undersanding on how stream.iterate works.
     */
    @Test
    void streamIterateWithData() throws IOException {

        var tape = "Supercalifragilisticexpialidocious";
        try (var is = new StringReader(tape)) {
            final var last = new AtomicReference<>(is.read());
            final var hasNextRef = new AtomicBoolean(last.get() != -1);

            Predicate<Integer> hasNext = (i) -> hasNextRef.get();
            UnaryOperator<Integer> next = i -> {
                try {
                    last.set(is.read());
                    if (last.get() == -1) {
                        hasNextRef.set(false);
                    }
                    return last.get();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            };
            assertThat(
                    Stream.iterate(last.get(), hasNext, next)
                            .limit(40)
                            .map(Character::toString)
                            .collect(Collectors.joining()))
                    .isEqualTo(tape);
        }
    }

    @Data
    @With
    @NoArgsConstructor
    @AllArgsConstructor
    private static class SampleEntity {
        private String data = "";

        private String meta;
    }
}
