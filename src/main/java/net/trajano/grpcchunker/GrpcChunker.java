package net.trajano.grpcchunker;

import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class GrpcChunker {

    /**
     * Assembles one stream object from the chunks.
     * This has a side effect of modifying the metaChunk reference with the next meta chunk or null.
     *
     * @param metaChunkRef   meta chunk reference.  This gets replaces with the next meta chunk or null.
     * @param chunkIterator  chunk iterator
     * @param metaPredicate  predicate that returns true if it is a meta chunk indicating a start of a new object.
     * @param objectSupplier this function gets the meta chunk and supplies a new object
     * @param combiner       this function takes the current entity state and the chunk and returns a copy of the combined result.  Note the combiner may modify the existing data, but may cause unexpected behaviour.
     * @param <T>            entity type
     * @param <R>            GRPC chunk message type
     * @return assembled entity
     */
    private static <T, R> T assembleOne(
            final AtomicReference<R> metaChunkRef,
            final Iterator<R> chunkIterator,
            final Predicate<R> metaPredicate,
            final Function<R, T> objectSupplier,
            final BiFunction<T, R, T> combiner
    ) {

        var o = objectSupplier.apply(metaChunkRef.getAndSet(null));

        while (chunkIterator.hasNext()) {
            final var chunk = chunkIterator.next();
            if (metaPredicate.test(chunk)) {
                metaChunkRef.set(chunk);
                break;
            }
            o = combiner.apply(o, chunk);
        }
        return o;
    }

    /**
     * Given an input stream of objects, it will create chunks for the response observer.
     *
     * @param input            stream
     * @param mapper           maps each object to a stream of chunks
     * @param responseObserver response observer
     * @param <T>              object type
     * @param <R>              chunk type
     */
    public static <T, R> void chunk(
            Stream<T> input,
            Function<T, Stream<R>> mapper,
            StreamObserver<R> responseObserver) {

        input
                .flatMap(mapper)
                .forEach(responseObserver::onNext);

    }

    /**
     * Given an input stream of objects, it will create chunks for the response observer.  This separates
     * the concerns of the meta from the data.
     *
     * @param input            stream
     * @param metaMapper       maps an object to build the meta chunk
     * @param dataMapper       maps an object to build the meta chunk
     * @param responseObserver response observer
     * @param <T>              object type
     * @param <R>              chunk type
     */
    public static <T, R> void chunk(
            Stream<T> input,
            Function<T, R> metaMapper,
            Function<T, Stream<R>> dataMapper,
            StreamObserver<R> responseObserver) {

        chunk(input, e ->
                        Stream.concat(Stream.of(metaMapper.apply(e)), dataMapper.apply(e)),
                responseObserver
        );

    }

    /**
     * @param chunkIterator  iterator of stream chunks.  This is the data that would be received.
     * @param metaPredicate  predicate that returns true if it is a meta chunk indicating a start of a new object.
     * @param objectSupplier this function gets the meta chunk and supplies a new object
     * @param combiner       this function takes the current entity state and the chunk and returns a copy of the combined result.
     * @param <T>            entity type
     * @param <R>            GRPC chunk message type
     * @return stream of entities
     */
    public static <T, R> Stream<T> dechunk(
            final Iterator<R> chunkIterator,
            final Predicate<R> metaPredicate,
            final Function<R, T> objectSupplier,
            final BiFunction<T, R, T> combiner) {

        if (!chunkIterator.hasNext()) {
            // empty case
            return Stream.empty();
        }
        final var lastChunkRef = new AtomicReference<>(chunkIterator.next());

        final var seed = assembleOne(lastChunkRef, chunkIterator, metaPredicate, objectSupplier, combiner);
        final var hasNextRef = new AtomicBoolean(true);
        return Stream.iterate(
                seed,
                o -> hasNextRef.get(),
                o -> {
                    if (lastChunkRef.get() == null) {
                        hasNextRef.set(false);
                    }
                    return assembleOne(lastChunkRef, chunkIterator, metaPredicate, objectSupplier, combiner);
                });

    }

    /**
     * Dechunks a GRPC stream from the request and calls the consumer when a complete object is created.  This stops
     * further processing once an error has occurred.
     *
     * @param metaPredicate    predicate that returns true if it is a meta chunk indicating a start of a new object.
     * @param objectSupplier   this function gets the meta chunk and supplies a new object
     * @param combiner         this function takes the current entity state and the chunk and returns a copy of the combined result.  Note the combiner may modify the existing data, but may cause unexpected behaviour.
     * @param consumer         a function that takes in the assembled object.
     * @param responseObserver GRPC response observer
     * @param <T>              entity type
     * @param <R>              GRPC chunk message type
     * @param <S>              GRPC message type for response streams
     * @return stream observer
     */
    public static <T, R, S> StreamObserver<R> dechunkingStreamObserver(
            final Predicate<R> metaPredicate,
            final Function<R, T> objectSupplier,
            final BiFunction<T, R, T> combiner,
            final Consumer<T> consumer,
            final StreamObserver<S> responseObserver
    ) {

        return new DechunkingStreamObserver<>(metaPredicate, objectSupplier, combiner, consumer, null, responseObserver);
    }

    /**
     * Dechunks a GRPC stream from the request and calls the consumer when a complete object is created.  This stops
     * further processing once an error has occurred.  This takes in a finalizer {@link Consumer} which may call
     * {@link StreamObserver#onNext(Object)} to provide a single value to the stream before completion.
     *
     * @param metaPredicate    predicate that returns true if it is a meta chunk indicating a start of a new object.
     * @param objectSupplier   this function gets the meta chunk and supplies a new object
     * @param combiner         this function takes the current entity state and the chunk and returns a copy of the combined result.  Note the combiner may modify the existing data, but may cause unexpected behaviour.
     * @param consumer         a function that takes in the assembled object.
     * @param finalizer        a consumer that takes in the current object but is only called on the final one.  May  be null.
     * @param responseObserver GRPC response observer
     * @param <T>              entity type
     * @param <R>              GRPC chunk message type
     * @param <S>              GRPC message type for response streams
     * @return stream observer
     */
    public static <T, R, S> StreamObserver<R> dechunkingStreamObserver(
            final Predicate<R> metaPredicate,
            final Function<R, T> objectSupplier,
            final BiFunction<T, R, T> combiner,
            final Consumer<T> consumer,
            final Consumer<T> finalizer,
            final StreamObserver<S> responseObserver
    ) {

        return new DechunkingStreamObserver<>(metaPredicate, objectSupplier, combiner, consumer, finalizer, responseObserver);
    }

    /**
     * Creates a stream observer that waits for a single and only a single value.
     *
     * @param clazz class of the value expected
     * @param <T>   type of value expected
     * @return stream observer
     */
    public static <T> SingleValueStreamObserver<T> singleValueStreamObserver(Class<T> clazz) {
        return new SingleValueStreamObserver<>();
    }

    /**
     * Dechunks a GRPC stream from the request and calls the consumer when a complete object is created.  This stops
     * further processing once an error has occurred.  This takes in a countdown latch that is countdown when the
     * processing completes or errors out.
     *
     * @param metaPredicate  predicate that returns true if it is a meta chunk indicating a start of a new object.
     * @param objectSupplier this function gets the meta chunk and supplies a new object
     * @param combiner       this function takes the current entity state and the chunk and returns a copy of the combined result.  Note the combiner may modify the existing data, but may cause unexpected behaviour.
     * @param consumer       a function that takes in the assembled object.
     * @param latch          a countdown latch of 1.
     * @param <T>            entity type
     * @param <R>            GRPC chunk message type
     * @return stream observer
     */
    public static <T, R> StreamObserver<R> dechunkingStreamObserver(
            final Predicate<R> metaPredicate,
            final Function<R, T> objectSupplier,
            final BiFunction<T, R, T> combiner,
            final Consumer<T> consumer,
            final CountDownLatch latch
    ) {

        return new LatchedDechunkingStreamObserver<>(metaPredicate, objectSupplier, combiner, consumer, latch);
    }

}
