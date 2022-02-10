package net.trajano.grpcchunker;

import io.grpc.stub.StreamObserver;

import java.util.function.*;

/**
 * Dechunks a GRPC stream from the request and calls the consumer when a complete object is created.  This stops
 * further processing once an error has occurred.
 *
 * @param <T> entity type
 * @param <R> GRPC chunk message type
 * @param <S> GRPC message type for response streams
 */
class DechunkingStreamObserver<T, R, S> implements StreamObserver<R> {

    /**
     * This function takes the current entity state and the chunk and returns a copy of the combined result.  Note the combiner may modify the existing data, but may cause unexpected behaviour.
     */
    private final BiFunction<T, R, T> combiner;

    /**
     * A function that takes in the assembled object and the GRPC response observer.
     */
    private final Consumer<T> consumer;

    /**
     * Predicate that returns true if it is a meta chunk indicating a start of a new object.
     */
    private final Predicate<R> metaPredicate;

    /**
     * this function gets the meta chunk and supplies a new object.
     */
    private final Function<R, T> objectSupplier;

    /**
     * GRPC response observer.
     */
    private final StreamObserver<S> responseObserver;

    /**
     * Currently being processed entity.
     */
    private T current;

    /**
     * In error state.  Starts {@code false}, but once it is set to {@code true} it stops processing {@link #onNext(Object)}.
     */
    private boolean inError;

    /**
     * @param metaPredicate    predicate that returns true if it is a meta chunk indicating a start of a new object.
     * @param objectSupplier   this function gets the meta chunk and supplies a new object
     * @param combiner         this function takes the current entity state and the chunk and returns a copy of the combined result.  Note the combiner may modify the existing data, but may cause unexpected behaviour.
     * @param consumer         a function that takes in the assembled object.
     * @param responseObserver GRPC response observer
     */
    DechunkingStreamObserver(
            final Predicate<R> metaPredicate,
            final Function<R, T> objectSupplier,
            final BiFunction<T, R, T> combiner,
            final Consumer<T> consumer,
            final StreamObserver<S> responseObserver) {

        this.metaPredicate = metaPredicate;
        this.objectSupplier = objectSupplier;
        this.combiner = combiner;
        this.consumer = consumer;
        this.responseObserver = responseObserver;
        current = null;
        inError = false;
    }

    @Override
    public void onCompleted() {

        if (inError) {
            return;
        }
        try {
            if (current != null) {
                consumer.accept(current);
            }
            responseObserver.onCompleted();
        } catch (final Exception e) {
            onError(e);
        }

    }

    @Override
    public void onError(final Throwable throwable) {

        responseObserver.onError(throwable);
        inError = true;

    }

    @Override
    public void onNext(final R chunk) {

        if (inError) {
            return;
        }
        try {
            if (metaPredicate.test(chunk)) {
                if (current != null) {
                    consumer.accept(current);
                }
                current = objectSupplier.apply(chunk);
            } else {
                current = combiner.apply(current, chunk);
            }
        } catch (final Exception e) {
            onError(e);
        }
    }
}
