package net.trajano.grpcchunker;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Dechunks a GRPC stream from the request and calls the consumer when a complete object is created.
 * This stops further processing once an error has occurred.
 *
 * @param <T> entity type
 * @param <R> GRPC chunk message type
 * @param <S> GRPC message type for response streams
 */
class DechunkingStreamObserver<T, R, S> implements StreamObserver<R> {

  /**
   * This function takes the current entity state and the chunk and returns a copy of the combined
   * result. Note the combiner may modify the existing data, but may cause unexpected behaviour.
   */
  private final BiFunction<T, R, T> combiner;

  /** A function that takes in the assembled object. */
  private final Consumer<T> consumer;

  /**
   * A function that takes in the last assembled object. This may be null and the object passed into
   * it may also be null. This is called just before {@link #onCompleted()} is called.
   */
  private final Consumer<T> finalizer;

  /** Predicate that returns true if it is a meta chunk indicating a start of a new object. */
  private final Predicate<R> metaPredicate;

  /** this function gets the meta chunk and supplies a new object. */
  private final Function<R, T> objectSupplier;

  /** GRPC response observer. */
  private final StreamObserver<S> responseObserver;

  /** Currently being processed entity. */
  private T current;

  /**
   * In error state. Starts {@code false}, but once it is set to {@code true} it stops processing
   * {@link #onNext(Object)}.
   */
  private boolean inError;

  /**
   * @param metaPredicate predicate that returns true if it is a meta chunk indicating a start of a
   *     new object.
   * @param objectSupplier this function gets the meta chunk and supplies a new object
   * @param combiner this function takes the current entity state and the chunk and returns a copy
   *     of the combined result. Note the combiner may modify the existing data, but may cause
   *     unexpected behaviour.
   * @param consumer a function that takes in the assembled object.
   * @param finalizer a consumer that takes in the current object but is only called on the final
   *     one. May be null.
   * @param responseObserver GRPC response observer
   */
  DechunkingStreamObserver(
      final Predicate<R> metaPredicate,
      final Function<R, T> objectSupplier,
      final BiFunction<T, R, T> combiner,
      final Consumer<T> consumer,
      final Consumer<T> finalizer,
      final StreamObserver<S> responseObserver) {

    this.metaPredicate = metaPredicate;
    this.objectSupplier = objectSupplier;
    this.combiner = combiner;
    this.consumer = consumer;
    this.finalizer = finalizer;
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
      if (finalizer != null) {
        finalizer.accept(current);
      }
      responseObserver.onCompleted();
    } catch (final Exception e) {
      onError(e);
    }
  }

  /**
   * Transmits the error to the response observer and rethrows it if it is an unchecked exception,
   * otherwise it will wrap it
   *
   * @param throwable throwable
   */
  @Override
  public void onError(final Throwable throwable) {

    responseObserver.onError(throwable);
    inError = true;

    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    } else {
      throw Status.fromThrowable(throwable)
          .augmentDescription(throwable.getMessage())
          .asRuntimeException();
    }
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
