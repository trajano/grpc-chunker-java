package net.trajano.grpcchunker;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SingleValueStreamObserver<T> implements StreamObserver<T> {
  final AtomicReference<T> ref = new AtomicReference<>();
  final CountDownLatch latch = new CountDownLatch(1);

  @Override
  public void onNext(T value) {
    if (ref.get() != null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("Only expecting one call for onNext")
          .asRuntimeException();
    }
    ref.set(value);
  }

  @Override
  public void onError(Throwable throwable) {
    latch.countDown();
    if (throwable instanceof RuntimeException) {
      throw (RuntimeException) throwable;
    } else {
      throw Status.fromThrowable(throwable).asRuntimeException();
    }
  }

  @Override
  public void onCompleted() {
    latch.countDown();
  }

  public void await() throws InterruptedException {
    latch.await();
  }

  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    return latch.await(timeout, unit);
  }

  public Optional<T> getValue() {
    return Optional.ofNullable(ref.get());
  }
}
