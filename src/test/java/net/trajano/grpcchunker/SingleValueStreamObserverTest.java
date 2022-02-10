package net.trajano.grpcchunker;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class SingleValueStreamObserverTest {

  @Test
  void failDoubleOnNext() {
    final var stringSingleValueStreamObserver = GrpcChunker.singleValueStreamObserver(String.class);
    stringSingleValueStreamObserver.onNext("Hello");
    assertThatThrownBy(() -> stringSingleValueStreamObserver.onNext("World"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void failOnError() {
    final var stringSingleValueStreamObserver = GrpcChunker.singleValueStreamObserver(String.class);
    stringSingleValueStreamObserver.onNext("Hello");
    final var ex = new IllegalArgumentException();
    assertThatThrownBy(() -> stringSingleValueStreamObserver.onError(ex))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void failOnCheckedException() {
    final var stringSingleValueStreamObserver = GrpcChunker.singleValueStreamObserver(String.class);
    stringSingleValueStreamObserver.onNext("Hello");
    final var ex = new Exception();
    assertThatThrownBy(() -> stringSingleValueStreamObserver.onError(ex))
        .isInstanceOf(IllegalStateException.class);
  }
}
