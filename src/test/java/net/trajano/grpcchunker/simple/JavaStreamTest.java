package net.trajano.grpcchunker.simple;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class JavaStreamTest {
  /** Tests understanding on how {@link Stream#iterate(Object, Predicate, UnaryOperator)} works. */
  @Test
  void streamIterate() {

    Predicate<Integer> hasNext = (i) -> i < 10;
    UnaryOperator<Integer> next = i -> i + 1;
    assertThat(Stream.iterate(0, hasNext, next).collect(Collectors.toList()))
        .asList()
        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  /** Tests understanding on how {@link Stream#iterate(Object, Predicate, UnaryOperator)} works. */
  @Test
  void streamIterateWithData() throws IOException {

    var tape = "Supercalifragilisticexpialidocious";
    try (var is = new StringReader(tape)) {
      final var last = new AtomicReference<>(is.read());
      final var hasNextRef = new AtomicBoolean(last.get() != -1);

      Predicate<Integer> hasNext = (i) -> hasNextRef.get();
      UnaryOperator<Integer> next =
          i -> {
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

  /** Tests understanding on how {@link Stream#iterate(Object, Predicate, UnaryOperator)} works. */
  @Test
  void streamIterateWithChunks() {

    var tape = IntStream.range(0, 10);

    var tapeIterator = tape.iterator();

    /*
     * take two elements at a time.
     */
    final BiFunction<AtomicReference<Integer>, PrimitiveIterator.OfInt, int[]> assembleOne =
        (ref, iterator) -> {
          var v0 = ref.get();
          if (v0 == null) {
            return null;
          }
          var v1 = iterator.next();
          if (iterator.hasNext()) {
            ref.set(iterator.next());
          } else {
            ref.set(null);
          }
          return new int[] {v0, v1};
        };

    final var lastRef = new AtomicReference<>(tapeIterator.next());

    final var seed = assembleOne.apply(lastRef, tapeIterator);
    final Predicate<int[]> hasNext = (val) -> val != null || lastRef.get() != null;
    assertThat(
            Stream.iterate(seed, hasNext, a -> assembleOne.apply(lastRef, tapeIterator))
                .limit(40)
                .flatMap(is -> Stream.of(is[0], is[1]))
                .map(i -> i + (int) 'a')
                .map(Character::toString)
                .collect(Collectors.joining()))
        .hasSize(10)
        .isEqualTo("abcdefghij");
  }
}
