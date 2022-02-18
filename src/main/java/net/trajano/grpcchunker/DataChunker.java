package net.trajano.grpcchunker;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Provides a chunkData function that would take string or byte array data and chunks them into
 * streams of ByteString.
 */
public final class DataChunker {
  private DataChunker() {}

  public static Stream<ByteString> chunkData(final String s, final int size) {
    if (s.isEmpty()) {
      return Stream.empty();
    }
    var lastRef = new AtomicReference<>(sliceString(s, 0, size));
    final var pRef = new AtomicInteger(0);
    return Stream.iterate(
        lastRef.get(),
        c -> lastRef.get() != null,
        i -> {
          final var p = pRef.addAndGet(size);
          if (p < s.length()) {
            lastRef.set(sliceString(s, p, size));
          } else {
            lastRef.set(null);
          }
          return lastRef.get();
        });
  }

  public static Stream<ByteString> chunkData(final byte[] s, final int size) {
    if (s.length == 0) {
      return Stream.empty();
    }
    var lastRef = new AtomicReference<>(ByteString.copyFrom(s, 0, Math.min(size, s.length)));
    final var pRef = new AtomicInteger(0);
    return Stream.iterate(
        lastRef.get(),
        c -> lastRef.get() != null,
        i -> {
          final var p = pRef.addAndGet(size);
          if (p < s.length) {
            lastRef.set(ByteString.copyFrom(s, p, Math.min(size, s.length - p)));
          } else {
            lastRef.set(null);
          }
          return lastRef.get();
        });
  }

  public static Stream<ByteString> chunkData(final Reader is, final int size) throws IOException {
    final var buffer = new char[size];

    int c = is.read(buffer);
    if (c == -1) {
      return Stream.of();
    }

    var lastRef = new AtomicReference<>(ByteString.copyFromUtf8(new String(buffer, 0, c)));
    return Stream.iterate(
        lastRef.get(),
        i -> lastRef.get() != null,
        i -> {
          try {
            int r = is.read(buffer);
            if (r == -1) {
              lastRef.set(null);
            } else {
              lastRef.set(ByteString.copyFromUtf8(new String(buffer, 0, r)));
            }
            return lastRef.get();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  public static Stream<ByteString> chunkData(final InputStream is, final int size)
      throws IOException {
    final var buffer = new byte[size];

    int c = is.read(buffer);
    if (c == -1) {
      return Stream.of();
    }

    var lastRef = new AtomicReference<>(ByteString.copyFrom(buffer, 0, c));
    return Stream.iterate(
        lastRef.get(),
        i -> lastRef.get() != null,
        i -> {
          try {
            int r = is.read(buffer);
            if (r == -1) {
              lastRef.set(null);
            } else {
              lastRef.set(ByteString.copyFrom(buffer, 0, r));
            }
            return lastRef.get();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  private static ByteString sliceString(String s, int beginIndex, int size) {
    if (beginIndex + size < s.length()) {
      return ByteString.copyFromUtf8(s.substring(beginIndex, beginIndex + size));
    } else {
      return ByteString.copyFromUtf8(s.substring(beginIndex));
    }
  }
}
