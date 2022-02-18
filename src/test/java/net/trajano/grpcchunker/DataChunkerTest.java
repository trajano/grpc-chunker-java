package net.trajano.grpcchunker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

class DataChunkerTest {

  @Test
  void chunkData() {
    assertThat(DataChunker.chunkData("hello world", 3).limit(40))
        .containsExactly(
            ByteString.copyFromUtf8("hel"),
            ByteString.copyFromUtf8("lo "),
            ByteString.copyFromUtf8("wor"),
            ByteString.copyFromUtf8("ld"));
  }

  @Test
  void chunkDataByteStream() throws IOException {
    assertThat(
            DataChunker.chunkData(
                    new ByteArrayInputStream(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), 3)
                .limit(40))
        .containsExactly(
            ByteString.copyFrom(new byte[] {0, 1, 2}),
            ByteString.copyFrom(new byte[] {3, 4, 5}),
            ByteString.copyFrom(new byte[] {6, 7, 8}),
            ByteString.copyFrom(new byte[] {9, 10, 11}));
  }

  @Test
  void chunkDataBytes() {
    assertThat(
            DataChunker.chunkData(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, 3)
                .limit(40))
        .containsExactly(
            ByteString.copyFrom(new byte[] {0, 1, 2}),
            ByteString.copyFrom(new byte[] {3, 4, 5}),
            ByteString.copyFrom(new byte[] {6, 7, 8}),
            ByteString.copyFrom(new byte[] {9, 10, 11}),
            ByteString.copyFrom(new byte[] {12}));
  }

  @Test
  void chunkDataEmpty() {
    assertThat(DataChunker.chunkData("", 30).limit(40)).isEmpty();
  }

  @Test
  void chunkDataEmptyBytes() {
    assertThat(DataChunker.chunkData(new byte[0], 30).limit(40)).isEmpty();
  }

  @Test
  void chunkDataEmptyReader() throws IOException {
    assertThat(DataChunker.chunkData(new StringReader(""), 30).limit(40)).isEmpty();
  }

  @Test
  void chunkDataEmptyStream() throws IOException {
    assertThat(DataChunker.chunkData(new ByteArrayInputStream(new byte[0]), 30).limit(40))
        .isEmpty();
  }

  @Test
  void chunkDataExactByteStream() throws IOException {
    assertThat(
            DataChunker.chunkData(
                    new ByteArrayInputStream(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}), 3)
                .limit(40))
        .containsExactly(
            ByteString.copyFrom(new byte[] {0, 1, 2}),
            ByteString.copyFrom(new byte[] {3, 4, 5}),
            ByteString.copyFrom(new byte[] {6, 7, 8}),
            ByteString.copyFrom(new byte[] {9}));
  }

  @Test
  void chunkDataExactByteStreamWithError() throws IOException {
    final var is = spy(new ByteArrayInputStream(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    doCallRealMethod()
        .doCallRealMethod()
        .doCallRealMethod()
        .doThrow(new IOException("FOO"))
        .when(is)
        .read(any(byte[].class));
    final var byteStringStream = DataChunker.chunkData(is, 3);
    final var iterator = byteStringStream.iterator();
    assertThat(iterator.next()).hasSize(3).isEqualTo(ByteString.copyFrom(new byte[] {0, 1, 2}));
    assertThat(iterator.next()).hasSize(3).isEqualTo(ByteString.copyFrom(new byte[] {3, 4, 5}));
    assertThat(iterator.next()).hasSize(3).isEqualTo(ByteString.copyFrom(new byte[] {6, 7, 8}));

    assertThatThrownBy(iterator::next).isInstanceOf(UncheckedIOException.class);
  }

  @Test
  void chunkDataExactBytes() {
    assertThat(
            DataChunker.chunkData(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 3).limit(40))
        .containsExactly(
            ByteString.copyFrom(new byte[] {0, 1, 2}),
            ByteString.copyFrom(new byte[] {3, 4, 5}),
            ByteString.copyFrom(new byte[] {6, 7, 8}),
            ByteString.copyFrom(new byte[] {9, 10, 11}));
  }

  @Test
  void chunkDataOneLarge() {
    assertThat(DataChunker.chunkData("hello world!", 30).limit(40))
        .containsExactly(ByteString.copyFromUtf8("hello world!"));
  }

  @Test
  void chunkDataReaderWithError() throws IOException {
    final var is = spy(new StringReader("HALLO WORLD"));
    doReturn(3, 3, 3).doThrow(new IOException("FOO")).when(is).read(any(char[].class));
    final var byteStringStream = DataChunker.chunkData(is, 3);
    final var iterator = byteStringStream.iterator();
    assertThat(iterator.next()).hasSize(3);
    assertThat(iterator.next()).hasSize(3);
    assertThat(iterator.next()).hasSize(3);

    assertThatThrownBy(iterator::next).isInstanceOf(UncheckedIOException.class);
  }

  @Test
  void chunkDataUsingReader() throws IOException {
    assertThat(DataChunker.chunkData(new StringReader("hello world"), 3).limit(40))
        .containsExactly(
            ByteString.copyFromUtf8("hel"),
            ByteString.copyFromUtf8("lo "),
            ByteString.copyFromUtf8("wor"),
            ByteString.copyFromUtf8("ld"));
  }

  @Test
  void validateSpyByteInputStream() throws IOException {
    var is = spy(new ByteArrayInputStream(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    doCallRealMethod()
        .doCallRealMethod()
        .doCallRealMethod()
        .doThrow(new IOException("FOO"))
        .when(is)
        .read(any(byte[].class));
    byte[] buf = new byte[3];
    assertThat(is.read(buf)).isEqualTo(3);
    assertThat(buf).isEqualTo(new byte[] {0, 1, 2});
    assertThat(is.read(buf)).isEqualTo(3);
    assertThat(buf).isEqualTo(new byte[] {3, 4, 5});
    assertThat(is.read(buf)).isEqualTo(3);
    assertThat(buf).isEqualTo(new byte[] {6, 7, 8});
    assertThatThrownBy(
            () -> {
              try {
                is.read(buf);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            })
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(IOException.class)
        .hasRootCauseMessage("FOO");
  }
}
