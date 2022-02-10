package net.trajano.grpcchunker.withproto;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import net.trajano.grpcchunker.GrpcStreamsOuterClass;

@Data
@With
@NoArgsConstructor
@AllArgsConstructor
class SampleEntity {
  private String data = "";

  private String meta;

  public SampleEntity withMetaChunk(GrpcStreamsOuterClass.SavedFormChunk metaChunk) {
    return withMeta(metaChunk.getMeta().getId());
  }

  public SampleEntity withDataChunkAdded(GrpcStreamsOuterClass.SavedFormChunk dataChunk) {
    return withData(data + dataChunk.getData().toStringUtf8());
  }

  public GrpcStreamsOuterClass.SavedFormChunk toMetaChunk() {
    return GrpcStreamsOuterClass.SavedFormChunk.newBuilder()
        .setMeta(GrpcStreamsOuterClass.SavedFormMeta.newBuilder().setId(meta))
        .build();
  }

  public Stream<GrpcStreamsOuterClass.SavedFormChunk> toDataChunkStream() {
    return Arrays.stream(data.split("(?<=\\G.{2})"))
        .map(
            s ->
                GrpcStreamsOuterClass.SavedFormChunk.newBuilder()
                    .setData(ByteString.copyFromUtf8(s))
                    .build());
  }
}
