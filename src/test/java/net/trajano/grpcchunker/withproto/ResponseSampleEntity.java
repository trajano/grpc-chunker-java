package net.trajano.grpcchunker.withproto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import net.trajano.grpcchunker.GrpcStreamsOuterClass;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormMeta;

@Data
@With
@NoArgsConstructor
@AllArgsConstructor
class ResponseSampleEntity {
  private String data = "";

  private String meta;

  public static ResponseSampleEntity buildFromMetaAndData(
      GrpcStreamsOuterClass.ResponseMetaAndData o) {
    return new ResponseSampleEntity().withMeta(o.getId()).withData(o.getData().toStringUtf8());
  }

  public ResponseFormChunk toMetaChunk() {
    return ResponseFormChunk.newBuilder().setMeta(SavedFormMeta.newBuilder().setId(meta)).build();
  }

  public ResponseSampleEntity withMetaChunk(ResponseFormChunk metaChunk) {
    return withMeta(metaChunk.getMeta().getId());
  }

  public ResponseSampleEntity withDataChunkAdded(ResponseFormChunk dataChunk) {
    return withData(data + dataChunk.getData().toStringUtf8());
  }

  public static ResponseSampleEntity buildFromMetaChunk(ResponseFormChunk chunk) {
    return new ResponseSampleEntity().withMetaChunk(chunk);
  }

  public static ResponseSampleEntity combineWithAddedDataChunk(
      ResponseSampleEntity current, ResponseFormChunk chunk) {
    return new ResponseSampleEntity()
        .withMeta(current.getMeta())
        .withData(current.getData())
        .withDataChunkAdded(chunk);
  }
}
