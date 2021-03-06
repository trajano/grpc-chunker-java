package net.trajano.grpcchunker.withproto;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import net.trajano.grpcchunker.GrpcChunker;
import net.trajano.grpcchunker.GrpcStreamsGrpc.GrpcStreamsImplBase;
import net.trajano.grpcchunker.GrpcStreamsOuterClass;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.MetaAndData;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormMeta;

public class GrpcStreamsService extends GrpcStreamsImplBase {
  @Override
  public StreamObserver<SavedFormChunk> bidirectionalStreaming(
      final StreamObserver<ResponseFormChunk> responseObserver) {
    return GrpcChunker.dechunkingStreamObserver(
        SavedFormChunk::hasMeta,
        (SavedFormChunk chunk) -> new SampleEntity().withMetaChunk(chunk),
        (current, chunk) ->
            new SampleEntity()
                .withMeta(current.getMeta())
                .withData(current.getData())
                .withDataChunkAdded(chunk),
        (SampleEntity o) ->
            GrpcChunker.chunk(
                // process one a time
                Stream.of(o),
                // ID is the meta value.
                obj ->
                    ResponseFormChunk.newBuilder()
                        .setMeta(SavedFormMeta.newBuilder().setId(o.getMeta()).build())
                        .build(),
                (SampleEntity obj) -> {
                  // Split data two characters at a time
                  return Arrays.stream(obj.getData().split("(?<=\\G.{2})"))
                      .map(
                          s ->
                              ResponseFormChunk.newBuilder()
                                  .setData(ByteString.copyFromUtf8(s))
                                  .build());
                },
                responseObserver),
        responseObserver);
  }

  @Override
  @SneakyThrows
  public StreamObserver<SavedFormChunk> streamingUpload(
      StreamObserver<GrpcStreamsOuterClass.ResponseMetaAndData> responseObserver) {

    var responseBuilder = GrpcStreamsOuterClass.ResponseMetaAndData.newBuilder();
    return GrpcChunker.dechunkingStreamObserver(
        SavedFormChunk::hasMeta,
        (SavedFormChunk chunk) -> new SampleEntity().withMetaChunk(chunk),
        (current, chunk) ->
            new SampleEntity()
                .withMeta(current.getMeta())
                .withData(current.getData())
                .withDataChunkAdded(chunk),
        (SampleEntity o) -> {
          responseBuilder
              .setId(o.getMeta())
              .setData(
                  ByteString.copyFromUtf8(responseBuilder.getData().toStringUtf8() + o.getData()));
        },
        (SampleEntity o) -> responseObserver.onNext(responseBuilder.build()),
        responseObserver);
  }

  @Override
  public void streamingDownload(
      MetaAndData request, StreamObserver<ResponseFormChunk> responseObserver) {
    GrpcChunker.chunk(
        Stream.of(request, request, request),
        r ->
            ResponseFormChunk.newBuilder()
                .setMeta(SavedFormMeta.newBuilder().setId(r.getId()).build())
                .build(),
        r -> Stream.of(ResponseFormChunk.newBuilder().setData(r.getData()).build()),
        responseObserver);
    responseObserver.onCompleted();
  }
}
