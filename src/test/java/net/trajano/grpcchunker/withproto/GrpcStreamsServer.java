package net.trajano.grpcchunker.withproto;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import net.trajano.grpcchunker.GrpcChunker;
import net.trajano.grpcchunker.GrpcStreamsGrpc.GrpcStreamsImplBase;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormMeta;

import java.util.Arrays;
import java.util.stream.Stream;

public class GrpcStreamsServer extends GrpcStreamsImplBase {
    @Override
    public StreamObserver<SavedFormChunk> bidirectionalStreaming(StreamObserver<ResponseFormChunk> responseObserver) {
        return GrpcChunker.dechunkingStreamObserver(
                SavedFormChunk::hasMeta,
                (SavedFormChunk chunk) -> new SampleEntity()
                        .withMetaChunk(chunk),
                (current, chunk) -> new SampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData())
                        .withDataChunkAdded(chunk),
                (SampleEntity o) -> GrpcChunker.chunk(
                        // process one a time
                        Stream.of(o),
                        // ID is the meta value.
                        obj -> ResponseFormChunk.newBuilder().setMeta(SavedFormMeta.newBuilder().setId(o.getMeta()).build()).build(),
                        (SampleEntity obj) -> {
                            // Split data two characters at a time
                            return Arrays.stream(obj
                                            .getData()
                                            .split("(?<=\\G.{2})"))
                                    .map(
                                            s -> ResponseFormChunk.newBuilder().setData(ByteString.copyFromUtf8(s)).build()
                                    );
                        },
                        responseObserver
                ),
                responseObserver
        );
    }
}
