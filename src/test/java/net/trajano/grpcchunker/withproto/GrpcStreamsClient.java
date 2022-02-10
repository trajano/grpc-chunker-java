package net.trajano.grpcchunker.withproto;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import net.trajano.grpcchunker.GrpcChunker;
import net.trajano.grpcchunker.GrpcStreamsGrpc;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.SavedFormMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

public class GrpcStreamsClient {

    private final GrpcStreamsGrpc.GrpcStreamsStub stub;

    public GrpcStreamsClient(Channel channel) {
        this.stub = GrpcStreamsGrpc.newStub(channel);
    }

    public Stream<ResponseSampleEntity> sendEntities(Stream<SampleEntity> entityStream) throws InterruptedException {

        var collector = new ArrayList<ResponseSampleEntity>();

        var latch = new CountDownLatch(1);
        var responseObserver = GrpcChunker.dechunkingStreamObserver(
                ResponseFormChunk::hasMeta,
                (ResponseFormChunk chunk) -> new ResponseSampleEntity()
                        .withMetaChunk(chunk),
                (current, chunk) -> new ResponseSampleEntity()
                        .withMeta(current.getMeta())
                        .withData(current.getData())
                        .withDataChunkAdded(chunk),
                collector::add,
                latch
        );

        var requestObserver = stub.saveFormDataStream(responseObserver);
        GrpcChunker.chunk(
                entityStream,
                obj -> SavedFormChunk.newBuilder().setMeta(
                        SavedFormMeta.newBuilder().setId(obj.getMeta()).build()
                ).build(),
                (SampleEntity obj) -> {
                    // Split data two characters at a time
                    return Arrays.stream(obj
                                    .getData()
                                    .split("(?<=\\G.{2})"))
                            .map(
                                    s -> SavedFormChunk.newBuilder().setData(ByteString.copyFromUtf8(s)).build()
                            );
                },
                requestObserver
        );
        requestObserver.onCompleted();

        latch.await();
        return collector.stream();
    }

}
