package net.trajano.grpcchunker.withproto;

import io.grpc.Channel;
import net.trajano.grpcchunker.GrpcChunker;
import net.trajano.grpcchunker.GrpcStreamsGrpc;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseFormChunk;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

public class GrpcStreamsClient {

    private final GrpcStreamsGrpc.GrpcStreamsStub stub;

    public GrpcStreamsClient(Channel channel) {
        this.stub = GrpcStreamsGrpc.newStub(channel);
    }

    public Stream<ResponseSampleEntity> sendEntities(Stream<SampleEntity> entityStream) throws InterruptedException {

        final var collector = new ArrayList<ResponseSampleEntity>();
        final var latch = new CountDownLatch(1);
        final var responseObserver = GrpcChunker.dechunkingStreamObserver(
                ResponseFormChunk::hasMeta,
                ResponseSampleEntity::buildFromMetaChunk,
                ResponseSampleEntity::combineWithAddedDataChunk,
                collector::add,
                latch
        );

        final var requestObserver = stub.saveFormDataStream(responseObserver);
        GrpcChunker.chunk(
                entityStream,
                SampleEntity::toMetaChunk,
                SampleEntity::toDataChunkStream,
                requestObserver
        );
        requestObserver.onCompleted();

        latch.await();
        return collector.stream();
    }

}
