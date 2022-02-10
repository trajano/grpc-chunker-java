package net.trajano.grpcchunker.withproto;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import net.trajano.grpcchunker.GrpcChunker;
import net.trajano.grpcchunker.GrpcStreamsGrpc;
import net.trajano.grpcchunker.GrpcStreamsOuterClass;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseMetaAndData;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class GrpcStreamsClient {

    private final GrpcStreamsGrpc.GrpcStreamsStub stub;
    private final GrpcStreamsGrpc.GrpcStreamsBlockingStub blockingStub;

    public GrpcStreamsClient(Channel channel) {
        this.stub = GrpcStreamsGrpc.newStub(channel);
        this.blockingStub = GrpcStreamsGrpc.newBlockingStub(channel);
    }

    public Stream<ResponseSampleEntity> download(SampleEntity request) throws InterruptedException {


        final var collector = new ArrayList<ResponseSampleEntity>();
        final var latch = new CountDownLatch(1);

        final var responseObserver = GrpcChunker.dechunkingStreamObserver(
                ResponseFormChunk::hasMeta,
                ResponseSampleEntity::buildFromMetaChunk,
                ResponseSampleEntity::combineWithAddedDataChunk,
                collector::add,
                latch
        );
        final var grpcRequest = GrpcStreamsOuterClass.MetaAndData.newBuilder()
                .setId(request.getMeta())
                .setData(ByteString.copyFromUtf8(request.getData()))
                .build();
        stub.streamingDownload(grpcRequest, responseObserver);

        latch.await();
        return collector.stream();
    }

    public Stream<ResponseSampleEntity> downloadBlocking(SampleEntity request) {


        final var grpcRequest = GrpcStreamsOuterClass.MetaAndData.newBuilder()
                .setId(request.getMeta())
                .setData(ByteString.copyFromUtf8(request.getData()))
                .build();
        final var responseFormChunkIterator2 = blockingStub.streamingDownload(grpcRequest);
        final var responseFormChunkIterator = blockingStub.streamingDownload(grpcRequest);
        return GrpcChunker.dechunk(
                responseFormChunkIterator,
                ResponseFormChunk::hasMeta,
                ResponseSampleEntity::buildFromMetaChunk,
                ResponseSampleEntity::combineWithAddedDataChunk
        );

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

        final var requestObserver = stub.bidirectionalStreaming(responseObserver);
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

    public ResponseSampleEntity upload(Stream<SampleEntity> entityStream) throws InterruptedException {

        final var responseObserver = GrpcChunker.singleValueStreamObserver(ResponseMetaAndData.class);

        final var requestObserver = stub.streamingUpload(responseObserver);
        GrpcChunker.chunk(
                entityStream,
                SampleEntity::toMetaChunk,
                SampleEntity::toDataChunkStream,
                requestObserver
        );
        requestObserver.onCompleted();

        responseObserver.await();

        return ResponseSampleEntity.buildFromMetaAndData(responseObserver.getValue().orElseThrow());
    }

    public Optional<ResponseSampleEntity> upload(Stream<SampleEntity> entityStream, long timeoutInMs) throws InterruptedException {

        final var responseObserver = GrpcChunker.singleValueStreamObserver(ResponseMetaAndData.class);

        final var requestObserver = stub.streamingUpload(responseObserver);
        GrpcChunker.chunk(
                entityStream,
                SampleEntity::toMetaChunk,
                SampleEntity::toDataChunkStream,
                requestObserver
        );
        requestObserver.onCompleted();

        if (responseObserver.await(timeoutInMs, TimeUnit.MILLISECONDS)) {
            return responseObserver.getValue()
                    .map(ResponseSampleEntity::buildFromMetaAndData);
        } else {
            return Optional.empty();
        }
    }


}
