package net.trajano.grpcchunker.withproto;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import net.trajano.grpcchunker.GrpcChunker;
import net.trajano.grpcchunker.GrpcStreamsGrpc;
import net.trajano.grpcchunker.GrpcStreamsOuterClass;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseFormChunk;
import net.trajano.grpcchunker.GrpcStreamsOuterClass.ResponseMetaAndData;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

public class GrpcStreamsClient {

    private final GrpcStreamsGrpc.GrpcStreamsStub stub;
    private final GrpcStreamsGrpc.GrpcStreamsBlockingStub blockingStub;

    public GrpcStreamsClient(Channel channel) {
        this.stub = GrpcStreamsGrpc.newStub(channel);
        this.blockingStub = GrpcStreamsGrpc.newBlockingStub(channel);
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

        final var latch = new CountDownLatch(1);

        final var ret = new AtomicReference<ResponseMetaAndData>();
        final var responseObserver = new StreamObserver<ResponseMetaAndData>() {

            @Override
            public void onNext(ResponseMetaAndData value) {
                if (ret.get() != null) {
                    throw new IllegalStateException("Only expecting one call for onNext");
                }
                ret.set(value);
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
                if (throwable instanceof RuntimeException) {
                    throw (RuntimeException) throwable;
                } else {
                    throw new IllegalStateException(throwable);
                }
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        };

        final var requestObserver = stub.streamingUpload(responseObserver);
        GrpcChunker.chunk(
                entityStream,
                SampleEntity::toMetaChunk,
                SampleEntity::toDataChunkStream,
                requestObserver
        );
        requestObserver.onCompleted();

        latch.await();

        return ResponseSampleEntity.buildFromMetaAndData(ret.get());
    }

    public Stream<ResponseSampleEntity> download(SampleEntity request) {


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

        System.out.println(latch.getCount());
        return collector.stream();
    }


}
