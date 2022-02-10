package net.trajano.grpcchunker.withproto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;
import net.trajano.grpcchunker.GrpcStreamsOuterClass;

@Data
@With
@NoArgsConstructor
@AllArgsConstructor
class ResponseSampleEntity {
    private String data = "";

    private String meta;

    public static ResponseSampleEntity buildFromMetaAndData(GrpcStreamsOuterClass.ResponseMetaAndData o) {
        return new ResponseSampleEntity()
                .withMeta(o.getId())
                .withData(o.getData().toStringUtf8());
    }

    public ResponseSampleEntity withMetaChunk(GrpcStreamsOuterClass.ResponseFormChunk metaChunk) {
        return withMeta(metaChunk.getMeta().getId());
    }

    public ResponseSampleEntity withDataChunkAdded(GrpcStreamsOuterClass.ResponseFormChunk dataChunk) {
        return withData(data + dataChunk.getData().toStringUtf8());
    }

    public static ResponseSampleEntity buildFromMetaChunk(GrpcStreamsOuterClass.ResponseFormChunk chunk) {
        return new ResponseSampleEntity()
                .withMetaChunk(chunk);
    }

    public static ResponseSampleEntity combineWithAddedDataChunk(ResponseSampleEntity current, GrpcStreamsOuterClass.ResponseFormChunk chunk) {
        return new ResponseSampleEntity()
                .withMeta(current.getMeta())
                .withData(current.getData())
                .withDataChunkAdded(chunk);
    }


}
