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

    public ResponseSampleEntity withMetaChunk(GrpcStreamsOuterClass.ResponseFormChunk metaChunk) {
        return withMeta(metaChunk.getMeta().getId());
    }

    public ResponseSampleEntity withDataChunkAdded(GrpcStreamsOuterClass.ResponseFormChunk dataChunk) {
        return withData(data + dataChunk.getData().toStringUtf8());
    }

}
