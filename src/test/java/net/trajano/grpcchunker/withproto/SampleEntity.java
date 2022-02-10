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
class SampleEntity {
    private String data = "";

    private String meta;

    public SampleEntity withMetaChunk(GrpcStreamsOuterClass.SavedFormChunk metaChunk) {
        return withMeta(metaChunk.getMeta().getId());
    }

    public SampleEntity withDataChunkAdded(GrpcStreamsOuterClass.SavedFormChunk dataChunk) {
        return withData(data + dataChunk.getData().toStringUtf8());
    }

}
