package net.trajano.grpcchunker.simple;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.With;

@Data
@With
@NoArgsConstructor
@AllArgsConstructor
class SampleEntity {
  private String data = "";

  private String meta;
}
