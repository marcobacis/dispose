package dispose.client.consumer;

import dispose.client.Op;
import dispose.client.Stream;

public class FileConsumerStream extends Stream
{
  private static final long serialVersionUID = 1389004132285518627L;
  private String filepath;
  
  public FileConsumerStream(String filepath, Stream inStream) {
    super(Op.NONE, 1, inStream);
    this.filepath = filepath;
    
    this.params.add(this.filepath);
  }
  
  public String getFilePath() {
    return this.filepath;
  }
}
