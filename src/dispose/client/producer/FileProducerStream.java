package dispose.client.producer;

import dispose.client.Stream;

public class FileProducerStream extends Stream
{
  private static final long serialVersionUID = 422454195691636339L;
  
  private String filepath;
  
  public FileProducerStream(String filepath) {
    super();
    this.filepath = filepath;
    
    this.params.add(this.filepath);
  }
  
  public String getFilePath() {
    return this.filepath;
  }
  
}
