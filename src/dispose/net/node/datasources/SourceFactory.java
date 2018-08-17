package dispose.net.node.datasources;

import dispose.client.Stream;
import dispose.client.producer.FileProducerStream;
import dispose.client.producer.RandomProducerStream;

public class SourceFactory
{

  public static DataSource getFromStream(Stream producer)
  {
    if(producer instanceof FileProducerStream)
      return new FileDataSource(producer.getID(), ((FileProducerStream) producer).getFilePath());
    
    return new RandomFloatDataSrc(producer.getID(), ((RandomProducerStream) producer).getThrottle());
  }
  
}
