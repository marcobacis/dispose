package dispose.net.node.datasources;

import dispose.client.Stream;
import dispose.client.producer.FileProducerStream;
import dispose.client.producer.RandomProducerStream;
import dispose.client.producer.SequenceProducerStream;

public class SourceFactory
{

  public static DataSource getFromStream(Stream producer)
  {
    if(producer instanceof FileProducerStream)
      return new FileDataSource((FileProducerStream) producer);
    
    else if(producer instanceof SequenceProducerStream) 
      return new SequenceDataSource((SequenceProducerStream) producer);
    
    return new RandomFloatDataSrc(producer.getID(), ((RandomProducerStream) producer).getThrottle());
  }
  
}
