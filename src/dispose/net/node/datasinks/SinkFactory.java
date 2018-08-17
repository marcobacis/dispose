package dispose.net.node.datasinks;

import dispose.client.Stream;
import dispose.client.consumer.FileConsumerStream;

public class SinkFactory
{

  public static DataSink getFromStream(Stream consumer)
  {
    if(consumer instanceof FileConsumerStream)
      return new FileDataSink(consumer.getID(), (((FileConsumerStream) consumer).getFilePath()));
    
    return new ObjectLogDataSink(consumer.getID()); //default
  }
}
