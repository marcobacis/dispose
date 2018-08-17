package dispose.client.consumer;

import dispose.client.Op;
import dispose.client.Stream;

public class StdOutConsumerStream extends Stream
{

  private static final long serialVersionUID = 205030467330173582L;
  
  public StdOutConsumerStream(Stream stream)
  {
    super(Op.NONE, 1, stream);
  }

}
