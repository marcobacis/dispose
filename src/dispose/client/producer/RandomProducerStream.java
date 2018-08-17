package dispose.client.producer;

import dispose.client.Stream;

public class RandomProducerStream extends Stream
{

  private static final long serialVersionUID = -1923057402288946786L;
  private int throttle = 1000;
  
  public RandomProducerStream()
  {
    super();
  }
  
  public RandomProducerStream(int throttlems)
  {
    super();
    this.throttle = throttlems;
  }
  
  public int getThrottle()
  {
    return throttle;
  }
}
