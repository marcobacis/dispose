package dispose.client.producer;

import dispose.client.Stream;

public class SequenceProducerStream extends Stream
{

  private static final long serialVersionUID = -4515331518014152974L;
  private int start;
  private int end;
  private int throttle = 1000;
  
  public SequenceProducerStream() {
    this.start = 0;
    this.end = 0;
  }
  
  public SequenceProducerStream(int throttlems) {
    this();
    this.throttle = throttlems;
  }
  
  public SequenceProducerStream(int throttlems, int start)
  {
    this(throttlems);
    this.start = start;
  }
  
  public SequenceProducerStream(int throttlems, int start, int end)
  {
    this(throttlems, start);
    this.end = end;
  }
  
  public int getThrottle()
  {
    return throttle;
  }
  
  public int getStart()
  {
    return start;
  }
  
  public int getEnd()
  {
    return end;
  }
  
}
