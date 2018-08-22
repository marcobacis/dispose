package dispose.test;

import dispose.client.Context;
import dispose.client.Op;
import dispose.client.Stream;
import dispose.client.consumer.StdOutConsumerStream;
import dispose.client.producer.SequenceProducerStream;

public class ExampleJob
{
  public static void main(String[] args) throws Exception
  {
    Context context = new Context("127.0.0.1");
    
    /* Simple stream example. The final output is a series 
     * of numbers starting from 5.0 and so on.
     */
    Stream source = new SequenceProducerStream(50, 0, 10000);
    Stream left = source.apply(Op.MAX, 5);
    Stream right = source.apply(Op.AVG, 3).apply(Op.MAX,5);
    Stream joined = left.join(5, right);
    Stream consumer = new StdOutConsumerStream(joined);
    
    context.run(consumer);
  }
}
