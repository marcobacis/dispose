
package dispose.test;

import dispose.client.ClientDag;
import dispose.client.Op;
import dispose.client.Stream;
import dispose.client.consumer.FileConsumerStream;
import dispose.client.producer.FileProducerStream;

public class ExampleDag
{
  public static void main(String[] args)
  {
    Stream source = new FileProducerStream("ciao.csv");

    Stream a = source.apply(Op.MAX, 5);

    Stream b = source.apply(Op.AVG, 5);

    Stream d = b.join(3, source.apply(Op.MIN, 5)).apply(Op.MIN, 4);

    Stream consumer = new FileConsumerStream("output.csv", a.join(4, d).apply(Op.MAX, 1));

    ClientDag compDag = ClientDag.derive(consumer);

    System.out.println(compDag.toString());
  }
}
