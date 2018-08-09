
package dispose.client;

public class ClientMain
{
  public static void main(String[] args)
  {

    Stream source = new FileProducerStream("ciao.csv");

    Stream a = source.apply(Op.MAX, 5);

    Stream b = source.apply(Op.AVG, 5);

    Stream d = b.join(3, source.apply(Op.MIN, 5)).apply(Op.MIN, 4);

    Stream consumer = new FileConsumerStream("output.csv",
      a.join(4, d, b).apply(Op.MAX, 1));

    ClientDag compDag = ClientDag.derive(consumer);

    System.out.println(compDag.toString());
  }
}
