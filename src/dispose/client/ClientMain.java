
package dispose.client;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class ClientMain
{
  public static void main(String[] args)
  {

    Stream source = new FileProducerStream("ciao.csv");

    Stream a = source.apply(Op.MAX, 5);

    Stream b = source.apply(Op.AVG, 5);

    Stream d = b.join(source.apply(Op.MIN, 5)).apply(Op.MIN, 4);

    Stream consumer = new FileConsumerStream("output.csv",
      a.join(d, b).apply(Op.MAX, 1));

    ClientDag compDag = ClientDag.derive(consumer);

    System.out.println(compDag.toString());
  }
}
