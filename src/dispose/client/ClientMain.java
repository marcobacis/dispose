package dispose.client;

public class ClientMain
{
  public static void main(String[] args)
  {
    Stream source = new Stream();
    
    Stream a = source.apply(Op.MAX, 5);
    
    Stream b = source.apply(Op.AVG, 5);
    
    Stream d = b.join(source.apply(Op.MIN,5)).apply(Op.MIN, 4);
    
    Stream consumer = a.join(d,b).apply(Op.MAX,1);
        
    System.out.println(ClientDag.derive(consumer).serialize());
  }
  
}
