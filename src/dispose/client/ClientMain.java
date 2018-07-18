package dispose.client;

public class ClientMain
{
  public static void main(String[] args)
  {
    Stream source = new Stream();
    
    Stream a = source.apply(Op.MAX, 5);
    
    Stream b = source.apply(Op.AVG, 5);
    
    Stream d = b.join(source.apply(Op.MIN,5)).apply(Op.MIN, 4);
    
    Stream consumer = a.join(d).apply(Op.MAX,1);
    
    Stream curr = consumer;
    
    printDag(curr);
  }
  
  private static void printDag(Stream s) {
    System.out.println("Stream "+ s.getID() + " with op " + s.getOperation() + " and window of " + s.getWindowSize());
    
    if(!s.getParents().isEmpty()) {
      System.out.print("Parents are ");
      for(Stream p : s.getParents()) {
        System.out.print(p.getID() +  " ");
      }
      System.out.print("\n\n");

      for(Stream p : s.getParents()) {
        printDag(p);
      }
    }
  }
  
}
