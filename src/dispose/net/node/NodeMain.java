package dispose.net.node;

import dispose.net.common.types.FloatData;
import dispose.net.node.operators.IncrementOperator;

public class NodeMain
{

  public static void main(String[] args)
  {
    IncrementOperator op = new IncrementOperator();
    for (int i=0; i<10; i++) {
      System.out.println(op.processAtom(new FloatData(i)));
    }
    System.out.println(op.clock());
  }

}
