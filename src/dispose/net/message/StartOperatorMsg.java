package dispose.net.message;

import java.util.Collections;
import java.util.Set;

import dispose.net.node.Node;
import dispose.net.node.OperatorThread;

public class StartOperatorMsg implements CtrlMessage
{
  private static final long serialVersionUID = -2977267460391785421L;

  private int opID = 0;
  
  private boolean all = false;
  
  
  public StartOperatorMsg()
  {
    this.all = true;
  }
  
  
  public StartOperatorMsg(int id)
  {
    this.opID = id;
  }
  
  
  public boolean all()
  {
    return this.all;
  }
  
  
  public int id()
  {
    return this.opID;
  }

  
  @Override
  public void executeOnNode(Node node) throws Exception
  {
    Set<Integer> ops;
    
    if (all())
      ops = node.getCurrentlyInstantiatedOperators();
    else
      ops = Collections.singleton(id());
    
    for (Integer opid: ops) {
      OperatorThread opthd = node.getOperator(opid);
      opthd.start();
    }
  }
}
