package dispose.net.message;

import java.util.Collections;
import java.util.Set;

import dispose.net.node.ComputeThread;
import dispose.net.node.Node;

public class StartThreadMsg extends CtrlMessage
{
  private static final long serialVersionUID = -2977267460391785421L;

  private int opID = 0;
  
  private boolean all = false;
  
  
  public StartThreadMsg()
  {
    this.all = true;
  }
  
  
  public StartThreadMsg(int id)
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
      ops = node.getCurrentlyInstantiatedThreads();
    else
      ops = Collections.singleton(id());
    
    for (Integer opid: ops) {
      ComputeThread opthd = node.getComputeThread(opid);
      opthd.start();
    }
  }
}
