package dispose.net.message;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import dispose.net.links.LinkBrokenException;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.supervisor.NodeProxy;
import dispose.net.supervisor.Supervisor;

public class StartThreadMsg extends CtrlMessage
{
  private static final long serialVersionUID = -2977267460391785421L;
  private Set<Integer> threads;
  private boolean all = false;
  
  
  public StartThreadMsg()
  {
    this.all = true;
  }
  
  
  public StartThreadMsg(int id)
  {
    this.threads = Collections.singleton(id);
  }
  
  
  public StartThreadMsg(Collection<Integer> threads)
  {
    this.threads = new HashSet<>(threads);
  }

  
  @Override
  public void executeOnNode(Node node) throws MessageFailureException
  {
    Set<Integer> ops;
    
    if (all)
      ops = node.getCurrentlyInstantiatedThreads();
    else
      ops = threads;
    
    for (Integer opid: ops) {
      ComputeThread opthd = node.getComputeThread(opid);
      opthd.start();
    }
  }
  
  
  @Override
  public void executeOnSupervisor(Supervisor supervis, NodeProxy nodem) throws MessageFailureException
  {
    Set<NodeProxy> nodes = supervis.getNodes();
    for (NodeProxy node: nodes) {
      try {
        node.getLink().sendMsg(this);
      } catch (LinkBrokenException e) {
        throw new MessageFailureException(e);
      }
    }
  }
}
