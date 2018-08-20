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

public class ThreadCommandMsg extends CtrlMessage
{
  private static final long serialVersionUID = -2977267460391785421L;
  private Set<Integer> threads;
  private boolean all = false;
  private Command cmd;
  
  
  public enum Command
  {
    START,
    STOP
  }
  
  
  public ThreadCommandMsg(Command cmd)
  {
    this.all = true;
    this.cmd = cmd;
  }
  
  
  public ThreadCommandMsg(int id, Command cmd)
  {
    this.threads = Collections.singleton(id);
    this.cmd = cmd;
  }
  
  
  public ThreadCommandMsg(Collection<Integer> threads, Command cmd)
  {
    this.threads = new HashSet<>(threads);
    this.cmd = cmd;
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
      switch (cmd) {
        case START:
          opthd.start();
          break;
        case STOP:
          opthd.stop();
          break;
        default:
          break;
      }
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
