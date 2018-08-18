package dispose.net.node;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.message.CtrlMessage;
import dispose.net.message.Message;
import dispose.net.message.MessageFailureException;
import dispose.net.node.threads.SourceThread;


public class Node implements Runnable, MonitoredLink.Delegate
{
  private Map<Integer, ComputeThread> operators;
  private MonitoredLink ctrlLink;
  
  
  public Node(Link ctrlLink)
  {
    operators = new HashMap<>();
    this.ctrlLink = new MonitoredLink(ctrlLink, this);
  }

  
  @Override
  public void run()
  {
    ctrlLink.monitorSynchronously();
  }
  
  
  @Override
  public void messageReceived(Message msg) throws MessageFailureException
  {
    //TODO handle errors on the supervisor link (and on the creation of operator links)
    CtrlMessage cmsg = (CtrlMessage)msg;
    cmsg.executeOnNode(this);
  }
  
  
  /** Returns the operator with the specified ID or null if that operator is
   * not materialized on this node.
   * @param opid The ID of the operator.
   * @return An operator or null. */
  synchronized public ComputeThread getComputeThread(int opid)
  {
    return operators.get(opid);
  }
  
  
  /** Add an instantiated operator to the local directory of operators.
   * @param opid The ID of the operator.
   * @param opthd The materialized operator. */
  synchronized public void addComputeThread(int opid, ComputeThread opthd)
  {
    operators.put(opid, opthd);
  }
  
  
  public MonitoredLink getControlLink()
  {
    return ctrlLink;
  }
  
  
  synchronized public Set<Integer> getCurrentlyInstantiatedThreads()
  {
    return Collections.unmodifiableSet(new HashSet<>(operators.keySet()));
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    System.out.println("link down in node");
    e.printStackTrace();
  }
  
  public synchronized void sendMsgToSupervisor(int opID, Message msg)
  {
    try {
      this.ctrlLink.sendMsg(msg);
    } catch (Exception e) {
      linkIsBroken(e);
    }
  }
  
  public void injectIntoSource(Message toInject)
  {
    //first, check if there is a source here (at most one)
    Collection<ComputeThread> threads = operators.values();
    
    for(ComputeThread thread : threads) {
      if(thread instanceof SourceThread) {
        ((SourceThread) thread).injectMessage(toInject);
        return;
      }
    }
  }
  
}
