package dispose.net.node;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import dispose.net.links.Link;
import dispose.net.links.MonitoredLink;
import dispose.net.message.CtrlMessage;


public class Node implements Runnable, MonitoredLink.Delegate
{
  private Map<Integer, OperatorThread> operators;
  private Link ctrlLink;
  
  
  public Node(Link ctrlLink)
  {
    operators = new HashMap<>();
    this.ctrlLink = ctrlLink;
  }

  
  @Override
  public void run()
  {
    MonitoredLink.syncMonitorLink(ctrlLink, this);
  }
  
  
  @Override
  public void messageReceived(CtrlMessage msg) throws Exception
  {
    //TODO handle errors on the supervisor link (and on the creation of operator links)
    msg.executeOnNode(this);
  }
  
  
  /** Returns the operator with the specified ID or null if that operator is
   * not materialized on this node.
   * @param opid The ID of the operator.
   * @return An operator or null. */
  synchronized public OperatorThread getOperator(int opid)
  {
    return operators.get(opid);
  }
  
  
  /** Add an instantiated operator to the local directory of operators.
   * @param opid The ID of the operator.
   * @param opthd The materialized operator. */
  synchronized public void addOperator(int opid, OperatorThread opthd)
  {
    operators.put(opid, opthd);
  }
  
  
  synchronized public Set<Integer> getCurrentlyInstantiatedOperators()
  {
    return operators.keySet();
  }


  @Override
  public void linkIsBroken(Exception e)
  {
    System.out.println("link down");
  }
}
