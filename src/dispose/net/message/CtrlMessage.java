package dispose.net.message;

import java.io.Serializable;
import java.util.UUID;

import dispose.net.node.Node;
import dispose.net.supervisor.Supervisor;

@SuppressWarnings("serial")
public class CtrlMessage implements Serializable
{
  private UUID uuid = UUID.randomUUID();
  
  
  /** Performs the action associated with the control message on the specified
   * node.
   * @param node The node who received the message. 
   * @throws Exception Causes the failure of the node. */
  public void executeOnNode(Node node) throws Exception
  {
    throw new Exception("message " + this.getClass().toString() + " not to be sent to a node");
  }
  
  
  /** Performs the action associated with the control message on the specified
   * supervisor.
   * @param node The supervisor who received the message. 
   * @throws Exception Causes the failure of the supervisor. */
  public void executeOnSupervisor(Supervisor supervis) throws Exception
  {
    throw new Exception("message " + this.getClass().toString() + " not to be sent to a supervisor");
  }
  
  
  public UUID getUUID()
  {
    return uuid;
  }
}
