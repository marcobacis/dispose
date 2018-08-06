package dispose.net.message;

import java.io.Serializable;

import dispose.net.node.Node;

public interface CtrlMessage extends Serializable
{
  /** Performs the action associated with the control message on the specified
   * node.
   * @param node The node who received the message. 
   * @throws Exception Causes the failure of the node. */
  public void executeOnNode(Node node) throws Exception;
}
