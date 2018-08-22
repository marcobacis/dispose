
package dispose.net.message.chkp;

import java.util.UUID;

import dispose.net.message.CtrlMessage;
import dispose.net.node.ComputeThread;
import dispose.net.node.Node;
import dispose.net.node.checkpoint.Checkpoint;
import dispose.net.node.threads.OperatorThread;


/** Reload the state of an OperatorThread to the one set into the given
 * checkpoint. If the operatorThread doesn't exists on the node, it deploys it
 * with the given state. */
public class DeployComputeNodeFromChkpMsg extends CtrlMessage
{

  private static final long serialVersionUID = 6569246127640196223L;
  private Checkpoint checkpoint;
  private UUID jid;


  public DeployComputeNodeFromChkpMsg(UUID jid, Checkpoint chkp)
  {
    checkpoint = chkp;
    this.jid = jid;
  }


  @Override
  public void executeOnNode(Node node)
  {
    int opID = checkpoint.getComputeNode().getID();
    ComputeThread compThread = ((OperatorThread) node.getComputeThread(opID));

    if (compThread == null) {
      compThread = ComputeThread.createComputeThread(node, jid, checkpoint.getComputeNode());
      node.addComputeThread(opID, compThread);
    }

    compThread.reloadFromCheckpoint(checkpoint);
  }

}
