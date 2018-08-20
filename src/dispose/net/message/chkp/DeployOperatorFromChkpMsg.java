
package dispose.net.message.chkp;

import dispose.net.message.CtrlMessage;
import dispose.net.node.Node;
import dispose.net.node.OperatorCheckpoint;
import dispose.net.node.threads.OperatorThread;


/** Reload the state of an OperatorThread to the one set into the given
 * checkpoint. If the operatorThread doesn't exists on the node, it deploys it
 * with the given state. */
public class DeployOperatorFromChkpMsg extends CtrlMessage
{

  private static final long serialVersionUID = 6569246127640196223L;
  private OperatorCheckpoint checkpoint;


  public DeployOperatorFromChkpMsg(OperatorCheckpoint chkp)
  {
    checkpoint = chkp;
  }


  @Override
  public void executeOnNode(Node node)
  {
    int opID = checkpoint.getOperator().getID();
    OperatorThread op = ((OperatorThread) node.getComputeThread(opID));

    if (op == null) {
      op = new OperatorThread(node, checkpoint.getOperator());
      node.addComputeThread(opID, op);
    }

    op.reloadFromCheckPoint(checkpoint);
  }

}
