package dispose.net.message;

import java.util.UUID;

import dispose.net.node.Node;
import dispose.net.node.operators.Operator;
import dispose.net.node.threads.OperatorThread;

public class DeployOperatorThreadMsg extends CtrlMessage
{
  private static final long serialVersionUID = 8391051064976656835L;
  
  private Operator op;
  private UUID jid;
  
  
  public DeployOperatorThreadMsg(UUID jid, Operator op) {
    this.op = op;
    this.jid = jid;
  }
  
  
  public Operator getOperator()
  {
    return this.op;
  }

  public UUID getJobID()
  {
    return this.jid;
  }
  
  @Override
  public void executeOnNode(Node node)
  {
    Operator op = getOperator();
    OperatorThread opthd = new OperatorThread(node, jid, op);
    node.addComputeThread(op.getID(), opthd);
  }
}
