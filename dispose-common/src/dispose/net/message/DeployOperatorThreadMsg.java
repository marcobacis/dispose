package dispose.net.message;

import dispose.net.node.Node;
import dispose.net.node.operators.Operator;
import dispose.net.node.threads.OperatorThread;

public class DeployOperatorThreadMsg extends CtrlMessage
{
  private static final long serialVersionUID = 8391051064976656835L;
  
  private Operator op;
  
  
  public DeployOperatorThreadMsg(Operator op) {
    this.op = op;
  }
  
  
  public Operator getOperator()
  {
    return this.op;
  }

  
  @Override
  public void executeOnNode(Node node) throws Exception
  {
    Operator op = getOperator();
    OperatorThread opthd = new OperatorThread(op);
    node.addComputeThread(op.getID(), opthd);
  }
}
