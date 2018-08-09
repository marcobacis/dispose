package dispose.net.message;

import dispose.net.node.Node;
import dispose.net.node.Operator;
import dispose.net.node.OperatorThread;

public class DeployOperatorMsg extends CtrlMessage
{
  private static final long serialVersionUID = 8391051064976656835L;
  
  private Operator op;
  
  
  public DeployOperatorMsg(Operator op) {
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
    node.addOperator(op.getID(), opthd);
  }
}
