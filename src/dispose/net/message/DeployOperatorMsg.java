package dispose.net.message;

import dispose.net.node.Operator;

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
  
}
