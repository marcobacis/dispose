package dispose.net.message;

import dispose.net.links.PipeLink;
import dispose.net.node.Node;

public class ConnectOperatorMsg extends CtrlMessage
{
  private static final long serialVersionUID = 2787870806083718206L;

  private int fromID;
  private int toID;
  
  
  public ConnectOperatorMsg(int from, int to)
  {
    this.fromID = from;
    this.toID = to;
  }
  
  
  public int getFrom()
  {
    return this.fromID;
  }
  
  
  public int getTo()
  {
    return this.toID;
  }

  
  @Override
  public void executeOnNode(Node node) throws Exception
  {
    PipeLink pipeLinkA = new PipeLink();
    PipeLink pipeLinkB = new PipeLink();
    pipeLinkA.connect(pipeLinkB);
    
    node.getOperator(getFrom()).addOutput(pipeLinkA);
    node.getOperator(getTo()).addInput(pipeLinkB);
  }

}
