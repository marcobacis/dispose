package dispose.net.message;

import dispose.net.links.Link;
import dispose.net.links.PipeLink;
import dispose.net.node.Node;

public class ConnectOperatorMsg implements CtrlMessage
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
    Link pipeLink = new PipeLink();
    
    node.getOperator(getFrom()).addOutput(pipeLink);
    node.getOperator(getTo()).addInput(pipeLink);
  }

}
