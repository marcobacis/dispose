package dispose.net.message;

import dispose.net.links.ObjectFifoLink;
import dispose.net.node.Node;
import dispose.net.node.threads.ClosedEndException;

public class ConnectThreadsMsg extends CtrlMessage
{
  private static final long serialVersionUID = 2787870806083718206L;

  private int fromID;
  private int toID;
  
  
  public ConnectThreadsMsg(int from, int to)
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
  public void executeOnNode(Node node) throws MessageFailureException
  {
    ObjectFifoLink pipeLinkA = new ObjectFifoLink();
    ObjectFifoLink pipeLinkB = new ObjectFifoLink();
    pipeLinkA.connect(pipeLinkB);
    
    try {
      node.getComputeThread(getFrom()).addOutputLink(pipeLinkA, getTo());
      node.getComputeThread(getTo()).addInputLink(pipeLinkB, getFrom());
    } catch (ClosedEndException e) {
      throw new MessageFailureException(e);
    }
  }

}
